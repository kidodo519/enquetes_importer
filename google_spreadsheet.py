import logging
import os
from argparse import ArgumentParser
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import gspread
import jaconv
import psycopg2
import yaml
from dateutil import parser as date_parser
from dateutil import tz
from oauth2client.service_account import ServiceAccountCredentials
from psycopg2 import extras

DEFAULT_TABLE_NAME = "enquetes"
GENERATED_FIELDS = ("facility_code", "enquete_key", "import_date")
DEFAULT_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
DEFAULT_TIMEZONE = tz.gettz("Asia/Tokyo") or tz.tzlocal()

FIELD_SECTION_PREFIXES = ("string", "text", "integer", "date", "datetime")
CONVERSION_PREFIX = "conversion"


@dataclass
class FieldSpec:
    csv_header: str
    conversion: Optional[Dict[str, str]] = None
    clamp: Optional[Tuple[int, int]] = None


@dataclass
class ImportJob:
    table: str
    mapping: Dict[str, Dict[str, FieldSpec]]
    mapping_name: Optional[str] = None

logger = logging.getLogger(__name__)


def is_section_key(key: str) -> bool:
    return any(key == prefix or key.startswith(prefix) for prefix in FIELD_SECTION_PREFIXES)


def is_conversion_key(key: str) -> bool:
    return key == CONVERSION_PREFIX or key.startswith(CONVERSION_PREFIX)


def is_mapping_definition_dict(value: Any) -> bool:
    if not isinstance(value, dict):
        return False

    if not value:
        return True

    keys = list(value.keys())
    return all(is_section_key(key) or is_conversion_key(key) for key in keys)


def merge_mapping_catalog(
    target: Dict[str, Any], catalog: Optional[Dict[str, Any]]
) -> None:
    if not catalog:
        return

    if is_mapping_definition_dict(catalog):
        target["default"] = catalog
        return

    for key, value in catalog.items():
        target[key] = value


def apply_conversion(value: str, conversion: Optional[Dict[str, str]]) -> str:
    if not value or not conversion:
        return value
    return conversion.get(value, value)


def parse_integer_cell(value: str, clamp: Optional[Tuple[int, int]] = None) -> Optional[int]:
    if not value:
        return None

    normalized_value = jaconv.z2h(value, digit=True, ascii=True).replace(",", "")

    if normalized_value.isdecimal():
        parsed = int(normalized_value)
    else:
        try:
            parsed = int(float(normalized_value))
        except ValueError:
            return None

    if clamp:
        minimum, maximum = clamp
        parsed = max(parsed, minimum)
        parsed = min(parsed, maximum)

    return parsed


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Import enquete spreadsheet data into the configured databases."
    )
    parser.add_argument(
        "-c",
        "--corporation",
        dest="corporations",
        action="append",
        help="Corporation key(s) to import. Defaults to all corporations defined in the config.",
    )
    parser.add_argument(
        "-f",
        "--facility",
        dest="facilities",
        action="append",
        help=(
            "Facility key(s) to import. Use either the facility name or the 'corporation.facility' format."
        ),
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE_NAME,
        help="Destination table name. Defaults to '%(default)s'.",
    )
    return parser


def load_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as fp:
        return yaml.safe_load(fp)


def create_gspread_client(base_path: str) -> gspread.Client:
    json_file = os.path.join(base_path, "client_secret.json")
    if not os.path.exists(json_file):
        raise FileNotFoundError(f"Google service account credentials not found: {json_file}")

    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_file, DEFAULT_SCOPE)
    return gspread.authorize(credentials)


def normalize_cell_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def normalize_header_name(value: Any) -> str:
    return normalize_cell_value(value)


def replace_invalid_shiftjis_chars(value: str, replace_with: str = "?") -> str:
    return "".join(
        char if char.encode("shift_jis", errors="ignore") else replace_with for char in value
    )


def parse_datetime_value(value: Any) -> Optional[datetime]:
    text = normalize_cell_value(value)
    if not text or text == "0":
        return None

    normalized = text.replace("　", " ")
    if any(char in normalized for char in ("年", "月", "日")):
        normalized = normalized.replace("年", "/").replace("月", "/").replace("日", "")

    try:
        parsed = date_parser.parse(normalized, yearfirst=True, dayfirst=False)
    except (ValueError, TypeError):
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=DEFAULT_TIMEZONE)

    return parsed


def normalize_mapping(mapping: Dict[str, Any]) -> Dict[str, Dict[str, FieldSpec]]:
    conversions: Dict[str, Dict[str, str]] = {}

    for key, value in mapping.items():
        if not is_conversion_key(key):
            continue
        if value is None:
            conversions[key[len(CONVERSION_PREFIX) :]] = {}
            continue
        if not isinstance(value, dict):
            raise TypeError(f"Conversion '{key}' must be a dictionary.")
        suffix = key[len(CONVERSION_PREFIX) :]
        conversions[suffix] = {
            normalize_cell_value(source): normalize_cell_value(target)
            for source, target in value.items()
        }

    normalized: Dict[str, Dict[str, FieldSpec]] = {
        section: {} for section in FIELD_SECTION_PREFIXES
    }

    for key, value in mapping.items():
        if not is_section_key(key):
            continue

        if value is None:
            value = {}

        if not isinstance(value, dict):
            raise TypeError(f"Mapping section '{key}' must be a dictionary.")

        base_prefix = next(prefix for prefix in FIELD_SECTION_PREFIXES if key.startswith(prefix))
        suffix = key[len(base_prefix) :]

        for db_key, csv_key in value.items():
            normalized_csv = normalize_header_name(csv_key)

            conversion = conversions.get(suffix)
            clamp: Optional[Tuple[int, int]] = None
            if base_prefix == "integer" and suffix == "2":
                clamp = (0, 100)

            normalized[base_prefix][db_key] = FieldSpec(
                csv_header=normalized_csv,
                conversion=conversion,
                clamp=clamp,
            )

    return normalized


def resolve_mapping(
    mappings: Dict[str, Any], reference: Any
) -> Dict[str, Dict[str, FieldSpec]]:
    if reference is None:
        reference = "default"

    if isinstance(reference, str):
        if reference not in mappings:
            raise KeyError(f"Mapping '{reference}' is not defined in the configuration.")
        mapping = deepcopy(mappings[reference])
    elif isinstance(reference, dict):
        mapping = deepcopy(reference)
    else:
        raise TypeError("Mapping reference must be either a string key or a dictionary.")

    conversion_entries = {
        key: deepcopy(value)
        for key, value in mappings.items()
        if isinstance(key, str) and is_conversion_key(key)
    }

    for key, value in conversion_entries.items():
        mapping.setdefault(key, value)

    return normalize_mapping(mapping)


def build_ordered_keys(mapping: Dict[str, Dict[str, FieldSpec]]) -> List[str]:
    ordered_keys: List[str] = []
    for section in FIELD_SECTION_PREFIXES:
        ordered_keys.extend(mapping[section].keys())
    ordered_keys.extend(GENERATED_FIELDS)
    return ordered_keys


def make_record_from_row(
    row: Dict[str, Any], mapping: Dict[str, Dict[str, FieldSpec]]
) -> Dict[str, Any]:
    record: Dict[str, Any] = {}

    for db_key, field in mapping["string"].items():
        value = normalize_cell_value(row.get(field.csv_header))
        value = apply_conversion(value, field.conversion)
        record[db_key] = jaconv.h2z(value) if value else None

    for db_key, field in mapping["text"].items():
        value = normalize_cell_value(row.get(field.csv_header))
        value = apply_conversion(value, field.conversion)
        value = replace_invalid_shiftjis_chars(value)
        record[db_key] = jaconv.h2z(value) if value else None

    for db_key, field in mapping["integer"].items():
        value = normalize_cell_value(row.get(field.csv_header))
        converted_value = apply_conversion(value, field.conversion)
        record[db_key] = parse_integer_cell(converted_value, field.clamp)

    for db_key, field in mapping["date"].items():
        value = normalize_cell_value(row.get(field.csv_header))
        parsed = parse_datetime_value(value)
        record[db_key] = parsed.date() if parsed else None

    for db_key, field in mapping["datetime"].items():
        value = normalize_cell_value(row.get(field.csv_header))
        parsed = parse_datetime_value(value)
        record[db_key] = parsed if parsed else None

    return record


def build_enquete_key(
    row: Dict[str, Any], mapping: Dict[str, Dict[str, FieldSpec]]
) -> Optional[str]:
    room_field = (
        mapping["string"].get("room_number")
        or mapping["integer"].get("room_number")
    )
    start_field = mapping["date"].get("start_date") or mapping["datetime"].get("start_date")

    if not room_field or not start_field:
        return None

    room_value_raw = normalize_cell_value(row.get(room_field.csv_header))
    room_value_raw = apply_conversion(room_value_raw, room_field.conversion)

    if room_field in mapping["integer"].values():
        parsed_room = parse_integer_cell(room_value_raw, room_field.clamp)
        room_value = str(parsed_room) if parsed_room is not None else None
    else:
        normalized = jaconv.z2h(room_value_raw, digit=True, ascii=True)
        room_value = normalized if normalized and normalized.isdecimal() else None

    if not room_value:
        return None

    start_date_value = normalize_cell_value(row.get(start_field.csv_header))
    parsed = parse_datetime_value(start_date_value)
    if not parsed:
        return None

    return f"{room_value}-{parsed.strftime('%Y%m%d')}-1"


def build_generated_fields(
    row: Dict[str, Any], mapping: Dict[str, Dict[str, FieldSpec]], facility_code: int
) -> Dict[str, Any]:
    return {
        "facility_code": facility_code,
        "enquete_key": build_enquete_key(row, mapping),
        "import_date": datetime.now(),
    }


def sanitize_mapping_reference(reference: Any) -> Any:
    if isinstance(reference, str):
        reference = reference.strip()
        if not reference:
            return None
    return reference


def normalize_optional_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    normalized = str(value).strip()
    return normalized or None


def open_worksheet(
    client: gspread.Client,
    facility_config: Dict[str, Any],
    default_worksheet: Optional[str],
):

    spreadsheet_config = facility_config.get("spreadsheet") or {}
    spreadsheet_id = normalize_optional_string(
        spreadsheet_config.get("id") or facility_config.get("spreadsheet_id")
    )

    if not spreadsheet_id:
        raise ValueError("'spreadsheet.id' is required in each facility configuration.")

    workbook = client.open_by_key(spreadsheet_id)

    worksheet_name = normalize_optional_string(
        spreadsheet_config.get("worksheet") or facility_config.get("worksheet")
    )
    if not worksheet_name:
        worksheet_name = default_worksheet

    if worksheet_name:
        return workbook.worksheet(worksheet_name)

    return workbook.sheet1


def build_facility_filter(corporation: str, facility: str) -> str:
    return f"{corporation}.{facility}"


def facility_selected(
    corporation: str, facility: str, filters: Set[str]
) -> bool:
    if not filters:
        return True

    return facility in filters or build_facility_filter(corporation, facility) in filters


def build_header_index(headers: Iterable[Any]) -> Dict[str, int]:
    index: Dict[str, int] = {}
    for position, header in enumerate(headers):
        normalized = normalize_header_name(header)
        if not normalized or normalized in index:
            continue
        index[normalized] = position
    return index


def extract_required_headers(mapping: Dict[str, Dict[str, FieldSpec]]) -> Set[str]:
    headers: Set[str] = set()
    for section in FIELD_SECTION_PREFIXES:
        for field in mapping[section].values():
            if field.csv_header:
                headers.add(field.csv_header)
    return headers


def read_records(
    worksheet: gspread.Worksheet, required_headers: Set[str]
) -> List[Dict[str, Any]]:
    rows = worksheet.get_all_values()
    if not rows:
        return []

    header_index = build_header_index(rows[0])
    missing = sorted(required_headers - set(header_index))
    if missing:
        raise ValueError(
            "Missing required header(s) in worksheet: " + ", ".join(missing)
        )

    records: List[Dict[str, Any]] = []
    for raw_row in rows[1:]:
        if not any(normalize_cell_value(cell) for cell in raw_row):
            continue

        record: Dict[str, Any] = {}
        for header in required_headers:
            index = header_index[header]
            record[header] = raw_row[index] if index < len(raw_row) else ""
        records.append(record)

    return records


def import_facility(
    connection: psycopg2.extensions.connection,
    cursor: psycopg2.extensions.cursor,
    client: gspread.Client,
    corporation: str,
    facility_name: str,
    facility_config: Dict[str, Any],
    corporation_config: Dict[str, Any],
    base_mappings: Dict[str, Any],
    default_worksheet: Optional[str],
    default_table_name: str,
    cleared_facilities: Dict[str, Set[int]],
) -> None:
    if "facility_code" not in facility_config:
        raise ValueError("'facility_code' is required in each facility configuration.")

    facility_code = facility_config["facility_code"]

    available_mappings: Dict[str, Any] = {}
    merge_mapping_catalog(available_mappings, base_mappings)
    merge_mapping_catalog(available_mappings, corporation_config.get("mappings", {}) or {})
    merge_mapping_catalog(available_mappings, facility_config.get("mappings", {}) or {})

    facility_default_table = normalize_optional_string(facility_config.get("table"))
    default_table = facility_default_table or default_table_name

    jobs: List[ImportJob] = []

    imports_config = facility_config.get("imports")
    if imports_config:
        if not isinstance(imports_config, list):
            raise TypeError("'imports' configuration must be a list if provided.")

        for entry in imports_config:
            table_override = default_table
            mapping_reference: Any = None
            mapping_label: Optional[str] = None

            if isinstance(entry, dict) and not is_mapping_definition_dict(entry):
                table_override = (
                    normalize_optional_string(entry.get("table")) or default_table
                )
                if "mapping" in entry:
                    mapping_reference = entry["mapping"]
                    if isinstance(mapping_reference, str):
                        mapping_label = mapping_reference
                else:
                    mapping_reference = entry
            else:
                mapping_reference = entry

            mapping_reference = sanitize_mapping_reference(mapping_reference)
            mapping = resolve_mapping(available_mappings, mapping_reference)
            if mapping_label is None and isinstance(mapping_reference, str):
                mapping_label = mapping_reference

            jobs.append(
                ImportJob(
                    table=table_override,
                    mapping=mapping,
                    mapping_name=mapping_label,
                )
            )
    else:
        mapping_reference = sanitize_mapping_reference(facility_config.get("mapping"))
        if mapping_reference is None:
            mapping_reference = sanitize_mapping_reference(
                corporation_config.get("mapping")
            )
        mapping = resolve_mapping(available_mappings, mapping_reference)
        mapping_label = mapping_reference if isinstance(mapping_reference, str) else None
        jobs.append(
            ImportJob(table=default_table, mapping=mapping, mapping_name=mapping_label)
        )

    if not jobs:
        logger.info(
            "No import jobs configured for %s/%s. Skipping.",
            corporation,
            facility_name,
        )
        return

    worksheet = open_worksheet(client, facility_config, default_worksheet)

    required_headers: Set[str] = set()
    for job in jobs:
        required_headers.update(extract_required_headers(job.mapping))

    records = read_records(worksheet, required_headers)

    logger.info(
        "Fetched %d rows from %s/%s", len(records), corporation, facility_name
    )

    for job in jobs:
        ordered_keys = list(build_ordered_keys(job.mapping))
        insert_query = f"INSERT INTO {job.table} ({', '.join(ordered_keys)}) VALUES %s"

        cleared_for_table = cleared_facilities.setdefault(job.table, set())
        if facility_code not in cleared_for_table:
            cursor.execute(
                f"DELETE FROM {job.table} WHERE facility_code = %s",
                (facility_code,),
            )
            cleared_for_table.add(facility_code)

        if not records:
            connection.commit()
            logger.info(
                "No rows to import for %s/%s (table: %s, mapping: %s).",
                corporation,
                facility_name,
                job.table,
                job.mapping_name or "default",
            )
            continue

        buffer: List[List[Any]] = []
        for row in records:
            record = make_record_from_row(row, job.mapping)
            record.update(build_generated_fields(row, job.mapping, facility_code))
            buffer.append([record.get(key) for key in ordered_keys])

        if not buffer:
            connection.commit()
            logger.info(
                "No rows to import for %s/%s (table: %s, mapping: %s).",
                corporation,
                facility_name,
                job.table,
                job.mapping_name or "default",
            )
            continue

        extras.execute_values(cursor, insert_query, buffer)
        connection.commit()

        logger.info(
            "Imported %d rows for %s/%s into %s (mapping: %s).",
            len(buffer),
            corporation,
            facility_name,
            job.table,
            job.mapping_name or "default",
        )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = build_parser()
    args = parser.parse_args()

    base_path = os.path.dirname(__file__)
    config_path = os.path.join(base_path, "config.yaml")
    config = load_config(config_path)

    mappings = config.get("mappings", {})
    corporations = config.get("corporations", {})

    if not corporations:
        raise ValueError("No corporations configured. Please update config.yaml.")

    selected_corporations = set(args.corporations or [])
    unknown_corporations = selected_corporations - set(corporations.keys())
    if unknown_corporations:
        raise ValueError(
            "Unknown corporation(s) specified: " + ", ".join(sorted(unknown_corporations))
        )

    facility_filters = set(args.facilities or [])

    if facility_filters:
        valid_facility_filters: Set[str] = set()
        for corporation_name, corporation_config in corporations.items():
            facilities_config = corporation_config.get("facilities", {})
            for facility_name in facilities_config.keys():
                valid_facility_filters.add(facility_name)
                valid_facility_filters.add(
                    build_facility_filter(corporation_name, facility_name)
                )

        unknown_facilities = facility_filters - valid_facility_filters
        if unknown_facilities:
            raise ValueError(
                "Unknown facility filter(s) specified: "
                + ", ".join(sorted(unknown_facilities))
            )

    client = create_gspread_client(base_path)
    default_worksheet = normalize_optional_string(
        (config.get("google", {}) or {}).get("worksheet")
    )

    for corporation, corporation_config in corporations.items():
        if selected_corporations and corporation not in selected_corporations:
            continue

        db_config = corporation_config.get("db")
        if not db_config:
            logger.warning("Skipping %s: missing database configuration.", corporation)
            continue

        facilities = corporation_config.get("facilities", {})
        if not facilities:
            logger.warning("Skipping %s: no facilities configured.", corporation)
            continue

        logger.info("Processing corporation %s", corporation)

        connection = psycopg2.connect(**db_config)
        try:
            cleared_facilities: Dict[str, Set[int]] = defaultdict(set)
            for facility_name, facility_config in facilities.items():
                if not facility_selected(corporation, facility_name, facility_filters):
                    continue

                logger.info("Processing facility %s/%s", corporation, facility_name)

                try:
                    with connection.cursor() as cursor:
                        import_facility(
                            connection,
                            cursor,
                            client,
                            corporation,
                            facility_name,
                            facility_config,
                            corporation_config,
                            mappings,
                            default_worksheet,
                            args.table,
                            cleared_facilities,
                        )
                except ValueError as exc:
                    connection.rollback()
                    logger.warning(
                        "Skipping facility %s/%s: %s", corporation, facility_name, exc
                    )
                except Exception:
                    connection.rollback()
                    logger.exception(
                        "Failed to import data for facility %s/%s", corporation, facility_name
                    )
                    raise
        finally:
            connection.close()


if __name__ == "__main__":
    main()
