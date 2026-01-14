import logging
import os
from argparse import ArgumentParser
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set

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

ENGLISH_TO_JAPANESE_CONVERSIONS = (
    {
        "Very Good": "非常に良い",
        "Good": "良い",
        "Average": "普通",
        "Poor": "悪い",
        "Very Poor": "非常に悪い",
    },
    {
        "Yes": "はい",
        "No": "いいえ",
    },
)

logger = logging.getLogger(__name__)


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


def convert_english_to_japanese(value: str) -> str:
    for conversion_table in ENGLISH_TO_JAPANESE_CONVERSIONS:
        if value in conversion_table:
            return conversion_table[value]
    return value


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


def normalize_mapping(mapping: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    normalized: Dict[str, Dict[str, str]] = {}
    for section in ("string", "text", "integer", "date", "datetime"):
        value = mapping.get(section) or {}
        if not isinstance(value, dict):
            raise TypeError(f"Mapping section '{section}' must be a dictionary.")
        normalized[section] = {
            db_key: normalize_header_name(csv_key) for db_key, csv_key in value.items()
        }
    return normalized


def resolve_mapping(mappings: Dict[str, Any], reference: Any) -> Dict[str, Dict[str, str]]:
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

    return normalize_mapping(mapping)


def build_ordered_keys(mapping: Dict[str, Dict[str, str]]) -> List[str]:
    ordered_keys: List[str] = []
    for section in ("string", "text", "integer", "date", "datetime"):
        ordered_keys.extend(mapping[section].keys())
    ordered_keys.extend(GENERATED_FIELDS)
    return ordered_keys


def normalize_value_conversions(
    conversions: Optional[Dict[str, Dict[str, Any]]]
) -> Dict[str, Dict[str, Any]]:
    if not conversions:
        return {}
    normalized: Dict[str, Dict[str, Any]] = {}
    for db_key, mapping in conversions.items():
        if mapping is None:
            continue
        if not isinstance(mapping, dict):
            raise TypeError(f"Value conversion for '{db_key}' must be a dictionary.")
        normalized[db_key] = {
            normalize_cell_value(source): target for source, target in mapping.items()
        }
    return normalized


def apply_value_conversion(
    value: Any, db_key: str, conversions: Dict[str, Dict[str, Any]]
) -> Any:
    if not conversions:
        return value
    table = conversions.get(db_key)
    if not table:
        return value
    normalized = normalize_cell_value(value)
    if not normalized:
        return value
    return table.get(normalized, value)


def make_record_from_row(
    row: Dict[str, Any],
    mapping: Dict[str, Dict[str, str]],
    value_conversions: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    record: Dict[str, Any] = {}
    normalized_conversions = value_conversions or {}

    for db_key, csv_key in mapping["string"].items():
        value = apply_value_conversion(row.get(csv_key), db_key, normalized_conversions)
        value = normalize_cell_value(value)
        if value:
            value = convert_english_to_japanese(value)
        record[db_key] = jaconv.h2z(value) if value else None

    for db_key, csv_key in mapping["text"].items():
        value = apply_value_conversion(row.get(csv_key), db_key, normalized_conversions)
        value = normalize_cell_value(value)
        value = replace_invalid_shiftjis_chars(value)
        record[db_key] = jaconv.h2z(value) if value else None

    for db_key, csv_key in mapping["integer"].items():
        value = apply_value_conversion(row.get(csv_key), db_key, normalized_conversions)
        value = normalize_cell_value(value)
        if value:
            normalized_value = jaconv.z2h(value, digit=True, ascii=True)
            if db_key == "comprehensive_evaluation":
                record[db_key] = normalize_comprehensive_evaluation(normalized_value)
            else:
                record[db_key] = int(normalized_value) if normalized_value.isdecimal() else None
        else:
            record[db_key] = None

    for db_key, csv_key in mapping["date"].items():
        value = apply_value_conversion(row.get(csv_key), db_key, normalized_conversions)
        value = normalize_cell_value(value)
        parsed = parse_datetime_value(value)
        record[db_key] = parsed.date() if parsed else None

    for db_key, csv_key in mapping["datetime"].items():
        value = apply_value_conversion(row.get(csv_key), db_key, normalized_conversions)
        value = normalize_cell_value(value)
        parsed = parse_datetime_value(value)
        record[db_key] = parsed if parsed else None

    return record


def build_enquete_key(
    row: Dict[str, Any],
    mapping: Dict[str, Dict[str, str]],
    facility_code: int,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
    value_conversions: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Optional[str]:
    room_header = mapping["string"].get("room_number") or mapping["integer"].get(
        "room_number"
    )
    start_date_header = mapping["date"].get("start_date") or mapping["datetime"].get("start_date")

    if not room_header or not start_date_header:
        return None

    room_raw_value = apply_value_conversion(
        row.get(room_header), "room_number", value_conversions or {}
    )
    room_value = jaconv.z2h(normalize_cell_value(room_raw_value), digit=True, ascii=True)
    if not room_value or not room_value.isdecimal():
        return None

    start_date_value = normalize_cell_value(row.get(start_date_header))
    parsed = parse_datetime_value(start_date_value)
    if not parsed:
        return None

    base_key = f"{room_value}-{parsed.strftime('%Y%m%d')}-{facility_code}"
    if prefix:
        base_key = f"{prefix}{base_key}"
    if suffix:
        base_key = f"{base_key}-{suffix}"
    return base_key


def build_generated_fields(
    row: Dict[str, Any],
    mapping: Dict[str, Dict[str, str]],
    facility_code: int,
    enquete_key_prefix: Optional[str] = None,
    enquete_key_suffix: Optional[str] = None,
    value_conversions: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    return {
        "facility_code": facility_code,
        "enquete_key": build_enquete_key(
            row,
            mapping,
            facility_code,
            prefix=enquete_key_prefix,
            suffix=enquete_key_suffix,
            value_conversions=value_conversions,
        ),
        "import_date": datetime.now(),
    }


def normalize_comprehensive_evaluation(value: str) -> Optional[int]:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None

    if numeric_value <= 0:
        return 0
    if numeric_value >= 100:
        return 100
    return int(numeric_value)


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


def normalize_language_key(value: Any) -> Optional[str]:
    normalized = normalize_optional_string(value)
    if normalized is None:
        return None
    return normalized.casefold()


def build_language_mappings(
    available_mappings: Dict[str, Any],
    mapping_reference: Any,
    language_mappings: Dict[str, Any],
) -> Dict[str, Dict[str, Dict[str, str]]]:
    resolved: Dict[str, Dict[str, Dict[str, str]]] = {}
    for language_key, reference in language_mappings.items():
        normalized_key = normalize_language_key(language_key)
        if normalized_key is None:
            continue
        if normalized_key == "default":
            normalized_key = "default"
        resolved[normalized_key] = resolve_mapping(available_mappings, reference)

    if "default" not in resolved and mapping_reference is not None:
        resolved["default"] = resolve_mapping(available_mappings, mapping_reference)

    return resolved


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


def extract_required_headers(mapping: Dict[str, Dict[str, str]]) -> Set[str]:
    headers: Set[str] = set()
    for section in ("string", "text", "integer", "date", "datetime"):
        headers.update(mapping[section].values())
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
    deleted_facility_codes: Set[Any],
    table_name: str,
) -> None:
    if "facility_code" not in facility_config:
        raise ValueError("'facility_code' is required in each facility configuration.")

    facility_code = facility_config["facility_code"]

    available_mappings: Dict[str, Any] = dict(base_mappings)
    available_mappings.update(corporation_config.get("mappings", {}) or {})

    facility_mappings = facility_config.get("mappings", {}) or {}
    available_mappings.update(facility_mappings)

    mapping_reference = sanitize_mapping_reference(facility_config.get("mapping"))
    if mapping_reference is None:
        mapping_reference = sanitize_mapping_reference(
            corporation_config.get("mapping")
        )

    language_column = normalize_optional_string(facility_config.get("language_column"))
    language_mappings_config = facility_config.get("language_mappings", {}) or {}
    value_conversions = normalize_value_conversions(
        facility_config.get("value_conversions")
    )

    if language_column:
        if not language_mappings_config and mapping_reference is None:
            raise ValueError(
                "language_column is set but no language_mappings or default mapping is defined."
            )
        resolved_language_mappings = build_language_mappings(
            available_mappings, mapping_reference, language_mappings_config
        )
        if not resolved_language_mappings:
            raise ValueError("language_mappings did not resolve to any valid mappings.")
        required_headers = {language_column}
        for mapping in resolved_language_mappings.values():
            required_headers.update(extract_required_headers(mapping))
        mapping_for_schema = resolved_language_mappings.get("default") or next(
            iter(resolved_language_mappings.values())
        )
    else:
        mapping_for_schema = resolve_mapping(available_mappings, mapping_reference)
        resolved_language_mappings = {"default": mapping_for_schema}
        required_headers = extract_required_headers(mapping_for_schema)

    worksheet = open_worksheet(client, facility_config, default_worksheet)
    records = read_records(worksheet, required_headers)

    logger.info(
        "Fetched %d rows from %s/%s", len(records), corporation, facility_name
    )

    ordered_keys = list(build_ordered_keys(mapping_for_schema))
    facility_table = normalize_optional_string(facility_config.get("table"))
    if not facility_table:
        facility_table = table_name

    insert_query = f"INSERT INTO {facility_table} ({', '.join(ordered_keys)}) VALUES %s"

    should_delete = facility_config.get("delete", True)
    enquete_key_prefix = facility_config.get("enquete_key_prefix")
    enquete_key_suffix = facility_config.get("enquete_key_suffix")

    buffer: List[List[Any]] = []
    for row in records:
        if language_column:
            language_value = normalize_language_key(row.get(language_column))
            mapping = resolved_language_mappings.get(language_value) or resolved_language_mappings.get(
                "default"
            )
            if mapping is None:
                logger.warning(
                    "Skipping row with unknown language '%s' in %s/%s.",
                    language_value or "",
                    corporation,
                    facility_name,
                )
                continue
        else:
            mapping = mapping_for_schema

        record = make_record_from_row(row, mapping, value_conversions=value_conversions)
        generated_fields = build_generated_fields(
            row,
            mapping,
            facility_code,
            enquete_key_prefix=enquete_key_prefix,
            enquete_key_suffix=enquete_key_suffix,
            value_conversions=value_conversions,
        )
        record.update(generated_fields)
        buffer.append([record.get(key) for key in ordered_keys])

    if should_delete:
        cursor.execute(
            f"DELETE FROM {facility_table} WHERE facility_code = %s", (facility_code,)
        )
    else:
        logger.info(
            "Skipping deletion for %s/%s because delete is disabled in the configuration.",
            corporation,
            facility_name,
        )

    if not buffer:
        connection.commit()
        logger.info(
            "No rows to import for %s/%s. Existing records have been cleared.",
            corporation,
            facility_name,
        )
        return

    extras.execute_values(cursor, insert_query, buffer)
    connection.commit()

    logger.info(
        "Imported %d rows for %s/%s.", len(buffer), corporation, facility_name
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
            deleted_facility_codes: Set[Any] = set()
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
                            deleted_facility_codes,
                            args.table,
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
