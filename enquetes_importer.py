import logging
import os
from argparse import ArgumentParser
from typing import Any, Dict, Iterable, List, Optional, Set

import gspread
import psycopg2
from oauth2client.service_account import ServiceAccountCredentials

from common_processors import (
    build_generated_fields,
    build_ordered_keys,
    make_record_from_row,
    normalize_value_conversions,
    resolve_mapping,
)
from config_loader import load_config
from converters import normalize_cell_value, normalize_header_name
from db_importer import (
    delete_existing_rows,
    execute_before_insert_sql,
    insert_rows,
    normalize_optional_string,
)
from facility_processors import (
    apply_facility_overrides,
    build_facility_value_conversions,
    get_additional_required_headers,
    resolve_facility_code,
)

DEFAULT_TABLE_NAME = "enquetes"
DEFAULT_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

logger = logging.getLogger(__name__)


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Import enquete spreadsheet data into the configured databases."
    )
    parser.add_argument(
        "--config",
        default="config_db.yaml",
        help=(
            "Configuration YAML path. Relative paths are resolved from the script directory. "
            "Defaults to '%(default)s'."
        ),
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


def create_gspread_client(base_path: str) -> gspread.Client:
    json_file = os.path.join(base_path, "client_secret.json")
    if not os.path.exists(json_file):
        raise FileNotFoundError(f"Google service account credentials not found: {json_file}")

    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_file, DEFAULT_SCOPE)
    return gspread.authorize(credentials)


def sanitize_mapping_reference(reference: Any) -> Any:
    if isinstance(reference, str):
        reference = reference.strip()
        if not reference:
            return None
    return reference


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

    return resolved


def build_language_mappings_from_mapping_names(
    available_mappings: Dict[str, Any], mapping_names: Iterable[str]
) -> Dict[str, Dict[str, Dict[str, str]]]:
    resolved: Dict[str, Dict[str, Dict[str, str]]] = {}
    for mapping_name in mapping_names:
        normalized_name = normalize_language_key(mapping_name)
        if normalized_name is None:
            continue
        mapping = resolve_mapping(available_mappings, mapping_name)
        lowered = normalized_name

        if "japanese" in lowered or "日本語" in lowered or lowered.endswith("_ja"):
            resolved["日本語"] = mapping
            resolved["japanese"] = mapping
        if "english" in lowered or "英語" in lowered or lowered.endswith("_en"):
            resolved["english"] = mapping
        if lowered in ("default",):
            resolved["default"] = mapping

    return resolved


def open_worksheet(
    client: gspread.Client,
    facility_config: Dict[str, Any],
    default_worksheet: Optional[str],
):
    spreadsheet_config = facility_config.get("spreadsheet") or {}
    spreadsheet_id = normalize_optional_string(
        facility_config.get("spreadsheet_id") or spreadsheet_config.get("id")
    )

    if not spreadsheet_id:
        raise ValueError("'spreadsheet_id' is required in each facility configuration.")

    workbook = client.open_by_key(spreadsheet_id)

    worksheet_name = normalize_optional_string(
        facility_config.get("worksheet") or spreadsheet_config.get("worksheet")
    )
    if not worksheet_name:
        worksheet_name = default_worksheet

    if worksheet_name:
        return workbook.worksheet(worksheet_name)

    return workbook.sheet1


def build_facility_filter(corporation: str, facility: str) -> str:
    return f"{corporation}.{facility}"


def facility_selected(corporation: str, facility: str, filters: Set[str]) -> bool:
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


def read_records(worksheet: gspread.Worksheet, required_headers: Set[str]) -> List[Dict[str, Any]]:
    rows = worksheet.get_all_values()
    if not rows:
        return []

    header_index = build_header_index(rows[0])
    missing = sorted(required_headers - set(header_index))
    if missing:
        raise ValueError("Missing required header(s) in worksheet: " + ", ".join(missing))

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
    table_name: str,
) -> None:
    facility_config = apply_facility_overrides(corporation, facility_name, facility_config)

    if "facility_code" not in facility_config:
        raise ValueError("'facility_code' is required in each facility configuration.")

    default_facility_code = facility_config["facility_code"]

    available_mappings: Dict[str, Any] = dict(base_mappings)
    available_mappings.update(corporation_config.get("mappings", {}) or {})
    available_mappings.update(facility_config.get("mappings", {}) or {})

    mapping_reference = sanitize_mapping_reference(facility_config.get("mapping"))
    if mapping_reference is None:
        mapping_reference = sanitize_mapping_reference(corporation_config.get("mapping"))

    language_column = normalize_optional_string(facility_config.get("language_column"))
    language_mappings_config = facility_config.get("language_mappings", {}) or {}
    value_conversions = build_facility_value_conversions(
        facility_config,
        normalize_value_conversions(facility_config.get("value_conversions")),
    )

    if language_column:
        if not language_mappings_config:
            language_mappings_config = {
                key: key for key in (facility_config.get("mappings", {}) or {}).keys()
            }
        resolved_language_mappings = build_language_mappings(
            available_mappings, mapping_reference, language_mappings_config
        )
        if "日本語" not in resolved_language_mappings and "english" not in resolved_language_mappings:
            inferred = build_language_mappings_from_mapping_names(
                available_mappings, language_mappings_config.values()
            )
            resolved_language_mappings.update(inferred)
        if not resolved_language_mappings:
            raise ValueError("language_mappings did not resolve to any valid mappings.")
        required_headers = {language_column}
        for mapping in resolved_language_mappings.values():
            required_headers.update(extract_required_headers(mapping))
        mapping_for_schema = resolved_language_mappings.get("default") or next(
            iter(resolved_language_mappings.values())
        )
    else:
        if mapping_reference is None:
            raise ValueError(
                "Mapping is required when language_column is not configured."
            )
        mapping_for_schema = resolve_mapping(available_mappings, mapping_reference)
        resolved_language_mappings = {"default": mapping_for_schema}
        required_headers = extract_required_headers(mapping_for_schema)

    required_headers.update(get_additional_required_headers(facility_config))

    worksheet = open_worksheet(client, facility_config, default_worksheet)
    records = read_records(worksheet, required_headers)

    logger.info("Fetched %d rows from %s/%s", len(records), corporation, facility_name)

    ordered_keys = list(build_ordered_keys(mapping_for_schema))
    fixed_values = facility_config.get("fixed_values")
    if isinstance(fixed_values, dict):
        for key in fixed_values:
            if key not in ordered_keys:
                ordered_keys.append(key)
    facility_table = normalize_optional_string(facility_config.get("table")) or table_name
    should_delete = facility_config.get("delete", True)

    execute_before_insert_sql(cursor, facility_config.get("before_insert_sql"), corporation, facility_name)
    delete_existing_rows(
        cursor, facility_table, default_facility_code, should_delete, corporation, facility_name
    )

    buffer: List[List[Any]] = []
    for row in records:
        if language_column:
            language_value = normalize_language_key(row.get(language_column))
            mapping = resolved_language_mappings.get(language_value) or resolved_language_mappings.get("default")
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

        actual_facility_code = resolve_facility_code(row, facility_config, default_facility_code)
        record = make_record_from_row(row, mapping, value_conversions=value_conversions)
        if isinstance(fixed_values, dict):
            record.update(fixed_values)
        record.update(
            build_generated_fields(
                row,
                mapping,
                actual_facility_code,
                enquete_key_prefix=facility_config.get("enquete_key_prefix"),
                enquete_key_suffix=facility_config.get("enquete_key_suffix"),
                value_conversions=value_conversions,
            )
        )
        buffer.append([record.get(key) for key in ordered_keys])

    insert_rows(connection, cursor, facility_table, ordered_keys, buffer, corporation, facility_name)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = build_parser()
    args = parser.parse_args()

    base_path = os.path.dirname(__file__)
    config_path = args.config
    if not os.path.isabs(config_path):
        config_path = os.path.join(base_path, config_path)
    config = load_config(config_path)

    mappings = config.get("mappings", {})
    corporations = config.get("corporations", {})
    if not corporations:
        raise ValueError("No corporations configured. Please update config file.")

    selected_corporations = set(args.corporations or [])
    unknown_corporations = selected_corporations - set(corporations.keys())
    if unknown_corporations:
        raise ValueError("Unknown corporation(s) specified: " + ", ".join(sorted(unknown_corporations)))

    facility_filters = set(args.facilities or [])
    if facility_filters:
        valid_facility_filters: Set[str] = set()
        for corporation_name, corporation_config in corporations.items():
            facilities_config = corporation_config.get("facilities", {})
            for facility_name in facilities_config.keys():
                valid_facility_filters.add(facility_name)
                valid_facility_filters.add(build_facility_filter(corporation_name, facility_name))

        unknown_facilities = facility_filters - valid_facility_filters
        if unknown_facilities:
            raise ValueError("Unknown facility filter(s) specified: " + ", ".join(sorted(unknown_facilities)))

    client = create_gspread_client(base_path)
    default_worksheet = normalize_optional_string((config.get("google", {}) or {}).get("worksheet"))

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
                        )
                except ValueError as exc:
                    connection.rollback()
                    logger.warning("Skipping facility %s/%s: %s", corporation, facility_name, exc)
                except Exception:
                    connection.rollback()
                    logger.exception("Failed to import data for facility %s/%s", corporation, facility_name)
                    raise
        finally:
            connection.close()


if __name__ == "__main__":
    main()
