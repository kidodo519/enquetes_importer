from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import re
from typing import Any, Dict, Optional

import jaconv

from converters import (
    normalize_cell_value,
    normalize_comprehensive_evaluation,
    normalize_header_name,
    parse_datetime_value,
)

GENERATED_FIELDS = ("facility_code", "enquete_key", "import_date")
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


def convert_english_to_japanese(value: str) -> str:
    for conversion_table in ENGLISH_TO_JAPANESE_CONVERSIONS:
        if value in conversion_table:
            return conversion_table[value]
    return value


def replace_invalid_shiftjis_chars(value: str, replace_with: str = "?") -> str:
    return "".join(
        char if char.encode("shift_jis", errors="ignore") else replace_with for char in value
    )


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
        raise ValueError("Mapping reference is required.")

    if isinstance(reference, str):
        if reference not in mappings:
            raise KeyError(f"Mapping '{reference}' is not defined in the configuration.")
        mapping = deepcopy(mappings[reference])
    elif isinstance(reference, dict):
        mapping = deepcopy(reference)
    else:
        raise TypeError("Mapping reference must be either a string key or a dictionary.")

    return normalize_mapping(mapping)


def build_ordered_keys(mapping: Dict[str, Dict[str, str]]) -> list[str]:
    ordered_keys: list[str] = []
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
    keep_room_leading_zeros: bool = False,
) -> str:
    room_db_key: Optional[str] = None
    room_header = (
        mapping["string"].get("room_number")
        or mapping["integer"].get("room_number")
        or mapping["text"].get("room_number")
    )
    if room_header:
        room_db_key = "room_number"
    else:
        room_header = (
            mapping["string"].get("room_code")
            or mapping["integer"].get("room_code")
            or mapping["text"].get("room_code")
        )
        if room_header:
            room_db_key = "room_code"
    start_date_header = mapping["date"].get("start_date") or mapping["datetime"].get("start_date")

    if not room_header or not start_date_header or room_db_key is None:
        raise ValueError(
            "enquete_key generation requires room_number/room_code and start_date mappings."
        )

    room_raw_value = apply_value_conversion(
        row.get(room_header), room_db_key, value_conversions or {}
    )
    room_value = jaconv.z2h(normalize_cell_value(room_raw_value), digit=True, ascii=True)
    if keep_room_leading_zeros and room_value and not room_value.isdecimal():
        matched = re.match(r"^(\d+)", room_value)
        if matched:
            room_value = matched.group(1)
    if not room_value or not room_value.isdecimal():
        raise ValueError(f"enquete_key generation failed: invalid room value '{room_raw_value}'.")
    if not keep_room_leading_zeros:
        room_value = str(int(room_value))

    start_date_value = normalize_cell_value(row.get(start_date_header))
    parsed = parse_datetime_value(start_date_value)
    if not parsed:
        raise ValueError(
            f"enquete_key generation failed: invalid start_date value '{start_date_value}'."
        )

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
    enquete_key_keep_room_leading_zeros: bool = False,
) -> Dict[str, Any]:
    enquete_key = build_enquete_key(
        row,
        mapping,
        facility_code,
        prefix=enquete_key_prefix,
        suffix=enquete_key_suffix,
        value_conversions=value_conversions,
        keep_room_leading_zeros=enquete_key_keep_room_leading_zeros,
    )
    return {
        "facility_code": facility_code,
        "enquete_key": enquete_key,
        "import_date": datetime.now(),
    }
