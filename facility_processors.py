from __future__ import annotations

from typing import Any, Dict, Optional

from converters import normalize_cell_value

SANKOH_FACILITY_NAME_HEADER = "宿泊施設"
SANKOH_FACILITY_CODE_MAP = {
    "夢乃井": 1,
    "夕やけこやけ": 2,
    "祥吉": 3,
    "加里屋旅館Q": 4,
}


def resolve_facility_code(
    row: Dict[str, Any],
    facility_config: Dict[str, Any],
    default_facility_code: int,
) -> int:
    processor = normalize_cell_value(facility_config.get("special_processor")).casefold()
    if processor == "sankoh":
        return _resolve_sankoh_facility_code(row, facility_config) or default_facility_code
    return default_facility_code


def get_additional_required_headers(facility_config: Dict[str, Any]) -> set[str]:
    processor = normalize_cell_value(facility_config.get("special_processor")).casefold()
    if processor == "sankoh":
        source_header = normalize_cell_value(
            facility_config.get("facility_code_source") or SANKOH_FACILITY_NAME_HEADER
        )
        return {source_header}
    return set()


def _resolve_sankoh_facility_code(
    row: Dict[str, Any], facility_config: Dict[str, Any]
) -> Optional[int]:
    source_header = normalize_cell_value(
        facility_config.get("facility_code_source") or SANKOH_FACILITY_NAME_HEADER
    )
    facility_name = normalize_cell_value(row.get(source_header))
    if not facility_name:
        return None
    return SANKOH_FACILITY_CODE_MAP.get(facility_name)
