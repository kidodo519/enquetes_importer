from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable, Dict, Optional

from converters import normalize_cell_value

FacilityCodeResolver = Callable[[Dict[str, Any], Dict[str, Any]], Optional[int]]
RequiredHeaderProvider = Callable[[Dict[str, Any]], set[str]]
ValueConversionProvider = Callable[[Dict[str, Any]], Dict[str, Dict[str, Any]]]

DEFAULT_FACILITY_NAME_HEADER = "宿泊施設"
FACILITY_CODE_MAPS = {
    "sankoh": {
        "夢乃井": 1,
        "夕やけこやけ": 2,
        "祥吉": 3,
        "加里屋旅館Q": 4,
    }
}

GOSHOBO_ROOM_NUMBER_CONVERSIONS = {
    "1号棟": "1",
    "2号棟": "2",
    "3号棟": "3",
    "4号棟": "4",
    "5号棟": "5",
    "6号棟": "6",
    "7号棟": "7",
    "8号棟": "8",
    "9号棟": "9",
    "10号棟": "10",
}


def resolve_facility_code(
    row: Dict[str, Any],
    facility_config: Dict[str, Any],
    default_facility_code: int,
) -> int:
    processor = normalize_cell_value(
        facility_config.get("facility_code_processor") or facility_config.get("special_processor")
    ).casefold()
    if processor:
        resolver = FACILITY_CODE_RESOLVERS.get(processor)
        if resolver:
            return resolver(row, facility_config) or default_facility_code
    return default_facility_code


def get_additional_required_headers(facility_config: Dict[str, Any]) -> set[str]:
    processor = normalize_cell_value(
        facility_config.get("facility_code_processor") or facility_config.get("special_processor")
    ).casefold()
    if processor:
        provider = REQUIRED_HEADER_PROVIDERS.get(processor)
        if provider:
            return provider(facility_config)
    return set()


def build_facility_value_conversions(
    facility_config: Dict[str, Any], base_value_conversions: Dict[str, Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    merged = deepcopy(base_value_conversions)
    processor = normalize_cell_value(facility_config.get("value_conversion_processor")).casefold()
    if not processor:
        return merged

    provider = VALUE_CONVERSION_PROVIDERS.get(processor)
    if provider is None:
        return merged

    for db_key, conversion_map in provider(facility_config).items():
        if not isinstance(conversion_map, dict):
            continue
        merged.setdefault(db_key, {}).update(conversion_map)
    return merged


def resolve_sankoh_facility_code(
    row: Dict[str, Any], facility_config: Dict[str, Any]
) -> Optional[int]:
    source_header = normalize_cell_value(
        facility_config.get("facility_code_source") or DEFAULT_FACILITY_NAME_HEADER
    )
    facility_name = normalize_cell_value(row.get(source_header))
    if not facility_name:
        return None
    return FACILITY_CODE_MAPS["sankoh"].get(facility_name)


def get_sankoh_required_headers(facility_config: Dict[str, Any]) -> set[str]:
    source_header = normalize_cell_value(
        facility_config.get("facility_code_source") or DEFAULT_FACILITY_NAME_HEADER
    )
    return {source_header}


def goshobo_room_number_conversions(_: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {"room_number": dict(GOSHOBO_ROOM_NUMBER_CONVERSIONS)}


FACILITY_CODE_RESOLVERS: Dict[str, FacilityCodeResolver] = {
    "sankoh": resolve_sankoh_facility_code,
    "sankoh_facility_code": resolve_sankoh_facility_code,
}

REQUIRED_HEADER_PROVIDERS: Dict[str, RequiredHeaderProvider] = {
    "sankoh": get_sankoh_required_headers,
    "sankoh_facility_code": get_sankoh_required_headers,
}

VALUE_CONVERSION_PROVIDERS: Dict[str, ValueConversionProvider] = {
    "goshobo_room_number": goshobo_room_number_conversions,
}
