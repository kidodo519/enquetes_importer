from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable, Dict, Optional

from converters import normalize_cell_value

FacilityCodeResolver = Callable[[Dict[str, Any], Dict[str, Any]], Optional[int]]
RequiredHeaderProvider = Callable[[Dict[str, Any]], set[str]]
ValueConversionProvider = Callable[[Dict[str, Any]], Dict[str, Dict[str, Any]]]
FacilitySettingsProvider = Callable[[Dict[str, Any]], Dict[str, Any]]
MappingSettingsProvider = Callable[[Dict[str, Any]], Dict[str, Any]]

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
    processor = _resolve_facility_code_processor(facility_config)
    if processor:
        resolver = FACILITY_CODE_RESOLVERS.get(processor)
        if resolver:
            return resolver(row, facility_config) or default_facility_code
    return default_facility_code


def get_additional_required_headers(facility_config: Dict[str, Any]) -> set[str]:
    processor = _resolve_required_header_processor(facility_config)
    if processor:
        provider = REQUIRED_HEADER_PROVIDERS.get(processor)
        if provider:
            return provider(facility_config)
    return set()


def build_facility_value_conversions(
    facility_config: Dict[str, Any], base_value_conversions: Dict[str, Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    merged = deepcopy(base_value_conversions)
    processor = _resolve_value_conversion_processor(facility_config)
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


def hachinobo_enquetes_settings(_: Dict[str, Any]) -> Dict[str, Any]:
    return {"worksheet": "アンケート結果", "table": "enquetes"}


def hachinobo_text_settings(_: Dict[str, Any]) -> Dict[str, Any]:
    return {"worksheet": "対応内容", "table": "enquetes_text"}


def goshobo_language_mapping_settings(facility_config: Dict[str, Any]) -> Dict[str, Any]:
    processors = _get_facility_processors_config(facility_config)
    mapping_config = processors.get("mapping") or {}
    if not isinstance(mapping_config, dict):
        return {}

    language_column = normalize_cell_value(mapping_config.get("language_column"))
    language_mappings = mapping_config.get("language_mappings")
    if not isinstance(language_mappings, dict):
        return {}

    resolved: Dict[str, Any] = {"language_mappings": deepcopy(language_mappings)}
    if language_column:
        resolved["language_column"] = language_column
    return resolved


def _get_facility_processors_config(facility_config: Dict[str, Any]) -> Dict[str, Any]:
    processors = facility_config.get("facility_processors") or {}
    return processors if isinstance(processors, dict) else {}


def _normalize_processor_name(value: Any) -> str:
    return normalize_cell_value(value).casefold()


def _resolve_facility_code_processor(facility_config: Dict[str, Any]) -> str:
    processors = _get_facility_processors_config(facility_config)
    facility_code_config = processors.get("facility_code") or {}
    if isinstance(facility_code_config, dict):
        processor = _normalize_processor_name(facility_code_config.get("resolver"))
        if processor:
            return processor
    return _normalize_processor_name(
        facility_config.get("facility_code_processor") or facility_config.get("special_processor")
    )


def _resolve_required_header_processor(facility_config: Dict[str, Any]) -> str:
    processors = _get_facility_processors_config(facility_config)
    facility_code_config = processors.get("facility_code") or {}
    if isinstance(facility_code_config, dict):
        processor = _normalize_processor_name(facility_code_config.get("required_headers"))
        if processor:
            return processor
        resolver = _normalize_processor_name(facility_code_config.get("resolver"))
        if resolver:
            return resolver
    return _resolve_facility_code_processor(facility_config)


def _resolve_value_conversion_processor(facility_config: Dict[str, Any]) -> str:
    processors = _get_facility_processors_config(facility_config)
    value_conversion_config = processors.get("value_conversions") or {}
    if isinstance(value_conversion_config, dict):
        processor = _normalize_processor_name(value_conversion_config.get("provider"))
        if processor:
            return processor
    return _normalize_processor_name(facility_config.get("value_conversion_processor"))


def _resolve_facility_settings_processor(facility_config: Dict[str, Any]) -> str:
    processors = _get_facility_processors_config(facility_config)
    settings_config = processors.get("facility_settings") or {}
    if isinstance(settings_config, dict):
        processor = _normalize_processor_name(settings_config.get("provider"))
        if processor:
            return processor
    return _normalize_processor_name(facility_config.get("facility_settings_processor"))


def _apply_facility_settings(facility_config: Dict[str, Any]) -> Dict[str, Any]:
    processor = _resolve_facility_settings_processor(facility_config)
    if not processor:
        return facility_config

    provider = FACILITY_SETTINGS_PROVIDERS.get(processor)
    if provider is None:
        return facility_config

    merged = deepcopy(facility_config)
    for key, value in provider(facility_config).items():
        merged.setdefault(key, value)
    return merged


def _resolve_mapping_settings_processor(facility_config: Dict[str, Any]) -> str:
    processors = _get_facility_processors_config(facility_config)
    mapping_config = processors.get("mapping") or {}
    if isinstance(mapping_config, dict):
        processor = _normalize_processor_name(mapping_config.get("provider"))
        if processor:
            return processor
    return _normalize_processor_name(facility_config.get("mapping_processor"))


def _apply_mapping_settings(facility_config: Dict[str, Any]) -> Dict[str, Any]:
    processor = _resolve_mapping_settings_processor(facility_config)
    if not processor:
        return facility_config

    provider = MAPPING_SETTINGS_PROVIDERS.get(processor)
    if provider is None:
        return facility_config

    merged = deepcopy(facility_config)
    for key, value in provider(facility_config).items():
        merged.setdefault(key, value)
    return merged


FACILITY_CODE_RESOLVERS: Dict[str, FacilityCodeResolver] = {
    "sankoh": resolve_sankoh_facility_code,
    "sankoh_facility_code": resolve_sankoh_facility_code,
    "resolve_sankoh_facility_code": resolve_sankoh_facility_code,
}

REQUIRED_HEADER_PROVIDERS: Dict[str, RequiredHeaderProvider] = {
    "sankoh": get_sankoh_required_headers,
    "sankoh_facility_code": get_sankoh_required_headers,
    "get_sankoh_required_headers": get_sankoh_required_headers,
}

VALUE_CONVERSION_PROVIDERS: Dict[str, ValueConversionProvider] = {
    "goshobo_room_number": goshobo_room_number_conversions,
    "goshobo_room_number_conversions": goshobo_room_number_conversions,
}

FACILITY_SETTINGS_PROVIDERS: Dict[str, FacilitySettingsProvider] = {
    "hachinobo_enquetes": hachinobo_enquetes_settings,
    "hachinobo_enquetes_settings": hachinobo_enquetes_settings,
    "hachinobo_text": hachinobo_text_settings,
    "hachinobo_text_settings": hachinobo_text_settings,
}

MAPPING_SETTINGS_PROVIDERS: Dict[str, MappingSettingsProvider] = {
    "goshobo_language_mapping": goshobo_language_mapping_settings,
    "goshobo_language_mapping_settings": goshobo_language_mapping_settings,
}


FACILITY_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "sankoh.sankoh": {
        "facility_processors": {
            "facility_code": {
                "resolver": "resolve_sankoh_facility_code",
                "required_headers": "get_sankoh_required_headers",
            }
        },
        "facility_code_source": "宿泊施設",
        "fixed_values": {"enquete_system_name": "spreadsheet"},
        "mapping": "sankoh",
    },
    "a_and_c.kifunosato": {
        "mapping": "a_and_c_japanese",
    },
    "a_and_c.roka_japanese": {
        "mapping": "a_and_c_japanese",
    },
    "a_and_c.roka_english": {
        "mapping": "a_and_c_english",
    },
    "hachinobo.hachinobo": {
        "facility_processors": {
            "facility_settings": {"provider": "hachinobo_enquetes_settings"}
        },
    },
    "hachinobo.hachinobo_text": {
        "facility_processors": {"facility_settings": {"provider": "hachinobo_text_settings"}},
    },
    "goshobo.goshobo": {
        "facility_processors": {
            "value_conversions": {"provider": "goshobo_room_number_conversions"},
            "mapping": {
                "provider": "goshobo_language_mapping_settings",
                "language_column": "language",
                "language_mappings": {
                    "日本語": "goshobo_japanese",
                    "english": "goshobo_english",
                },
            },
        },
    },
}


def apply_facility_overrides(
    corporation: str, facility_name: str, facility_config: Dict[str, Any]
) -> Dict[str, Any]:
    merged = deepcopy(facility_config)
    key = f"{corporation}.{facility_name}"
    overrides = FACILITY_OVERRIDES.get(key)
    if overrides:
        for override_key, override_value in overrides.items():
            if isinstance(override_value, dict) and isinstance(merged.get(override_key), dict):
                nested = deepcopy(merged[override_key])
                for nested_key, nested_value in override_value.items():
                    nested.setdefault(nested_key, nested_value)
                merged[override_key] = nested
            else:
                merged.setdefault(override_key, override_value)
    merged = _apply_facility_settings(merged)
    return _apply_mapping_settings(merged)
