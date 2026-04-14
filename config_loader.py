from __future__ import annotations

import os
from copy import deepcopy
from glob import glob
from typing import Any, Dict, Iterable, List

import yaml

MAPPING_SECTIONS = {"string", "text", "integer", "date", "datetime"}
DEFAULT_FACILITY_MAPPINGS_DIR = "facility_mappings"


def _read_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fp:
        loaded = yaml.safe_load(fp) or {}
    if not isinstance(loaded, dict):
        raise TypeError(f"Config file must be a dictionary: {path}")
    return loaded


def _resolve_path(base_path: str, candidate: str) -> str:
    return candidate if os.path.isabs(candidate) else os.path.join(base_path, candidate)


def _collect_yaml_paths(path: str) -> List[str]:
    if os.path.isdir(path):
        patterns = ("*.yaml", "*.yml")
        collected: List[str] = []
        for pattern in patterns:
            collected.extend(glob(os.path.join(path, "**", pattern), recursive=True))
        return sorted({yaml_path for yaml_path in collected if os.path.isfile(yaml_path)})
    return [path]


def _extract_mappings(loaded: Dict[str, Any], source_path: str) -> Dict[str, Any]:
    if "mappings" in loaded:
        mappings = loaded.get("mappings") or {}
        if isinstance(mappings, dict) and MAPPING_SECTIONS.issubset(set(mappings.keys())):
            mapping_key = os.path.splitext(os.path.basename(source_path))[0]
            mappings = {mapping_key: mappings}
    elif MAPPING_SECTIONS.issubset(set(loaded.keys())):
        mapping_key = os.path.splitext(os.path.basename(source_path))[0]
        mappings = {mapping_key: loaded}
    else:
        mappings = loaded
    if not isinstance(mappings, dict):
        raise TypeError(f"Mappings in '{source_path}' must be a dictionary.")
    return mappings


def _resolve_mapping_ref(base_path: str, ref: str) -> str:
    looks_like_path = os.path.isabs(ref) or os.path.sep in ref or ref.endswith((".yaml", ".yml"))
    if looks_like_path:
        return _resolve_path(base_path, ref)

    candidates = [
        os.path.join(base_path, DEFAULT_FACILITY_MAPPINGS_DIR, f"{ref}.yaml"),
        os.path.join(base_path, DEFAULT_FACILITY_MAPPINGS_DIR, f"{ref}.yml"),
        os.path.join(base_path, DEFAULT_FACILITY_MAPPINGS_DIR, ref),
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return candidates[0]


def _load_mappings_from_refs(base_path: str, refs: Iterable[str]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for ref in refs:
        resolved = _resolve_mapping_ref(base_path, ref)
        for mapping_path in _collect_yaml_paths(resolved):
            loaded = _read_yaml(mapping_path)
            merged.update(deepcopy(_extract_mappings(loaded, mapping_path)))
    return merged


def _normalize_mapping_refs(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    if isinstance(value, list):
        refs: List[str] = []
        for item in value:
            if not isinstance(item, str):
                raise TypeError("'mapping_files' must contain only strings.")
            stripped = item.strip()
            if stripped:
                refs.append(stripped)
        return refs
    raise TypeError("'mapping_file'/'mapping_files' must be a string or list of strings.")


def load_config(config_path: str) -> Dict[str, Any]:
    """Load and merge DB config + mapping config.

    Supported formats:
    - single-file config (recommended): config_db.yaml
      + optional per-facility mapping_file(s) / mapping_files
    - split config: db_config + mapping_config
    """

    root_config = _read_yaml(config_path)
    base_path = os.path.dirname(config_path)

    db_config_ref = root_config.get("db_config")
    mapping_config_ref = root_config.get("mapping_config")

    if not db_config_ref and not mapping_config_ref:
        merged = deepcopy(root_config)
        merged.setdefault("mappings", {})
        return _merge_facility_mappings(merged, base_path)

    if not db_config_ref or not mapping_config_ref:
        raise ValueError("Both 'db_config' and 'mapping_config' must be set for split config.")

    db_config = _read_yaml(_resolve_path(base_path, db_config_ref))
    mapping_config = _read_yaml(_resolve_path(base_path, mapping_config_ref))

    merged = deepcopy(db_config)
    merged["mappings"] = deepcopy(mapping_config.get("mappings", {}))

    merged_corporations = deepcopy(merged.get("corporations", {}))
    mapping_corporations = mapping_config.get("corporations", {}) or {}

    for corporation, corporation_config in merged_corporations.items():
        mapping_corp_config = mapping_corporations.get(corporation, {}) or {}
        corp_mappings = mapping_corp_config.get("mappings", {}) or {}
        if corp_mappings:
            corporation_config["mappings"] = deepcopy(corp_mappings)

        facilities = corporation_config.get("facilities", {}) or {}
        for facility_name, facility_config in facilities.items():
            refs = _normalize_mapping_refs(
                facility_config.get("mapping_files") or facility_config.get("mapping_file")
            )
            if not refs:
                continue
            facility_mappings = _load_mappings_from_refs(base_path, refs)
            if not facility_mappings:
                continue
            existing = deepcopy(facility_config.get("mappings", {}) or {})
            existing.update(facility_mappings)
            facility_config["mappings"] = existing

    merged["corporations"] = merged_corporations
    return _merge_facility_mappings(merged, base_path)


def _merge_facility_mappings(config: Dict[str, Any], base_path: str) -> Dict[str, Any]:
    merged = deepcopy(config)
    merged.setdefault("mappings", {})
    corporations = merged.get("corporations", {}) or {}

    for _, corporation_config in corporations.items():
        facilities = corporation_config.get("facilities", {}) or {}
        for _, facility_config in facilities.items():
            refs = _normalize_mapping_refs(
                facility_config.get("mapping_files") or facility_config.get("mapping_file")
            )
            if not refs:
                continue

            facility_mappings = _load_mappings_from_refs(base_path, refs)
            if not facility_mappings:
                continue

            existing = deepcopy(facility_config.get("mappings", {}) or {})
            existing.update(facility_mappings)
            facility_config["mappings"] = existing

            if "mapping" in facility_config:
                continue
            if "default" in existing:
                facility_config["mapping"] = "default"
            elif len(existing) == 1:
                facility_config["mapping"] = next(iter(existing.keys()))

    return merged
