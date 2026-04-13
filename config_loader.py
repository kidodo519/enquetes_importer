from __future__ import annotations

import os
from copy import deepcopy
from typing import Any, Dict

import yaml


def _read_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fp:
        loaded = yaml.safe_load(fp) or {}
    if not isinstance(loaded, dict):
        raise TypeError(f"Config file must be a dictionary: {path}")
    return loaded


def _resolve_path(base_path: str, candidate: str) -> str:
    return candidate if os.path.isabs(candidate) else os.path.join(base_path, candidate)


def load_config(config_path: str) -> Dict[str, Any]:
    """Load and merge DB config + mapping config.

    Supported formats:
    - split config (recommended):
      db_config: config_db.yaml
      mapping_config: config_mapping.yaml
    - legacy single-file config.
    """

    root_config = _read_yaml(config_path)
    base_path = os.path.dirname(config_path)

    db_config_ref = root_config.get("db_config")
    mapping_config_ref = root_config.get("mapping_config")

    if not db_config_ref and not mapping_config_ref:
        return root_config

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

    merged["corporations"] = merged_corporations
    return merged
