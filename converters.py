from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from dateutil import parser as date_parser
from dateutil import tz

DEFAULT_TIMEZONE = tz.gettz("Asia/Tokyo") or tz.tzlocal()


def normalize_cell_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def normalize_header_name(value: Any) -> str:
    return normalize_cell_value(value)


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
