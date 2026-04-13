from __future__ import annotations

import logging
from typing import Any, Optional

import psycopg2
from psycopg2 import extras

logger = logging.getLogger(__name__)


def normalize_optional_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    normalized = str(value).strip()
    return normalized or None


def execute_before_insert_sql(
    cursor: psycopg2.extensions.cursor,
    before_insert_sql: Optional[str],
    corporation: str,
    facility_name: str,
) -> None:
    sql = normalize_optional_string(before_insert_sql)
    if not sql:
        return

    cursor.execute(sql)
    logger.info("Executed before_insert_sql for %s/%s.", corporation, facility_name)


def delete_existing_rows(
    cursor: psycopg2.extensions.cursor,
    facility_table: str,
    facility_code: int,
    should_delete: bool,
    corporation: str,
    facility_name: str,
) -> None:
    if should_delete:
        cursor.execute(f"DELETE FROM {facility_table} WHERE facility_code = %s", (facility_code,))
        return

    logger.info(
        "Skipping deletion for %s/%s because delete is disabled in the configuration.",
        corporation,
        facility_name,
    )


def insert_rows(
    connection: psycopg2.extensions.connection,
    cursor: psycopg2.extensions.cursor,
    facility_table: str,
    ordered_keys: list[str],
    buffer: list[list[Any]],
    corporation: str,
    facility_name: str,
) -> None:
    if not buffer:
        connection.commit()
        logger.info("No rows to import for %s/%s.", corporation, facility_name)
        return

    insert_query = f"INSERT INTO {facility_table} ({', '.join(ordered_keys)}) VALUES %s"
    extras.execute_values(cursor, insert_query, buffer)
    connection.commit()
    logger.info("Imported %d rows for %s/%s.", len(buffer), corporation, facility_name)
