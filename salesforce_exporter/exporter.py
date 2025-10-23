"""Main Salesforce exporting logic."""

from __future__ import annotations

import csv
import logging
from datetime import datetime
from itertools import product
from typing import Dict, Iterable, List, Optional

import pandas as pd
import numpy as np
from pandas.api.types import DatetimeTZDtype
from simple_salesforce import Salesforce, SalesforceLogin

from .config import AppConfig, CombinedOutputConfig, QueryConfig, QueryJoinConfig
from .s3_uploader import upload_to_s3

LOGGER = logging.getLogger(__name__)


class SalesforceExporter:
    """Export data from Salesforce and upload it to S3."""

    
    def __init__(self, config: AppConfig) -> None:
        self.config = config

        login_kwargs = {
            "username": config.salesforce.username,
            "password": config.salesforce.password,
            "domain": config.salesforce.domain,
        }
        if config.salesforce.security_token:
            login_kwargs["security_token"] = config.salesforce.security_token

        session_id, instance = SalesforceLogin(**login_kwargs)
        self.sf = Salesforce(instance=instance, session_id=session_id)


    def run(self) -> None:
        LOGGER.info("Starting Salesforce export for %d query(ies)", len(self.config.queries))
        self.config.csv.output_directory.mkdir(parents=True, exist_ok=True)
        if self.config.csv.archive_directory:
            self.config.csv.archive_directory.mkdir(parents=True, exist_ok=True)

        results_cache: Dict[str, pd.DataFrame] = {}
        for query_config in self.config.queries:
            df = self._export_query(query_config, results_cache)
            results_cache[query_config.name] = df

        for combined_config in self.config.combined_outputs:
            df = self._build_combined_output(combined_config, results_cache)
            results_cache[combined_config.name] = df

    def _export_query(
        self,
        query_config: QueryConfig,
        results_cache: Dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        batches = self._build_relationship_batches(query_config, results_cache)

        dataframes: List[pd.DataFrame] = []
        if batches is None:
            batches_to_process = [()]
            chunked = False
        elif not batches:
            LOGGER.info(
                "Skipping query %s because no related records were found",
                query_config.name,
            )
            return pd.DataFrame()
        else:
            batches_to_process = product(*batches)
            chunked = True

        for index, additional_conditions in enumerate(batches_to_process, start=1):
            batch_index = index if chunked else None
            df = self._run_single_query(
                query_config, additional_conditions, batch_index=batch_index
            )
            if not df.empty:
                dataframes.append(df)

        if not dataframes:
            LOGGER.warning("Query %s returned no data", query_config.name)
            return pd.DataFrame()

        combined = pd.concat(dataframes, ignore_index=True)

        if query_config.write_output:
            self._write_output(query_config.name, query_config.output_file, combined)
        else:
            LOGGER.info("Skipping write for %s because write_output is false", query_config.name)
        return combined

    def _build_combined_output(
        self,
        combined_config: CombinedOutputConfig,
        results_cache: Dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        base_df = results_cache.get(combined_config.base_query)
        if base_df is None:
            raise ValueError(
                f"Combined output '{combined_config.name}' depends on "
                f"'{combined_config.base_query}' which has not been executed"
            )

        df = base_df.copy()
        if combined_config.joins:
            df = self._apply_joins(
                df,
                combined_config.joins,
                results_cache,
                combined_config.name,
            )

        if not df.empty:
            df = self._apply_custom_transformations(
                combined_config.name, df, results_cache
            )
            LOGGER.info(
                "Built combined output %s with %d rows",
                combined_config.name,
                len(df.index),
            )
        else:
            LOGGER.warning(
                "Combined output %s produced no rows", combined_config.name
            )

        self._write_output(
            combined_config.name, combined_config.output_file, df
        )
        return df

      
    def _build_relationship_batches(
        self,
        query_config: QueryConfig,
        results_cache: Dict[str, pd.DataFrame],
    ) -> Optional[List[List[str]]]:
        if not query_config.relationship_filters:
            return None

        batches: List[List[str]] = []
        for filter_config in query_config.relationship_filters:
            source_df = results_cache.get(filter_config.source_query)
            if source_df is None:
                raise ValueError(
                    f"Query '{query_config.name}' depends on '{filter_config.source_query}' "
                    "which has not been executed yet"
                )

            if source_df.empty:
                return []

            if filter_config.source_field not in source_df.columns:
                raise ValueError(
                    f"Field '{filter_config.source_field}' not found in results of "
                    f"query '{filter_config.source_query}'"
                )

            values_series = source_df[filter_config.source_field].dropna()
            if values_series.empty:
                return []

            values = [str(value) for value in values_series.tolist() if str(value)]
            if not values:
                return []

            unique_values = list(dict.fromkeys(values))
            chunked_conditions: List[str] = []
            for start in range(0, len(unique_values), filter_config.chunk_size):
                chunk = unique_values[start : start + filter_config.chunk_size]
                condition = self._build_in_condition(filter_config.target_field, chunk)
                chunked_conditions.append(condition)
            batches.append(chunked_conditions)

        return batches

    @staticmethod
    def _build_in_condition(field: str, values: Iterable[str]) -> str:
        formatted = ", ".join(SalesforceExporter._quote(value) for value in values)
        return f"{field} IN ({formatted})"

    @staticmethod
    def _quote(value: str) -> str:
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"

    def _run_single_query(
        self,
        query_config: QueryConfig,
        additional_conditions: Iterable[str],
        batch_index: Optional[int] = None,
    ) -> pd.DataFrame:
        query = query_config.build_query(additional_conditions)
        if batch_index is None:
            LOGGER.info("Running query for %s", query_config.name)
        else:
            LOGGER.info(
                "Running query for %s (batch %d)", query_config.name, batch_index
            )
        LOGGER.debug("SOQL: %s", query)

        results = self.sf.query_all(query)
        records = results.get("records", [])
        for record in records:
            record.pop("attributes", None)

        if not records:
            return pd.DataFrame()

        return pd.DataFrame(records)

    def _apply_joins(
        self,
        df: pd.DataFrame,
        joins: List[QueryJoinConfig],
        results_cache: Dict[str, pd.DataFrame],
        owner_name: str,
    ) -> pd.DataFrame:
        result = df
        for join in joins:
            other = results_cache.get(join.source_query)
            if other is None:
                raise ValueError(
                    f"{owner_name} requires '{join.source_query}' before joining"
                )

            other_df = other.copy()
            left_on: Iterable[str] | str
            right_on: Iterable[str] | str
            if len(join.left_on) == 1:
                left_on = join.left_on[0]
            else:
                left_on = list(join.left_on)
            if len(join.right_on) == 1:
                right_on = join.right_on[0]
            else:
                right_on = list(join.right_on)

            suffixes = join.suffixes or ("", f"_{join.source_query}")
            result = result.merge(
                other_df,
                how=join.how,
                left_on=left_on,
                right_on=right_on,
                suffixes=suffixes,
            )
        return result


    def _apply_custom_transformations(
        self,
        name: str,
        df: pd.DataFrame,
        results_cache: Dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """Apply dataset-specific transformations before writing output."""

        if name in {"Sales_history_combined", "Sales_onhand_combined"}:
            df = self._add_reservation_details(df, results_cache)
        if name in {"Reservations_history_combined", "Reservations_onhand_combined"}:
            df = self._add_number_of_use(df, results_cache)
        return df

    def _add_reservation_details(
        self, df: pd.DataFrame, results_cache: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """Populate reservation identifiers on sales outputs."""

        if df.empty or "ps__Relreserve__c" not in df.columns:
            return df

        required_columns = {"Id", "ps__No__c", "ps__EntryTime__c"}
        reservation_frames: List[pd.DataFrame] = []
        for key in ("Reservations_history", "Reservations_onhand"):
            frame = results_cache.get(key)
            if frame is None or frame.empty:
                continue
            if not required_columns.issubset(frame.columns):
                LOGGER.warning(
                    "Reservation dataset %s is missing required columns for sales join",
                    key,
                )
                continue
            reservation_frames.append(frame[list(required_columns)])

        if not reservation_frames:
            return df

        reservations_df = pd.concat(reservation_frames, ignore_index=True)
        reservations_df = reservations_df.drop_duplicates(subset=["Id"])
        reservations_lookup = reservations_df.set_index("Id")

        reservation_ids = df["ps__Relreserve__c"]
        for column in ("ps__No__c", "ps__EntryTime__c"):
            if column not in reservations_lookup.columns:
                continue
            df[column] = reservation_ids.map(reservations_lookup[column])

        return df

    def _add_number_of_use(
        self, df: pd.DataFrame, results_cache: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """Add number_of_use column counting prior confirmed stays per contact."""

        if df.empty:
            return df

        contact_column = "ps__Relcontact__c"
        entry_column = "ps__EntryTime__c"
        status_column = "ps__ReservedStatus__c"

        if contact_column not in df.columns or entry_column not in df.columns:
            return df

        required_columns = {"Id", contact_column, entry_column, status_column}
        reservation_frames: List[pd.DataFrame] = []
        for key in ("Reservations_history", "Reservations_onhand"):
            frame = results_cache.get(key)
            if frame is None or frame.empty:
                continue
            if not required_columns.issubset(frame.columns):
                LOGGER.warning(
                    "Reservation dataset %s is missing required columns for number_of_use",
                    key,
                )
                continue
            reservation_frames.append(frame[list(required_columns)])

        if not reservation_frames:
            df["number_of_use"] = 0
            return df

        reservations = pd.concat(reservation_frames, ignore_index=True)
        reservations = reservations.dropna(subset=[contact_column])
        reservations = reservations[reservations[status_column] == "確定"].copy()
        if reservations.empty:
            df["number_of_use"] = 0
            return df

        reservations[entry_column] = self._normalize_datetime_series(
            reservations[entry_column]
        )
        reservations = reservations.dropna(subset=[entry_column])
        if reservations.empty:
            df["number_of_use"] = 0
            return df

        now = datetime.now(self.config.timezone).replace(tzinfo=None)
        now_timestamp = pd.Timestamp(now)
        relevant = reservations[reservations[entry_column] <= now]
        if relevant.empty:
            df["number_of_use"] = 0
            return df

        relevant = relevant.sort_values([contact_column, entry_column, "Id"])
        times_by_contact = {
            contact: pd.Index(group[entry_column].tolist())
            for contact, group in relevant.groupby(contact_column)
        }

        entry_times = self._normalize_datetime_series(df[entry_column])
        counts: List[int] = []
        for contact, entry_time in zip(df[contact_column], entry_times):
            if pd.isna(contact) or pd.isna(entry_time):
                counts.append(0)
                continue

            times = times_by_contact.get(contact)
            if times is None or times.empty:
                counts.append(0)
                continue

            if not isinstance(entry_time, pd.Timestamp):
                entry_time = pd.to_datetime(entry_time, errors="coerce")
            if pd.isna(entry_time):
                counts.append(0)
                continue

            if entry_time <= now_timestamp:
                index = int(times.searchsorted(entry_time, side="left"))
            else:
                index = int(times.searchsorted(now_timestamp, side="right"))
            counts.append(index)

        df["number_of_use"] = counts
        return df

    def _normalize_datetime_series(self, series: pd.Series) -> pd.Series:
        """Convert a datetime-like series to naive timestamps in the app timezone."""

        parsed = pd.to_datetime(series, errors="coerce")
        if isinstance(parsed.dtype, DatetimeTZDtype):
            parsed = parsed.dt.tz_convert(self.config.timezone).dt.tz_localize(None)
        return parsed


    def _write_output(
        self, name: str, output_file: Optional[str], df: pd.DataFrame
    ) -> None:
        timestamp = datetime.now(self.config.timezone).strftime("%Y%m%d%H%M%S")
        output_name = output_file or name
        local_filename = f"{output_name}_{timestamp}.csv"
        local_path = self.config.csv.output_directory / local_filename
        LOGGER.info("Writing %d rows to %s", len(df.index), local_path)
        df.to_csv(
            local_path,
            index=False,
            quoting=csv.QUOTE_NONNUMERIC,
            encoding=self.config.csv.encoding,
        )

        remote_filename = f"{self.config.s3.file_name_prefix}{local_filename}"
        uploaded = upload_to_s3(local_path, self.config.s3, remote_filename)

        if uploaded and self.config.csv.archive_directory:
            destination = self.config.csv.archive_directory / local_filename
            LOGGER.info("Moving %s to %s", local_path, destination)
            local_path.replace(destination)


__all__ = ["SalesforceExporter"]
