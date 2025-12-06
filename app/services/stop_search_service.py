import asyncio
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, cast

import httpx
import pandas as pd
from fastapi.concurrency import run_in_threadpool
from pandera import errors
from prometheus_client import Counter, Summary
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.config import AVAILABLE_FORCES, settings
from app.core.http_client import make_request, make_request_async
from app.models.failed_row import FailedRow
from app.models.stop_search import StopSearch
from app.schemas.stop_search import StopSearchBase, StopSearchDataFrameSchema
from app.services.csv_handler import CSVHandler
from app.services.data_cleaner import DataCleaner

logger = logging.getLogger(__name__)

STOP_SEARCH_COLUMNS = [
    "force",
    "type",
    "involved_person",
    "datetime",
    "operation",
    "operation_name",
    "latitude",
    "longitude",
    "street_id",
    "street_name",
    "gender",
    "age_range",
    "self_defined_ethnicity",
    "officer_defined_ethnicity",
    "legislation",
    "object_of_search",
    "outcome",
    "outcome_linked_to_object_of_search",
    "removal_of_more_than_outer_clothing",
    "outcome_object_id",
    "outcome_object_name",
]

FAILED_ROW_COLUMNS = ["raw_data", "reason", "source"]

BASE_POLICE_URL = "https://data.police.uk/api"
STOP_SEARCH_URL = f"{BASE_POLICE_URL}/stops-force"
AVAILABILITY_URL = f"{BASE_POLICE_URL}/crimes-street-dates"
POLICE_FORCES = settings.POLICE_FORCES

# Metrics
PROCESSING_TIME = Summary(
    "stop_search_processing_seconds", "Time spent processing stop-search records"
)
RECORDS_PROCESSED = Counter(
    "stop_search_records_processed_total",
    "Total number of stop-search records processed",
)
FAILED_ROWS = Counter(
    "stop_search_failed_rows_total", "Total number of failed rows recorded"
)


class PoliceStopSearchService:
    def __init__(self, db: Session):
        self.db = db

    async def download_stop_search_data(
        self, force: AVAILABLE_FORCES, output_dir: str = "/tmp"
    ) -> Optional[Tuple[str, str]]:
        """
        Fetches data for a force and dumps it to CSV files.
        Returns paths to (valid_csv, failed_csv).
        """
        dates_to_fetch = self._get_dates_to_process(force)

        if not dates_to_fetch:
            logger.info(f"No new dates to fetch for {force}")
            return None

        logger.info(f"Fetching dates for {force}: {dates_to_fetch}")

        all_valid_objects = []
        all_failed_rows = []

        async with httpx.AsyncClient() as client:
            tasks = [
                self._fetch_stop_search_data(force, date, client)
                for date in dates_to_fetch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        successful_requests = 0
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error fetching data for {force}: {result}")
                continue

            successful_requests += 1
            valid_objects, failed_rows = result
            all_valid_objects.extend(valid_objects)
            all_failed_rows.extend(failed_rows)

        # If all requests failed, raise exception to trigger Celery retry
        if successful_requests == 0 and dates_to_fetch:
            first_exception = next((r for r in results if isinstance(r, Exception)), None)
            if first_exception:
                raise first_exception

        logger.info(
            f"Batch processing for {force} complete. "
            f"Successful requests: {successful_requests}/{len(dates_to_fetch)}. "
            f"Valid rows: {len(all_valid_objects)}. Failed rows: {len(all_failed_rows)}."
        )

        # Write valid objects to CSV
        valid_csv_path = os.path.join(output_dir, f"valid_{force}.csv")
        CSVHandler.write_rows(valid_csv_path, all_valid_objects, STOP_SEARCH_COLUMNS)

        # Write failed rows to CSV
        failed_csv_path = os.path.join(output_dir, f"failed_{force}.csv")
        CSVHandler.write_rows(failed_csv_path, all_failed_rows, FAILED_ROW_COLUMNS)

        return valid_csv_path, failed_csv_path

    def remediate_failed_rows(self) -> None:
        """
        Attempts to fix and re-insert failed rows.
        """
        failed_rows = (
            self.db.query(FailedRow)
            .filter(FailedRow.source == StopSearch.__tablename__)
            .all()
        )

        if not failed_rows:
            logger.info("No failed rows to remediate.")
            return

        logger.info(f"Attempting to remediate {len(failed_rows)} failed rows...")

        remediated_count = 0

        for row in failed_rows:
            try:
                cleaned_data = DataCleaner.clean(row.raw_data)

                stop_search = StopSearch(**cleaned_data)

                self.db.add(stop_search)
                self.db.delete(row)
                self.db.commit()

                remediated_count += 1
            except Exception as e:
                self.db.rollback()
                logger.error(f"Failed to remediate row {row.id}: {e}")

        logger.info(
            f"Remediation completed. Successfully remediated "
            f"{remediated_count}/{len(failed_rows)} rows."
        )

    async def _fetch_stop_search_data(
        self, force: AVAILABLE_FORCES, date: str, client: httpx.AsyncClient
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Fetches and processes data for a single force.
        Returns a tuple of (valid_rows, failed_rows).
        """
        with PROCESSING_TIME.time():
            try:
                logger.info(f"Fetching data for force: {force}, date: {date}")
                params: Dict[str, Any] = {"force": force}

                if date:
                    params["date"] = date

                data = await make_request_async(STOP_SEARCH_URL, params, client=client)

                if data:
                    return await run_in_threadpool(
                        self._process_stop_search_data, force, data
                    )
                else:
                    logger.info(f"No data found for force: {force}")
                    return [], []

            except Exception as e:
                logger.error(f"Error processing force {force} for date {date}: {e}")
                raise

    def _get_latest_datetime(self, force: AVAILABLE_FORCES) -> Optional[datetime]:
        """
        Get the latest datetime of a record for the given force.
        """
        return cast(
            Optional[datetime],
            self.db.query(func.max(StopSearch.datetime))
            .filter(StopSearch.force == force)
            .scalar(),
        )

    def _get_available_dates(self) -> Dict[str, List[str]]:
        """
        Fetches available dates from the API.
        Returns a dictionary mapping force IDs to a list of available dates.
        """
        try:
            data = make_request(AVAILABILITY_URL)
            availability: Dict[str, List[str]] = {}

            for entry in data:
                date = entry.get("date")
                forces = entry.get("stop-and-search", [])

                if not date:
                    continue

                for force_id in forces:
                    if force_id not in availability:
                        availability[force_id] = []

                    availability[force_id].append(date)

            for force_id in availability:
                availability[force_id].sort()

            return availability
        except Exception as e:
            logger.error(f"Failed to fetch available dates: {e}")
            return {}

    def _get_dates_to_process(
        self,
        force: AVAILABLE_FORCES,
    ) -> List[str]:
        """
        Determines which dates need to be processed for a given force.
        Checks available dates from API and compares with latest date in DB.
        """
        # Get available dates for this force
        availability = self._get_available_dates()
        available_dates = availability.get(force, [])

        if not available_dates:
            logger.warning(f"No available dates found for force: {force}")
            return []

        # Get latest date from DB
        latest_datetime = self._get_latest_datetime(force)
        latest_date_str = latest_datetime.strftime("%Y-%m") if latest_datetime else None

        logger.info(f"Latest date in DB for {force}: {latest_date_str}")

        # Filter dates to fetch
        dates_to_fetch = []
        for date in available_dates:
            # If we have data in DB, skip dates before or equal to latest
            if latest_date_str and date <= latest_date_str:
                continue

            dates_to_fetch.append(date)

        return dates_to_fetch

    def _process_stop_search_data(
        self, force: AVAILABLE_FORCES, data: List[Dict[str, Any]]
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Processes and validates raw stop search data, separating valid and failed rows
        """
        logger.info(f"Processing {len(data)} records for force: {force}")

        if not data:
            return [], []

        # Normalize JSON to flatten nested structures
        df = pd.json_normalize(data)
        df = df.replace(r"^\s*$", None, regex=True)

        # Rename columns to match schema
        column_mapping = {
            "location.latitude": "latitude",
            "location.longitude": "longitude",
            "location.street.id": "street_id",
            "location.street.name": "street_name",
            "outcome_object.id": "outcome_object_id",
            "outcome_object.name": "outcome_object_name",
        }

        df = df.rename(columns=column_mapping)

        df["force"] = force

        # Ensure all columns exist
        for col in STOP_SEARCH_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # Select only relevant columns
        df = df[STOP_SEARCH_COLUMNS]

        valid_rows: List[Dict[str, Any]] = []
        failed_rows: List[Dict[str, Any]] = []

        try:
            # Validate using Pandera
            df = StopSearchDataFrameSchema.validate(df, lazy=True)

            # Convert to object and replace pd.NA with None for JSON/CSV compatibility
            df = df.astype(object).where(pd.notnull(df), cast(Any, None))

            valid_rows = cast(List[Dict[str, Any]], df.to_dict(orient="records"))
            RECORDS_PROCESSED.inc(len(valid_rows))

        except errors.SchemaErrors as err:
            validated_df = err.data

            # Split valid and invalid
            failed_indices = [
                i for i in err.failure_cases["index"].unique() if pd.notna(i)
            ]

            valid_df = validated_df.drop(failed_indices)

            # Convert to object and replace pd.NA with None for JSON/CSV compatibility
            valid_df = valid_df.astype(object).where(
                pd.notnull(valid_df), cast(Any, None)
            )

            valid_rows = cast(List[Dict[str, Any]], valid_df.to_dict(orient="records"))
            RECORDS_PROCESSED.inc(len(valid_rows))

            # Handle failed rows
            remediated_valid = []

            for idx in failed_indices:
                # Cast idx to int because it might be float/object from pandas
                idx_int = int(idx)

                if idx_int < len(data):
                    item = data[idx_int]

                    try:
                        # Remediation attempt
                        cleaned_item = DataCleaner.clean(item)
                        valid_row = self._create_stop_search_dict(cleaned_item, force)
                        remediated_valid.append(valid_row)
                        RECORDS_PROCESSED.inc()
                    except Exception as e2:
                        failed_rows.append({"raw_data": item, "reason": str(e2)})
                        FAILED_ROWS.inc()
                else:
                    logger.error(
                        f"Index {idx_int} out of bounds for data list of size {len(data)}"
                    )

            valid_rows.extend(remediated_valid)

        logger.info(
            f"Processed force {force}: {len(valid_rows)} new valid rows, "
            f"{len(failed_rows)} failed rows"
        )

        return valid_rows, failed_rows

    def _create_stop_search_dict(
        self, item: Dict[str, Any], force: AVAILABLE_FORCES
    ) -> Dict[str, Any]:
        """
        Validate and create a flattened dictionary from a raw item.
        """
        schema_item = StopSearchBase(**item)

        latitude = None
        longitude = None
        street_id = None
        street_name = None

        if schema_item.location:
            latitude = schema_item.location.latitude
            longitude = schema_item.location.longitude

            if schema_item.location.street:
                street_id = schema_item.location.street.id
                street_name = schema_item.location.street.name

        outcome_object_id = None
        outcome_object_name = None

        if schema_item.outcome_object:
            outcome_object_id = schema_item.outcome_object.id
            outcome_object_name = schema_item.outcome_object.name

        return {
            "force": force,
            "type": schema_item.type,
            "involved_person": schema_item.involved_person,
            "datetime": schema_item.datetime,
            "operation": schema_item.operation,
            "operation_name": schema_item.operation_name,
            "latitude": latitude,
            "longitude": longitude,
            "street_id": street_id,
            "street_name": street_name,
            "gender": schema_item.gender,
            "age_range": schema_item.age_range,
            "self_defined_ethnicity": schema_item.self_defined_ethnicity,
            "officer_defined_ethnicity": schema_item.officer_defined_ethnicity,
            "legislation": schema_item.legislation,
            "object_of_search": schema_item.object_of_search,
            "outcome": schema_item.outcome,
            "outcome_linked_to_object_of_search": schema_item.outcome_linked_to_object_of_search,
            "removal_of_more_than_outer_clothing": schema_item.removal_of_more_than_outer_clothing,
            "outcome_object_id": outcome_object_id,
            "outcome_object_name": outcome_object_name,
        }
