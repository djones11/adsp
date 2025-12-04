import json
import logging
import os
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, cast

import requests  # type: ignore
from prometheus_client import Counter, Summary
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.failed_row import FailedRow
from app.models.stop_search import StopSearch
from app.schemas.stop_search import StopSearchCreate

logger = logging.getLogger(__name__)

BASE_URL = "https://data.police.uk/api/stops-force"
AVAILABILITY_URL = "https://data.police.uk/api/crimes-street-dates"
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


class PoliceAPIService:
    def __init__(self, db: Session):
        self.db = db

    @PROCESSING_TIME.time()
    def fetch_and_process_force(
        self, force: str, date: Optional[str] = None
    ) -> tuple[List[StopSearch], List[Dict[str, Any]]]:
        """
        Fetches and processes data for a single force.
        Returns a tuple of (valid_objects, failed_rows).
        """
        try:
            logger.info(f"Fetching data for force: {force}, date: {date}")
            data = self._fetch_data(force, date)

            if data:
                return self._process_data_in_memory(force, data)
            else:
                logger.info(f"No data found for force: {force}")
                return [], []

        except Exception as e:
            logger.error(f"Error processing force {force}: {e}")
            return [], []

    def _process_data_in_memory(
        self, force: str, data: List[Dict[str, Any]]
    ) -> tuple[List[StopSearch], List[Dict[str, Any]]]:
        latest_datetime = self._get_latest_datetime(force)
        logger.info(f"Latest datetime for {force}: {latest_datetime}")

        valid_objects = []
        failed_rows = []

        logger.info(f"Processing {len(data)} records for force: {force}")

        for item in data:
            try:
                # First attempt
                db_obj = self._create_stop_search_object(item, force)

                # Skip if record is older or equal to latest in DB
                obj_datetime = cast(datetime, db_obj.datetime)

                if latest_datetime and obj_datetime and obj_datetime <= latest_datetime:
                    continue

                valid_objects.append(db_obj)
                RECORDS_PROCESSED.inc()
            except Exception:
                # Remediation attempt
                try:
                    cleaned_item = self._clean_item(item)
                    db_obj = self._create_stop_search_object(cleaned_item, force)

                    # Skip if record is older or equal to latest in DB
                    obj_datetime = cast(datetime, db_obj.datetime)
                    if (
                        latest_datetime
                        and obj_datetime
                        and obj_datetime <= latest_datetime
                    ):
                        continue

                    valid_objects.append(db_obj)
                    RECORDS_PROCESSED.inc()
                except Exception as e2:
                    # Inject force into the item for context if it fails
                    failed_item = item.copy()
                    failed_item["force"] = force
                    failed_rows.append({"raw_data": failed_item, "reason": str(e2)})
                    FAILED_ROWS.inc()

        logger.info(
            f"Processed force {force}: {len(valid_objects)} new valid rows, {len(failed_rows)} failed rows"
        )

        return valid_objects, failed_rows

    def bulk_insert_from_csv(self, file_path: str):
        """
        Inserts data from a CSV file using COPY command.
        """
        try:
            # We need to use psycopg2 cursor for copy_expert
            conn = self.db.connection().connection
            cursor = conn.cursor()

            with open(file_path, "r") as f:
                # Assuming the CSV matches the table structure exactly
                # or we specify columns
                # For simplicity, let's assume we are writing all columns in order
                # But StopSearch has many columns. It's safer to specify them.
                # However, constructing the COPY command with all columns is verbose.
                # Let's rely on the CSV having a header and use CSV HEADER.

                row_count = sum(1 for line in f) - 1  # Exclude header
                f.seek(0)  # Reset file pointer to the beginning

                columns = (
                    "force, type, involved_person, datetime, operation, "
                    "operation_name, latitude, longitude, street_id, street_name, "
                    "gender, age_range, self_defined_ethnicity, "
                    "officer_defined_ethnicity, legislation, object_of_search, "
                    "outcome, outcome_linked_to_object_of_search, "
                    "removal_of_more_than_outer_clothing, outcome_object_id, "
                    "outcome_object_name"
                )
                sql = f"COPY stop_searches ({columns}) FROM STDIN WITH CSV HEADER"
                cursor.copy_expert(sql, f)

            conn.commit()
            # self.db.commit() # Committing via raw connection to ensure COPY is persisted

            logger.info(f"Successfully bulk inserted {row_count} rows from {file_path}")
        except Exception as e:
            logger.error(f"Bulk insert from CSV failed: {e}")
            conn.rollback()
            # self.db.rollback()
            raise

    def _fetch_data(
        self, force: str, date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        params = {"force": force}

        if date:
            params["date"] = date

        return self._make_request(BASE_URL, params)

    def get_available_dates(self) -> Dict[str, List[str]]:
        """
        Fetches available dates from the API.
        Returns a dictionary mapping force IDs to a list of available dates.
        """
        try:
            data = self._make_request(AVAILABILITY_URL)
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

            # Sort dates for each force
            for force_id in availability:
                availability[force_id].sort()

            return availability
        except Exception as e:
            logger.error(f"Failed to fetch available dates: {e}")
            return {}

    def _make_request(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Makes a request to the API with rate limiting handling.
        """
        max_retries = 5
        base_delay = 1

        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=30)

                if response.status_code == 429:
                    # Rate limited
                    retry_after = int(
                        response.headers.get("Retry-After", base_delay * (2**attempt))
                    )
                    logger.warning(
                        f"Rate limited. Retrying after {retry_after} seconds..."
                    )
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.RequestException as e:
                if attempt == max_retries - 1:
                    raise

                delay = base_delay * (2**attempt) + random.uniform(0, 1)
                logger.warning(
                    f"Request failed: {e}. Retrying in {delay:.2f} seconds..."
                )
                time.sleep(delay)

    def get_latest_date(self, force: str) -> Optional[datetime]:
        """
        Public method to get the latest datetime of a record for the given force.
        """
        return self._get_latest_datetime(force)

    def _get_latest_datetime(self, force: str) -> Optional[datetime]:
        """
        Get the latest datetime of a record for the given force.
        """
        return cast(
            Optional[datetime],
            self.db.query(func.max(StopSearch.datetime))
            .filter(StopSearch.force == force)
            .scalar(),
        )

    def _clean_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean up known data issues in the item.
        """
        # Convert empty strings to None for boolean fields
        bool_fields = [
            "operation",
            "outcome_linked_to_object_of_search",
            "removal_of_more_than_outer_clothing",
            "involved_person",
        ]
        for field in bool_fields:
            if field in item and item[field] == "":
                item[field] = None
        return item

    def _create_stop_search_object(
        self, item: Dict[str, Any], force: str
    ) -> StopSearch:
        """
        Validate and create a StopSearch object from a dictionary item.
        """
        schema_item = StopSearchCreate(**item)

        # Extract location details
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

        # Extract outcome object details
        outcome_object_id = None
        outcome_object_name = None

        if schema_item.outcome_object:
            outcome_object_id = schema_item.outcome_object.id
            outcome_object_name = schema_item.outcome_object.name

        return StopSearch(
            force=force,
            type=schema_item.type,
            involved_person=schema_item.involved_person,
            datetime=schema_item.datetime,
            operation=schema_item.operation,
            operation_name=schema_item.operation_name,
            latitude=latitude,
            longitude=longitude,
            street_id=street_id,
            street_name=street_name,
            gender=schema_item.gender,
            age_range=schema_item.age_range,
            self_defined_ethnicity=schema_item.self_defined_ethnicity,
            officer_defined_ethnicity=schema_item.officer_defined_ethnicity,
            legislation=schema_item.legislation,
            object_of_search=schema_item.object_of_search,
            outcome=schema_item.outcome,
            outcome_linked_to_object_of_search=schema_item.outcome_linked_to_object_of_search,
            removal_of_more_than_outer_clothing=schema_item.removal_of_more_than_outer_clothing,
            outcome_object_id=outcome_object_id,
            outcome_object_name=outcome_object_name,
        )

    @PROCESSING_TIME.time()
    def _process_data(self, force: str, data: List[Dict[str, Any]]):
        # Deprecated in favor of _process_data_in_memory
        pass

    def remediate_failed_rows(self):
        """
        Attempts to re-process rows from the failed_rows table.
        """
        failed_rows = self.db.query(FailedRow).all()
        logger.info(f"Found {len(failed_rows)} failed rows to remediate.")

        remediated_count = 0

        for row in failed_rows:
            try:
                data = row.raw_data

                # Handle case where data might be stored as a string representation
                # of a dict
                if isinstance(data, str):
                    import ast

                    try:
                        item = ast.literal_eval(data)
                    except (ValueError, SyntaxError):
                        try:
                            item = json.loads(data)
                        except json.JSONDecodeError:
                            logger.warning(f"Could not parse raw_data for row {row.id}")
                            continue
                else:
                    item = data

                item_dict = cast(Dict[str, Any], item)

                # Remediation: Clean up known data issues
                item_dict = self._clean_item(item_dict)

                force = cast(str, item_dict.get("force", "unknown"))
                db_obj = self._create_stop_search_object(item_dict, force)

                self.db.add(db_obj)
                self.db.delete(row)
                remediated_count += 1

            except Exception as e:
                logger.error(f"Failed to remediate row {row.id}: {e}")

        if remediated_count > 0:
            self.db.commit()
            logger.info(f"Successfully remediated {remediated_count} rows.")
        else:
            logger.info("No rows were remediated.")
