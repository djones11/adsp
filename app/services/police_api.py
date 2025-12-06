import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, cast

from prometheus_client import Counter, Summary
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.config import settings, AVAILABLE_FORCES
from app.core.http_client import make_request
from app.models.failed_row import FailedRow
from app.models.stop_search import StopSearch
from app.schemas.stop_search import StopSearchBase
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

FAILED_ROW_COLUMNS = [
    "raw_data",
    "reason"
]

BASE_POLICE_URL = "https://data.police.uk/api"
STOP_SEARCH_URL = f"{BASE_POLICE_URL}/stops-street"
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

    def download_stop_search_data(
        self, 
        force: AVAILABLE_FORCES, 
        output_dir: str = "/tmp"
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

        for date in dates_to_fetch:
            valid_objects, failed_rows = self._fetch_stop_search_data(force, date)

            all_valid_objects.extend(valid_objects)
            all_failed_rows.extend(failed_rows)        

        # Write valid objects to CSV
        valid_csv_path = os.path.join(output_dir, f"valid_{force}.csv")
        CSVHandler.write_rows(
            valid_csv_path, 
            all_valid_objects, 
            STOP_SEARCH_COLUMNS
        )

        # Write failed rows to CSV
        failed_csv_path = os.path.join(output_dir, f"failed_{force}.csv")
        CSVHandler.write_rows(
            failed_csv_path, 
            all_failed_rows, 
            FAILED_ROW_COLUMNS
        )

        return valid_csv_path, failed_csv_path
    
    @PROCESSING_TIME.time()
    def _fetch_stop_search_data(
        self, 
        force: AVAILABLE_FORCES, 
        date: str
    ) -> tuple[List[StopSearch], List[FailedRow]]:
        """
        Fetches and processes data for a single force.
        Returns a tuple of (valid_rows, failed_rows).
        """
        try:
            logger.info(f"Fetching data for force: {force}, date: {date}")
            params = {"force": force}

            if date:
                params["date"] = date

            data = make_request(STOP_SEARCH_URL, params)    

            if data:
                return self._process_stop_search_data(force, data)
            else:
                logger.info(f"No data found for force: {force}")
                return [], []

        except Exception as e:
            # Caught exceptions to prevent entire job failure if one force fails
            logger.error(f"Error processing force {force}: {e}")
            return [], []
        
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
        self, 
        force: AVAILABLE_FORCES, 
        data: List[Dict[str, Any]]
    ) -> tuple[List[StopSearch], List[FailedRow]]:
        """
        Converts raw data into StopSearch/FailedRow objects, 
        separating valid and failed rows.
        """
        valid_rows = []
        failed_rows = []

        logger.info(f"Processing {len(data)} records for force: {force}")

        for item in data:
            # Will first attempt normal processing, if this fails then attempt
            # known remediations and try again, if that fails then move to failed_rows
            try:
                db_obj = self._create_stop_search_object(item, force)

                valid_rows.append(db_obj)
                RECORDS_PROCESSED.inc()
            except Exception:
                # Remediation attempt
                try:
                    cleaned_item = DataCleaner.clean(item)
                    db_obj = self._create_stop_search_object(cleaned_item, force)

                    valid_rows.append(db_obj)
                    RECORDS_PROCESSED.inc()
                except Exception as e2:
                    failed_rows.append({"raw_data": item, "reason": str(e2)})
                    FAILED_ROWS.inc()

        logger.info(
            f"Processed force {force}: {len(valid_rows)} new valid rows, {len(failed_rows)} failed rows"
        )

        return valid_rows, failed_rows       

    def _create_stop_search_object(
        self, 
        item: Dict[str, Any], 
        force: AVAILABLE_FORCES
    ) -> StopSearch:
        """
        Validate and create a StopSearch object from a dictionary item.
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