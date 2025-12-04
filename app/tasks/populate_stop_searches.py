import csv
import json
import logging
import os
from typing import List, Optional, Tuple

from celery import chord, group  # type: ignore
from celery.exceptions import MaxRetriesExceededError

from app.core.celery_app import celery_app
from app.core.config import settings
from app.db.session import SessionLocal
from app.models.failed_row import FailedRow
from app.services.police_api import PoliceAPIService

logger = logging.getLogger(__name__)
POLICE_FORCES = settings.POLICE_FORCES


@celery_app.task(bind=True, max_retries=5)
def fetch_force_task(
    self, force: str, target_date: Optional[str] = None
) -> Optional[Tuple[str, str]]:
    """
    Fetches data for a single force and writes to temp CSVs.
    Handles backfill by checking available dates and latest DB record.
    Returns paths to (valid_csv, failed_csv).
    If the task fails after retries, it returns None to allow the chord to continue.
    """
    logger.info(f"Starting fetch task for {force} (target_date={target_date})")
    db = SessionLocal()

    try:
        service = PoliceAPIService(db)

        # 1. Get available dates for this force
        availability = service.get_available_dates()
        available_dates = availability.get(force, [])

        if not available_dates:
            logger.warning(f"No available dates found for force: {force}")
            return None

        # 2. Get latest date from DB
        latest_datetime = service.get_latest_date(force)
        latest_date_str = latest_datetime.strftime("%Y-%m") if latest_datetime else None

        logger.info(f"Latest date in DB for {force}: {latest_date_str}")

        # 3. Filter dates to fetch
        dates_to_fetch = []
        for date in available_dates:
            # If we have a target date, skip dates after it
            if target_date and date > target_date:
                continue

            # If we have data in DB, skip dates before or equal to latest
            if latest_date_str and date <= latest_date_str:
                continue

            dates_to_fetch.append(date)

        if not dates_to_fetch:
            logger.info(f"No new dates to fetch for {force}")
            return None

        logger.info(f"Fetching dates for {force}: {dates_to_fetch}")

        all_valid_objects = []
        all_failed_rows = []

        # 4. Fetch data for each date
        for date in dates_to_fetch:
            valid_objects, failed_rows = service.fetch_and_process_force(force, date)
            all_valid_objects.extend(valid_objects)
            all_failed_rows.extend(failed_rows)

        # Write valid objects to CSV
        valid_csv_path = f"/tmp/valid_{force}.csv"

        with open(valid_csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            # Write header matching the COPY command order
            writer.writerow(
                [
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
            )

            for obj in all_valid_objects:
                writer.writerow(
                    [
                        obj.force,
                        obj.type,
                        obj.involved_person,
                        obj.datetime,
                        obj.operation,
                        obj.operation_name,
                        obj.latitude,
                        obj.longitude,
                        obj.street_id,
                        obj.street_name,
                        obj.gender,
                        obj.age_range,
                        obj.self_defined_ethnicity,
                        obj.officer_defined_ethnicity,
                        obj.legislation,
                        obj.object_of_search,
                        obj.outcome,
                        obj.outcome_linked_to_object_of_search,
                        obj.removal_of_more_than_outer_clothing,
                        obj.outcome_object_id,
                        obj.outcome_object_name,
                    ]
                )

        # Write failed rows to CSV (or JSON lines)
        # FailedRow has raw_data (JSONB) and reason.
        failed_csv_path = f"/tmp/failed_{force}.csv"

        with open(failed_csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["raw_data", "reason"])

            for row in all_failed_rows:
                writer.writerow([json.dumps(row["raw_data"]), row["reason"]])

        return valid_csv_path, failed_csv_path

    except Exception as e:
        logger.error(f"Error in fetch task for {force}: {e}")

        # Manual retry imlemented instead of autoretry_for to prevent an error
        # when run in a chord causing the whole chord to fail.
        try:
            # Exponential backoff: 2^retries (1s, 2s, 4s, 8s, 16s)
            retry_delay = 2**self.request.retries
            self.retry(exc=e, countdown=retry_delay)
        except MaxRetriesExceededError:
            logger.error(
                f"Max retries exceeded for {force}. "
                "Returning None to allow chord to proceed."
            )

            return None
    finally:
        db.close()


@celery_app.task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 5},
)
def insert_data_task(self, results: List[Optional[Tuple[str, str]]]):
    """
    Consolidates CSVs and performs bulk insert.
    """
    logger.info("Starting bulk insert task")

    db = SessionLocal()

    try:
        service = PoliceAPIService(db)

        final_valid_csv = "/tmp/final_valid.csv"
        final_failed_csv = "/tmp/final_failed.csv"

        # Merge valid CSVs
        with open(final_valid_csv, "w", newline="") as outfile:
            writer = csv.writer(outfile)
            # Write header once
            writer.writerow(
                [
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
            )

            for result in results:
                if not result:
                    continue

                valid_path, _ = result

                if valid_path and os.path.exists(valid_path):
                    with open(valid_path, "r") as infile:
                        reader = csv.reader(infile)
                        next(reader, None)  # Skip header

                        for row in reader:
                            writer.writerow(row)

                    os.remove(valid_path)  # Cleanup

        with open(final_valid_csv, "r") as f:
            line_count = sum(1 for line in f) - 1  # Exclude header

            if line_count > 0:
                service.bulk_insert_from_csv(final_valid_csv)
            else:
                logger.info("No new valid rows to insert.")

        # Handle failed rows (Standard insert for now as FailedRow structure is simple)
        # Or we could use COPY for them too if volume is high.
        # Let's just read and insert using ORM for simplicity as they are likely few.
        failed_objects = []

        for result in results:
            if not result:
                continue

            _, failed_path = result

            if failed_path and os.path.exists(failed_path):
                with open(failed_path, "r") as infile:
                    reader = csv.reader(infile)
                    next(reader, None)

                    for row in reader:
                        if row:
                            failed_objects.append(
                                FailedRow(raw_data=json.loads(row[0]), reason=row[1])
                            )

                os.remove(failed_path)

        if failed_objects:
            db.bulk_save_objects(failed_objects)
            db.commit()
            logger.info(f"Inserted {len(failed_objects)} failed rows.")

        # Cleanup final files
        if os.path.exists(final_valid_csv):
            os.remove(final_valid_csv)

    except Exception as e:
        logger.error(f"Error in bulk insert task: {e}")
        raise
    finally:
        db.close()


@celery_app.task
def populate_stop_searches(date: Optional[str] = None):
    """
    Task scheduled to run at a scheduled time daily.
    Fetches Stop and Search data from the Police API.
    and populates the database.

    Args:
        date: Optional upper bound date (YYYY-MM). If provided, data will be fetched
              up to this date. If not provided, data will be fetched up to the latest
              available date.
    """
    logger.info(f"Starting populate stop searches task (target_date={date})")

    try:
        police_forces = POLICE_FORCES

        # Create a group of tasks for each force
        # We pass the target date (upper bound) to each task
        header = group(fetch_force_task.s(force, date) for force in police_forces)

        # Chain with the insert task
        callback = insert_data_task.s()

        # Execute the chord
        chord(header)(callback)

    except Exception as e:
        logger.error(f"Error triggering daily task chord: {e}")
