import asyncio
import logging
import os
import random
from typing import List, Optional, Tuple

from celery import chord, group
from celery.exceptions import MaxRetriesExceededError
from sqlalchemy.orm import Session

from app.core.celery_app import celery_app
from app.core.config import AVAILABLE_FORCES, settings
from app.db.session import SessionLocal
from app.services.csv_handler import CSVHandler
from app.services.stop_search_service import (
    FAILED_ROW_COLUMNS,
    STOP_SEARCH_COLUMNS,
    PoliceStopSearchService,
)

logger = logging.getLogger(__name__)

POLICE_FORCES = settings.POLICE_FORCES


def insert_rows(
    db: Session, csv_paths: List[str], columns: List[str], table_name: str
) -> None:
    final_csv_path = "/tmp/final_path.csv"

    if not csv_paths:
        logger.info(f"No CSV paths to process for {table_name}.")
        return

    CSVHandler.merge_csvs(
        final_csv_path,
        csv_paths,
        columns,
    )

    # Check if we have data to insert
    if os.path.exists(final_csv_path):
        with open(final_csv_path, "r") as f:
            line_count = sum(1 for line in f) - 1  # Exclude header

        if line_count > 0:
            logger.info(f"Starting bulk insert of {line_count} rows into {table_name}.")
            CSVHandler.bulk_insert_from_csv(db, final_csv_path, columns, table_name)
        else:
            logger.info(
                f"No rows found in merged file for {table_name}. "
                "Input files might have been empty or missing."
            )
    else:
        logger.error(f"Failed to create merged CSV file at {final_csv_path}")


@celery_app.task(bind=True, max_retries=5)
def fetch_stop_search_task(self, force: AVAILABLE_FORCES) -> Optional[Tuple[str, str]]:
    """
    Fetches data for a single force and writes to temp CSVs.
    Returns paths to (valid_csv, failed_csv).
    If the task fails after retries, it returns None to allow the chord to continue.
    """
    logger.info(f"Starting fetch task for {force}")
    db = SessionLocal()

    try:
        service = PoliceStopSearchService(db)
        return asyncio.run(service.download_stop_search_data(force))

    except Exception as e:
        logger.error(f"Error in fetch task for {force}: {e}")

        # Manual retry implemented instead of autoretry_for to prevent an error
        # when run in a chord causing the whole chord to fail.
        try:
            # Exponential backoff: 2^retries (1s, 2s, 4s, 8s, 16s) with jitter
            retry_delay = (2**self.request.retries) + random.uniform(0, 1)
            self.retry(exc=e, countdown=retry_delay)
            return None
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
        valid_csv_paths = []
        failed_csv_paths = []

        for result in results:
            if result:
                valid_path, failed_path = result

                if valid_path:
                    valid_csv_paths.append(valid_path)

                if failed_path:
                    failed_csv_paths.append(failed_path)

        insert_rows(db, valid_csv_paths, STOP_SEARCH_COLUMNS, "stop_searches")
        insert_rows(db, failed_csv_paths, FAILED_ROW_COLUMNS, "failed_rows")

        logger.info("Bulk insert task completed successfully")
    except Exception as e:
        logger.error(f"Error in bulk insert task: {e}")
        raise
    finally:
        db.close()


@celery_app.task
def ingest_stop_searches():
    """
    Task scheduled to run at a scheduled time daily.
    Fetches Stop and Search data from the Police API.
    and populates the database.
    """
    logger.info("Starting populate stop searches task")

    try:
        police_forces = POLICE_FORCES

        # Create a group of tasks for each force
        header = group(fetch_stop_search_task.s(force) for force in police_forces)

        # Chain with the insert task
        callback = insert_data_task.s()

        # Execute the chord
        chord(header)(callback)

        logger.info("Populate stop searches task completed successfully")
    except Exception as e:
        logger.error(f"Error triggering daily task chord: {e}")
