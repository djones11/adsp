import csv
import io
import logging
from typing import Any, Dict, List

from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.db.session import engine
from app.models.failed_row import FailedRow
from app.schemas.item import ItemCreate

logger = logging.getLogger(__name__)


class DataImporter:
    def __init__(self, db: Session):
        self.db = db

    def process_file(self, file_path: str, chunk_size: int = 1000):
        """
        Reads a CSV file, validates rows, and bulk inserts using COPY.
        Bad rows are saved to the database.
        """
        logger.info(f"Starting import from {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            chunk = []
            for row in reader:
                chunk.append(row)
                if len(chunk) >= chunk_size:
                    self._process_chunk(chunk)
                    chunk = []

            if chunk:
                self._process_chunk(chunk)

    def _process_chunk(self, rows: List[Dict[str, Any]]):
        valid_rows = []
        failed_rows = []

        for row in rows:
            try:
                # Validate using Pydantic
                item = ItemCreate(**row)
                # Prepare for CSV COPY (order must match DB columns or specify them)
                # Assuming DB columns: name, description, value
                # (id is auto, created_at is auto)
                valid_rows.append(item.model_dump())
            except ValidationError as e:
                failed_rows.append({"raw_data": row, "reason": str(e)})
            except Exception as e:
                failed_rows.append(
                    {"raw_data": row, "reason": f"Unexpected error: {str(e)}"}
                )

        if valid_rows:
            self._bulk_insert_copy(valid_rows)

        if failed_rows:
            self._insert_failed_rows(failed_rows)

    def _bulk_insert_copy(self, data: List[Dict[str, Any]]):
        """
        Uses Postgres COPY for high-performance insertion.
        """
        if not data:
            return

        # Create a CSV buffer
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header is not needed for copy_from if we don't use header,
        # but we need to ensure column order matches.
        # Columns: name, description, value
        for row in data:
            writer.writerow([row["name"], row["description"], row["value"]])

        output.seek(0)

        conn = engine.raw_connection()
        try:
            cursor = conn.cursor()
            # items table columns: name, description, value
            cursor.copy_expert(
                "COPY items (name, description, value) FROM STDIN WITH CSV", output
            )
            conn.commit()
            logger.info(f"Successfully inserted {len(data)} rows.")
        except Exception as e:
            logger.error(f"Bulk insert failed: {e}")
            conn.rollback()
            # Fallback strategy could be implemented here (e.g., row-by-row)
        finally:
            conn.close()

    def _insert_failed_rows(self, failures: List[Dict[str, Any]]):
        try:
            self.db.bulk_insert_mappings(FailedRow.__mapper__, failures)
            self.db.commit()

            logger.info(f"Recorded {len(failures)} failed rows.")
        except Exception as e:
            logger.error(f"Failed to record failed rows: {e}")
            self.db.rollback()
