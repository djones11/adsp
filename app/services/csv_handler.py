import csv
import io
import logging
import os
from typing import Any, List

from sqlalchemy.orm import Session

from app.models.failed_row import FailedRow
from app.models.stop_search import StopSearch

logger = logging.getLogger(__name__)


class CSVHandler:
    @staticmethod
    def write_rows(file_path: str, objects: List[Any], columns: List[str]) -> None:
        """
        Writes a list of objects to a CSV file using the provided columns.
        """
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)

            for obj in objects:
                row = []

                for col in columns:
                    if isinstance(obj, dict):
                        row.append(obj.get(col))
                    else:
                        row.append(getattr(obj, col))
                writer.writerow(row)

    @staticmethod
    def merge_csvs(
        output_path: str,
        input_paths: List[str],
        columns: List[str],
        cleanup: bool = True,
    ) -> None:
        """
        Merges multiple CSV files into one, assuming they all have the same header.
        The header is written only once.
        If cleanup is True, input files are deleted after merging.
        """

        if os.path.exists(output_path):
            os.remove(output_path)

        with open(output_path, "w", newline="", encoding="utf-8") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(columns)

            for path in input_paths:
                if path and os.path.exists(path):
                    with open(path, "r", encoding="utf-8") as infile:
                        reader = csv.reader(infile)
                        next(reader, None)  # Skip header

                        for row in reader:
                            writer.writerow(row)

                    if cleanup:
                        os.remove(path)
                else:
                    logger.warning(f"Merge skipped missing file: {path}")

    @staticmethod
    def read_rows(
        file_path: str,
    ) -> List[List[str]]:
        """
        Reads rows from a CSV file and returns a list of rows.
        """
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader, None)  # Skip header

                return list(reader)
        else:
            return []

    @staticmethod
    def bulk_insert_from_csv(
        db: Session, file_path: str, columns: List[str], table_name: str
    ) -> None:
        """
        Inserts data from a CSV file using COPY command.
        First attempts to copy the entire file.
        If that fails, falls back to adaptive chunking.
        """
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            return

        columns_str = ", ".join(columns)

        # Attempt 1: Full Copy
        conn = db.connection().connection
        cursor = conn.cursor()

        try:
            cursor.execute("SAVEPOINT full_copy_savepoint")

            with open(file_path, "r", encoding="utf-8") as f:
                sql = f"COPY {table_name} ({columns_str}) FROM STDIN WITH CSV HEADER"
                cursor.copy_expert(sql, f)

            cursor.execute("RELEASE SAVEPOINT full_copy_savepoint")

            db.commit()

            logger.info(f"Finished processing {file_path} (Full Copy)")

            return
        except Exception as e:
            logger.warning(f"Full copy failed for {file_path}, falling back to chunking: {e}")
            try:
                cursor.execute("ROLLBACK TO SAVEPOINT full_copy_savepoint")
            except Exception:
                pass
        finally:
            cursor.close()

        # Attempt 2: Adaptive Chunking
        BATCH_SIZE = 1000

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                header = f.readline()
                
                if not header:
                    return

                batch = []
                
                for line in f:
                    batch.append(line)

                    if len(batch) >= BATCH_SIZE:
                        CSVHandler._insert_batch(
                            db, batch, header, columns_str, table_name
                        )
                        batch = []

                if batch:
                    CSVHandler._insert_batch(db, batch, header, columns_str, table_name)

            db.commit()
            logger.info(f"Finished processing {file_path} (Chunked)")

        except Exception as e:
            logger.error(f"Critical error processing CSV {file_path}: {e}")
            db.rollback()
            raise

    @staticmethod
    def _insert_batch(
        db: Session,
        rows: List[str],
        header: str,
        columns_str: str,
        table_name: str,
    ) -> None:
        """
        Inserts a batch of rows into the database.
        If the batch fails, it splits it into smaller chunks recursively.
        """
        conn = db.connection().connection
        cursor = conn.cursor()

        try:
            cursor.execute("SAVEPOINT batch_savepoint")

            s_io = io.StringIO()
            s_io.write(header)
            s_io.writelines(rows)
            s_io.seek(0)

            sql = f"COPY {table_name} ({columns_str}) FROM STDIN WITH CSV HEADER"
            cursor.copy_expert(sql, s_io)

            cursor.execute("RELEASE SAVEPOINT batch_savepoint")

        except Exception as e:
            try:
                cursor.execute("ROLLBACK TO SAVEPOINT batch_savepoint")
            except Exception:
                # If rollback fails, the transaction might be irretrievably broken
                pass

            if len(rows) == 1:
                CSVHandler._handle_failed_row(db, rows[0], header, str(e), table_name)
            else:
                # Adaptive splitting: 1000 -> 100 -> 10 -> 1
                chunk_size = max(1, len(rows) // 10)
                for i in range(0, len(rows), chunk_size):
                    sub_batch = rows[i : i + chunk_size]
                    CSVHandler._insert_batch(
                        db, sub_batch, header, columns_str, table_name
                    )
        finally:
            cursor.close()

    @staticmethod
    def _handle_failed_row(
        db: Session, row_line: str, header: str, error_msg: str, table_name: str
    ) -> None:
        logger.warning(f"Row failed in {table_name}: {error_msg}")
        if table_name == StopSearch.__tablename__:
            try:
                # Parse failed row to store in FailedRow table
                row_reader = csv.DictReader(io.StringIO(header + row_line))
                row_dict = next(row_reader)

                failed_row = FailedRow(
                    raw_data=row_dict, reason=error_msg, source=table_name
                )
                db.add(failed_row)
            except Exception as parse_error:
                logger.error(f"Failed to process failed row: {parse_error}")
        else:
            logger.error(f"Failed to insert row into {table_name}: {error_msg}")
