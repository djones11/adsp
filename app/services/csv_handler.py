import csv
import os
import logging
from typing import Any, List
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

class CSVHandler:
    @staticmethod
    def write_rows(
        file_path: str, 
        objects: List[Any], 
        columns: List[str]
    ) -> None:
        """
        Writes a list of objects to a CSV file using the provided columns.
        """
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            
            for obj in objects:
                writer.writerow([getattr(obj, col) for col in columns])

    @staticmethod
    def merge_csvs(
        output_path: str, 
        input_paths: List[str], 
        columns: List[str], 
        cleanup: bool = True
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
        db: Session, 
        file_path: str, 
        columns: List[str]
    ) -> None:
        """
        Inserts data from a CSV file using COPY command.
        """
        try:
            conn = db.connection().connection
            cursor = conn.cursor()

            with open(file_path, "r") as f:
                row_count = sum(1 for line in f) - 1  # Exclude header
                f.seek(0)

                columns_str = ", ".join(columns)
                
                sql = f"COPY stop_searches ({columns_str}) FROM STDIN WITH CSV HEADER"
                cursor.copy_expert(sql, f)

            conn.commit()

            logger.info(f"Successfully bulk inserted {row_count} rows from {file_path}")
        except Exception as e:
            logger.error(f"Bulk insert from CSV failed: {e}")
            conn.rollback()
            raise