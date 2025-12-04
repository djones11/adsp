import logging
import os
import sys

# Add the parent directory to sys.path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal
from app.services.importer import DataImporter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    if len(sys.argv) < 2:
        print("Usage: python initial_load.py <path_to_csv>")
        sys.exit(1)

    file_path = sys.argv[1]

    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        sys.exit(1)

    db = SessionLocal()

    try:
        importer = DataImporter(db)
        importer.process_file(file_path)
        print("Initial load completed.")
    except Exception as e:
        print(f"Error during initial load: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
