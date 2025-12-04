import logging
import os
import sys

# Add the parent directory to sys.path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal
from app.services.police_api import PoliceAPIService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    db = SessionLocal()

    try:
        logger.info("Starting remediation...")
        service = PoliceAPIService(db)
        service.remediate_failed_rows()
        logger.info("Remediation completed.")
    except Exception as e:
        logger.error(f"Error during remediation: {e}")
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
