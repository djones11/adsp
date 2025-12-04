import argparse
import logging
import os
import sys

# Add the parent directory to sys.path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.tasks.populate_stop_searches import populate_stop_searches

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Trigger populate stop searches job")
    parser.add_argument("--date", type=str, help="Date in YYYY-MM format", default=None)
    args = parser.parse_args()

    try:
        logger.info(f"Triggering populate stop searches job via Celery (date={args.date})...")
        # Trigger the task asynchronously
        populate_stop_searches.delay(date=args.date)
        logger.info(
            "Populate stop searches job triggered. "
            'Use "invoke logs.view -s worker" to monitor progress.'
        )
    except Exception as e:
        logger.error(f"Error triggering populate stop searches job: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
