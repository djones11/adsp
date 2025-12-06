import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.tasks.stop_search_tasks import ingest_stop_searches

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    try:
        # Trigger the task asynchronously - runs in celery worker
        ingest_stop_searches.delay()

        logger.info(
            "Ingest stop searches job triggered. "
            'Use "invoke logs.view -s worker" to monitor progress.'
        )
    except Exception as e:
        logger.error(f"Error triggering ingest stop searches job: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
