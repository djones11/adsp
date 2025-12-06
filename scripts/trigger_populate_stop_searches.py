import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.tasks.populate_stop_searches import populate_stop_searches

logger = logging.getLogger(__name__)


def main():
    try:
        # Trigger the task asynchronously - runs in celery worker
        populate_stop_searches.delay()

        logger.info(
            "Populate stop searches job triggered. "
            'Use "invoke logs.view -s worker" to monitor progress.'
        )
    except Exception as e:
        logger.error(f"Error triggering populate stop searches job: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
