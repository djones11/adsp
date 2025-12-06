import logging
import random
from typing import Any, Dict, Optional

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    retry_if_exception_type,
    before_sleep_log
)

logger = logging.getLogger(__name__)

class RateLimitError(Exception):
    """Raised when a 429 response is received."""
    def __init__(self, retry_after: float):
        super().__init__("Rate limited")
        self.retry_after = retry_after


def rate_limit_wait(retry_state) -> float:
    """Wait function for rate-limited requests with jitter."""
    exc = retry_state.outcome.exception()

    if isinstance(exc, RateLimitError):
        logger.warning(f"Rate limited. Retrying after {exc.retry_after:.2f} seconds...")
        return exc.retry_after
    
    # Exponential backoff with small random jitter to avoid thundering herd
    attempt = retry_state.attempt_number - 1
    delay = 2 ** attempt * (1 + random.uniform(0, 0.1))

    logger.warning(f"Request failed: {exc}. Retrying in {delay:.2f} seconds...")

    return delay


@retry(
    stop=stop_after_attempt(5),  # max retries
    wait=rate_limit_wait,
    retry=retry_if_exception_type((requests.RequestException, RateLimitError)),
    reraise=True,
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def make_request(
    url: str, 
    params: Optional[Dict[str, Any]] = None, 
    timeout: float = 30
) -> Any:
    """
    Makes a request to the API.
    """
    response = requests.get(url, params=params, timeout=timeout)

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        retry_after = float(retry_after) if retry_after is not None else 1

        raise RateLimitError(retry_after)

    response.raise_for_status()

    return response.json()
