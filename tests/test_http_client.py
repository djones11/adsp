import pytest
from unittest.mock import MagicMock, patch
import requests
from app.core.http_client import make_request, RateLimitError
from tenacity import stop_after_attempt

def test_make_request_success():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": "ok"}

    with patch("requests.get", return_value=mock_response):
        result = make_request("http://test.com")
        assert result == {"data": "ok"}

def test_make_request_rate_limit():
    # First response 429, second 200
    mock_response_429 = MagicMock()
    mock_response_429.status_code = 429
    mock_response_429.headers = {"Retry-After": "0"}

    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {"data": "ok"}

    with patch("requests.get", side_effect=[mock_response_429, mock_response_200]):
        result = make_request("http://test.com")
        assert result == {"data": "ok"}

def test_make_request_retry_exception():
    # First raises exception, second succeeds
    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {"data": "ok"}

    with patch(
        "requests.get",
        side_effect=[requests.RequestException("Error"), mock_response_200],
    ):
        result = make_request("http://test.com")
        assert result == {"data": "ok"}

def test_make_request_max_retries():
    # Override retry stop to speed up test
    make_request.retry.stop = stop_after_attempt(2)
    
    with patch("requests.get", side_effect=requests.RequestException("Error")):
        with pytest.raises(requests.RequestException):
            make_request("http://test.com")

def test_make_request_max_retries_rate_limit():
    # Override retry stop to speed up test
    make_request.retry.stop = stop_after_attempt(2)

    mock_response_429 = MagicMock()
    mock_response_429.status_code = 429
    mock_response_429.headers = {"Retry-After": "0"}
    
    with patch("requests.get", return_value=mock_response_429):
        with pytest.raises(RateLimitError):
            make_request("http://test.com")
