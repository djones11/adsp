import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from tenacity import stop_after_attempt

from app.core.http_client import RateLimitError, make_request, make_request_async


def test_make_request_returns_json_on_success():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": "ok"}

    with patch("httpx.get", return_value=mock_response):
        result = make_request("http://test.com")
        assert result == {"data": "ok"}


def test_make_request_retries_on_rate_limit_error():
    # First response 429, second 200
    mock_response_429 = MagicMock()
    mock_response_429.status_code = 429
    mock_response_429.headers = {"Retry-After": "0"}

    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {"data": "ok"}

    with patch("httpx.get", side_effect=[mock_response_429, mock_response_200]):
        result = make_request("http://test.com")
        assert result == {"data": "ok"}


def test_make_request_retries_on_request_error():
    # First raises exception, second succeeds
    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {"data": "ok"}

    with patch(
        "httpx.get",
        side_effect=[
            httpx.RequestError("Error", request=MagicMock()),
            mock_response_200,
        ],
    ):
        result = make_request("http://test.com")
        assert result == {"data": "ok"}


def test_make_request_raises_error_after_max_retries():
    # Override retry stop to speed up test
    make_request.retry.stop = stop_after_attempt(2)

    with patch(
        "httpx.get", side_effect=httpx.RequestError("Error", request=MagicMock())
    ):
        with pytest.raises(httpx.RequestError):
            make_request("http://test.com")


def test_make_request_raises_rate_limit_error_after_max_retries():
    # Override retry stop to speed up test
    make_request.retry.stop = stop_after_attempt(2)

    mock_response_429 = MagicMock()
    mock_response_429.status_code = 429
    mock_response_429.headers = {"Retry-After": "0"}

    with patch("httpx.get", return_value=mock_response_429):
        with pytest.raises(RateLimitError):
            make_request("http://test.com")


def test_make_request_async_returns_json_on_success():
    async def run_test():
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "ok"}

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response

        result = await make_request_async("http://test.com", client=mock_client)
        assert result == {"data": "ok"}

    asyncio.run(run_test())


def test_make_request_async_creates_client_if_none_provided():
    async def run_test():
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "ok"}

        mock_async_client = AsyncMock()
        mock_async_client.__aenter__.return_value = mock_async_client
        mock_async_client.get.return_value = mock_response

        with patch("httpx.AsyncClient", return_value=mock_async_client):
            result = await make_request_async("http://test.com")
            assert result == {"data": "ok"}

    asyncio.run(run_test())


def test_make_request_async_rate_limit():
    async def run_test():
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "5"}

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response

        with pytest.raises(RateLimitError) as excinfo:
            await make_request_async("http://test.com", client=mock_client)

        assert excinfo.value.retry_after == 5.0

    asyncio.run(run_test())
