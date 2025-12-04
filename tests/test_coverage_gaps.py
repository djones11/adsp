import csv
import json
import os
from datetime import datetime
from unittest.mock import MagicMock, mock_open, patch

import pytest
import requests
from celery.exceptions import MaxRetriesExceededError

from app.services.police_api import PoliceAPIService
from app.tasks.populate_stop_searches import fetch_force_task, insert_data_task


@pytest.fixture
def mock_db_session():
    return MagicMock()


def test_get_db():
    """Test get_db dependency yields session and closes it."""
    from app.db.session import get_db

    with patch("app.db.session.SessionLocal") as mock_session_cls:
        mock_session = mock_session_cls.return_value

        gen = get_db()
        db = next(gen)

        assert db == mock_session

        try:
            next(gen)
        except StopIteration:
            pass

        mock_session.close.assert_called_once()


# --- PoliceAPIService Tests ---


def test_bulk_insert_from_csv_exception(mock_db_session):
    """Test exception handling and rollback in bulk_insert_from_csv."""
    service = PoliceAPIService(mock_db_session)

    # Mock the raw connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    # Setup context manager behavior
    mock_cursor.__enter__.return_value = mock_cursor

    # Fix: .connection is a property, so we set the attribute on the return value of connection()
    mock_db_session.connection.return_value.connection = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    # Make copy_expert raise an exception
    mock_cursor.copy_expert.side_effect = Exception("DB Error")

    with patch("builtins.open", mock_open(read_data="header\nrow1")):
        with pytest.raises(Exception, match="DB Error"):
            service.bulk_insert_from_csv("dummy.csv")

    mock_conn.rollback.assert_called_once()


def test_get_available_dates_missing_date(mock_db_session):
    """Test get_available_dates with an entry missing the date field."""
    service = PoliceAPIService(mock_db_session)

    mock_response = [
        {"date": "2023-01", "stop-and-search": ["force1"]},
        {"stop-and-search": ["force2"]},  # Missing date
    ]

    with patch.object(service, "_make_request", return_value=mock_response):
        availability = service.get_available_dates()

    assert "force1" in availability
    assert "force2" not in availability


def test_make_request_rate_limit(mock_db_session):
    """Test _make_request handling 429 Rate Limit."""
    service = PoliceAPIService(mock_db_session)

    mock_response_429 = MagicMock()
    mock_response_429.status_code = 429
    mock_response_429.headers = {"Retry-After": "0"}  # 0 seconds for test speed

    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {"data": "success"}

    with patch("requests.get", side_effect=[mock_response_429, mock_response_200]):
        with patch("time.sleep") as mock_sleep:
            result = service._make_request("http://test.com")

    assert result == {"data": "success"}
    mock_sleep.assert_called()


def test_make_request_max_retries(mock_db_session):
    """Test _make_request raising exception after max retries."""
    service = PoliceAPIService(mock_db_session)

    with patch("requests.get", side_effect=requests.RequestException("Fail")):
        with patch("time.sleep"):  # Speed up test
            with pytest.raises(requests.RequestException):
                service._make_request("http://test.com")


def test_get_latest_date_real(mock_db_session):
    """Test get_latest_date without mocking the private method."""
    service = PoliceAPIService(mock_db_session)

    # Mock the DB query chain
    # self.db.query(...).filter(...).scalar()
    mock_query = mock_db_session.query.return_value
    mock_filter = mock_query.filter.return_value
    mock_filter.scalar.return_value = datetime(2023, 1, 1)

    result = service.get_latest_date("force1")

    assert result == datetime(2023, 1, 1)
    mock_db_session.query.assert_called()


def test_process_data_deprecated(mock_db_session):
    """Test the deprecated _process_data method."""
    service = PoliceAPIService(mock_db_session)
    service._process_data("force1", [])

    # --- Task Tests ---

    def test_fetch_data_for_force_exception_retry(mock_db_session):
        """Test fetch_data_for_force retrying on exception."""
        # Mock the task instance
        mock_self = MagicMock()
        mock_self.request.retries = 0
        # mock_self.retry.side_effect = Exception("Retry Triggered")

        with patch(
            "app.tasks.populate_stop_searches.SessionLocal",
            return_value=mock_db_session,
        ):
            with patch(
                "app.tasks.populate_stop_searches.PoliceAPIService"
            ) as MockService:
                MockService.return_value.get_available_dates.side_effect = Exception(
                    "API Error"
                )

                # It should return None if retry doesn't raise (simulating retry scheduled)
                # But in reality retry raises. We just want to verify it was called.
                fetch_force_task.__wrapped__(mock_self, "force1")

        mock_self.retry.assert_called()


def test_fetch_data_for_force_max_retries(mock_db_session):
    """Test fetch_force_task handling MaxRetriesExceededError."""
    mock_self = MagicMock()
    mock_self.request.retries = 5
    mock_self.retry.side_effect = MaxRetriesExceededError()

    with patch(
        "app.tasks.populate_stop_searches.SessionLocal", return_value=mock_db_session
    ):
        with patch("app.tasks.populate_stop_searches.PoliceAPIService") as MockService:
            # Ensure we enter the loop
            MockService.return_value.get_available_dates.return_value = ["2023-01"]
            MockService.return_value.get_latest_date.return_value = None

            MockService.return_value.get_stop_searches.side_effect = Exception(
                "API Error"
            )

            # This should catch API Error, call retry (which raises MaxRetriesExceededError),
            # catch MaxRetriesExceededError, print message, and return None.

            # fetch_force_task is a PromiseProxy. __wrapped__ seems to be a bound method.
            # We need the unbound function to pass our mock_self.
            func = fetch_force_task.__wrapped__
            if hasattr(func, "__func__"):
                func = func.__func__

            func(mock_self, "force1", "2023-01")

    mock_self.retry.assert_called()


def test_create_stop_search_object_missing_fields(mock_db_session):
    """Test _create_stop_search_object with missing optional fields."""
    service = PoliceAPIService(mock_db_session)

    item = {
        "type": "Person search",
        "involved_person": True,
        "datetime": "2023-01-01T12:00:00+00:00",
        "operation": False,
        "operation_name": None,
        "location": None,  # Missing location
        "gender": "Male",
        "age_range": "18-24",
        "self_defined_ethnicity": "White",
        "officer_defined_ethnicity": "White",
        "legislation": "PACE",
        "object_of_search": "Drugs",
        "outcome": "Arrest",
        "outcome_linked_to_object_of_search": True,
        "removal_of_more_than_outer_clothing": False,
        "outcome_object": None,  # Missing outcome_object
    }

    obj = service._create_stop_search_object(item, "force1")

    assert obj.latitude is None
    assert obj.longitude is None
    assert obj.street_id is None
    assert obj.street_name is None
    assert obj.outcome_object_id is None
    assert obj.outcome_object_name is None

    # Test with location but no street
    item["location"] = {"latitude": "1.0", "longitude": "1.0", "street": None}
    obj = service._create_stop_search_object(item, "force1")
    assert obj.latitude == "1.0"
    assert obj.street_id is None


def test_insert_data_task_no_valid_rows(mock_db_session):
    """Test insert_data_task when there are no valid rows to insert."""
    mock_self = MagicMock()

    # Create a dummy empty CSV
    with open("/tmp/test_empty.csv", "w") as f:
        f.write("header\n")

    results = [("/tmp/test_empty.csv", "/tmp/test_empty_failed.csv")]

    with patch(
        "app.tasks.populate_stop_searches.SessionLocal", return_value=mock_db_session
    ):
        with patch("app.tasks.populate_stop_searches.PoliceAPIService") as MockService:
            with patch("app.tasks.populate_stop_searches.logger") as mock_logger:
                # Mock bulk_insert_from_csv to ensure it's NOT called
                mock_service_instance = MockService.return_value

                # Access the original function (might need double unwrap due to autoretry)
                func = insert_data_task.__wrapped__
                if hasattr(func, "__wrapped__"):
                    func = func.__wrapped__

                # Try calling without self if it's somehow bound, or maybe it expects self
                # If "3 were given", it means self is passed twice.
                # Let's try passing ONLY results.
                try:
                    func(results)
                except TypeError:
                    # Fallback if it was actually unbound
                    func(mock_self, results)

                mock_service_instance.bulk_insert_from_csv.assert_not_called()
                mock_logger.info.assert_any_call("No new valid rows to insert.")


def test_insert_data_task_exception(mock_db_session):
    """Test insert_data_task raising an exception."""
    mock_self = MagicMock()

    with patch(
        "app.tasks.populate_stop_searches.SessionLocal",
        side_effect=Exception("DB Error"),
    ):
        with pytest.raises(Exception, match="DB Error"):
            func = insert_data_task.__wrapped__
            if hasattr(func, "__wrapped__"):
                func = func.__wrapped__
            try:
                func([])
            except TypeError:
                func(mock_self, [])


def test_settings_assemble_db_connection():
    """Test Settings.assemble_db_connection logic."""
    from pydantic import PostgresDsn

    from app.core.config import Settings

    # Case 1: DATABASE_URL provided
    settings = Settings(
        DATABASE_URL="postgresql://user:pass@host:5432/db",
        CELERY_BROKER_URL="redis://",
        CELERY_RESULT_BACKEND="redis://",
    )
    assert str(settings.DATABASE_URL) == "postgresql://user:pass@host:5432/db"

    # Case 2: Components provided
    # We use patch.dict to simulate environment variables, which is how the app runs.
    # We also need to ensure DATABASE_URL is not set in env, or is empty.
    with patch.dict(
        os.environ,
        {
            "POSTGRES_USER": "user",
            "POSTGRES_PASSWORD": "pass",
            "POSTGRES_SERVER": "host",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "db",
            "DATABASE_URL": "",
        },
    ):
        settings = Settings(
            CELERY_BROKER_URL="redis://", CELERY_RESULT_BACKEND="redis://"
        )
        assert str(settings.DATABASE_URL) == "postgresql://user:pass@host:5432/db"
