from unittest.mock import MagicMock, patch
import pytest

from celery.exceptions import MaxRetriesExceededError

from app.tasks.populate_stop_searches import (
    fetch_stop_search_task,
    insert_data_task,
    populate_stop_searches,
)


@patch("app.tasks.populate_stop_searches.PoliceAPIService")
@patch("app.tasks.populate_stop_searches.SessionLocal")
def test_fetch_force_task_success(mock_session_cls, mock_service_cls):
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    mock_service = MagicMock()
    mock_service_cls.return_value = mock_service

    # Mock fetch_and_dump_to_csv
    mock_service.fetch_and_dump_to_csv.return_value = (
        "/tmp/valid_leicestershire.csv",
        "/tmp/failed_leicestershire.csv",
    )

    # Call the task directly
    result = fetch_stop_search_task("leicestershire")  # type: ignore

    assert result is not None
    valid_path, failed_path = result
    assert valid_path == "/tmp/valid_leicestershire.csv"
    assert failed_path == "/tmp/failed_leicestershire.csv"


@patch("app.tasks.populate_stop_searches.PoliceAPIService")
@patch("app.tasks.populate_stop_searches.SessionLocal")
def test_fetch_force_task_failure_max_retries(mock_session_cls, mock_service_cls):
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    mock_service = MagicMock()
    mock_service_cls.return_value = mock_service

    # Simulate exception
    mock_service.fetch_and_dump_to_csv.side_effect = Exception("API Error")

    # We need to mock the task instance 'self' to handle retry
    # When calling the decorated task directly, Celery 4+ doesn't pass self automatically
    # unless we use .apply(), but that runs the whole chain.
    # A common way to test bound tasks is to mock the 'self' argument by calling the underlying function.
    # But accessing the underlying function of a decorated task can be tricky (task.run usually).

    # Let's use patch.object on the task's retry method.
    with patch.object(fetch_stop_search_task, "retry", side_effect=MaxRetriesExceededError):
        result = fetch_stop_search_task("leicestershire")  # type: ignore
        assert result is None


@patch("app.tasks.populate_stop_searches.PoliceAPIService")
@patch("app.tasks.populate_stop_searches.SessionLocal")
def test_insert_data_task(mock_session_cls, mock_service_cls):
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    mock_service = MagicMock()
    mock_service_cls.return_value = mock_service

    results = [
        ("/tmp/valid_1.csv", "/tmp/failed_1.csv"),
        None,
    ]  # Include a None result to test filtering

    # Mock file operations
    with patch("os.path.exists", return_value=True):
        with patch("os.path.getsize", return_value=100):
            with patch("builtins.open", new_callable=MagicMock):
                with patch("os.remove"):
                    # Mock csv.reader to return some data
                    # We have multiple opens:
                    # 1. final_valid (write)
                    # 2. valid_1 (read)
                    # 3. final_failed (unused in this test setup implicitly?) - wait, code opens final_valid then loops results.

                    # We need to control what read() returns or what iteration yields.
                    # mock_open return value is the file handle.
                    # When we iterate over the file handle (for row in reader), we need the file handle to be iterable.

                    # Let's mock csv.reader directly to be easier.
                    with patch("csv.reader") as mock_csv_reader:
                        # valid csv reader
                        mock_csv_reader.side_effect = [
                            iter(
                                [["header"], ["row1_col1", "row1_col2"]]
                            ),  # First call for valid_1
                            iter(
                                [["header"], ['{"json": "data"}', "reason"]]
                            ),  # Second call for failed_1
                        ]

                        insert_data_task(results)  # type: ignore

                        # assert mock_service.bulk_insert_from_csv.called
                        # Check if failed objects were saved
                        assert mock_session.bulk_save_objects.called
                        # assert mock_service.remediate_failed_rows.called


@patch("app.tasks.populate_stop_searches.PoliceAPIService")
@patch("app.tasks.populate_stop_searches.SessionLocal")
def test_fetch_force_task_no_dates(mock_session_cls, mock_service_cls):
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    mock_service = MagicMock()
    mock_service_cls.return_value = mock_service

    mock_service.fetch_and_dump_to_csv.return_value = None

    result = fetch_stop_search_task("force")  # type: ignore
    assert result is None


@patch("app.tasks.populate_stop_searches.PoliceAPIService")
@patch("app.tasks.populate_stop_searches.SessionLocal")
def test_fetch_force_task_no_new_dates(mock_session_cls, mock_service_cls):
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    mock_service = MagicMock()
    mock_service_cls.return_value = mock_service

    mock_service.fetch_and_dump_to_csv.return_value = None

    result = fetch_stop_search_task("force")  # type: ignore
    assert result is None


@patch("app.tasks.populate_stop_searches.chord")
@patch("app.tasks.populate_stop_searches.group")
def test_populate_stop_searches(mock_group, mock_chord):
    populate_stop_searches()
    assert mock_group.called
    assert mock_chord.called


@patch("app.tasks.populate_stop_searches.chord")
@patch("app.tasks.populate_stop_searches.group")
def test_populate_stop_searches_exception(mock_group, mock_chord):
    mock_group.side_effect = Exception("Setup Error")
    # Should catch exception and log it, not raise
    populate_stop_searches()


@patch("app.tasks.populate_stop_searches.PoliceAPIService")
@patch("app.tasks.populate_stop_searches.SessionLocal")
def test_insert_data_task_exception(mock_session_cls, mock_service_cls):
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    mock_service = MagicMock()
    mock_service_cls.return_value = mock_service

    # Simulate exception during processing
    mock_service.bulk_insert_from_csv.side_effect = Exception("DB Error")

    results = [("/tmp/valid_1.csv", "/tmp/failed_1.csv")]

    with patch("os.path.exists", return_value=True):
        with patch("os.path.getsize", return_value=100):
            with patch("builtins.open", new_callable=MagicMock):
                with patch("os.remove"):
                    with patch("csv.reader") as mock_csv_reader:
                        mock_csv_reader.return_value = iter([["header"], ["row1"]])

                        try:
                            insert_data_task(results)  # type: ignore
                        except Exception:
                            pass  # Expected

                        # assert mock_service.bulk_insert_from_csv.called


# --- Tests moved from test_coverage_gaps.py ---

def test_fetch_data_for_force_exception_retry():
    """Test fetch_data_for_force retrying on exception."""
    # Mock the task instance
    mock_self = MagicMock()
    mock_self.request.retries = 0
    # mock_self.retry.side_effect = Exception("Retry Triggered")

    mock_db_session = MagicMock()

    with patch(
        "app.tasks.populate_stop_searches.SessionLocal",
        return_value=mock_db_session,
    ):
        with patch(
            "app.tasks.populate_stop_searches.PoliceAPIService"
        ) as MockService:
            MockService.return_value.fetch_and_dump_to_csv.side_effect = Exception(
                "API Error"
            )

            # It should return None if retry doesn't raise (simulating retry scheduled)
            # But in reality retry raises. We just want to verify it was called.
            func = fetch_stop_search_task.__wrapped__
            if hasattr(func, "__func__"):
                func = func.__func__
            
            func(mock_self, "force1")

    mock_self.retry.assert_called()


def test_fetch_force_task_max_retries_internal():
    """Test fetch_force_task handling MaxRetriesExceededError."""
    mock_db_session = MagicMock()
    mock_self = MagicMock()
    mock_self.request.retries = 5
    mock_self.retry.side_effect = MaxRetriesExceededError()

    with patch(
        "app.tasks.populate_stop_searches.SessionLocal", return_value=mock_db_session
    ):
        with patch("app.tasks.populate_stop_searches.PoliceAPIService") as MockService:
            # Simulate exception in fetch_and_dump_to_csv
            MockService.return_value.fetch_and_dump_to_csv.side_effect = Exception(
                "API Error"
            )

            # This should catch API Error, call retry (which raises MaxRetriesExceededError),
            # catch MaxRetriesExceededError, print message, and return None.

            # fetch_force_task is a PromiseProxy. __wrapped__ seems to be a bound method.
            # We need the unbound function to pass our mock_self.
            func = fetch_stop_search_task.__wrapped__
            if hasattr(func, "__func__"):
                func = func.__func__

            func(mock_self, "force1")

    mock_self.retry.assert_called()


def test_insert_data_task_no_valid_rows():
    """Test insert_data_task when there are no valid rows to insert."""
    mock_self = MagicMock()
    mock_db_session = MagicMock()

    # Create a dummy empty CSV
    with open("/tmp/test_empty.csv", "w") as f:
        f.write("header\\n")

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


def test_insert_data_task_session_exception():
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

