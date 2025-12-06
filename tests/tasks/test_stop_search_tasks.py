from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from celery.exceptions import MaxRetriesExceededError

from app.tasks.stop_search_tasks import (
    fetch_stop_search_task,
    ingest_stop_searches,
    insert_data_task,
    insert_rows,
)

# --- Fixtures ---


@pytest.fixture
def mock_db_session():
    with patch("app.tasks.stop_search_tasks.SessionLocal") as mock:
        session = MagicMock()
        mock.return_value = session
        yield session


@pytest.fixture
def mock_service():
    with patch("app.tasks.stop_search_tasks.PoliceStopSearchService") as mock:
        service = MagicMock()
        mock.return_value = service
        yield service


@pytest.fixture
def mock_csv_handler():
    with patch("app.tasks.stop_search_tasks.CSVHandler") as mock:
        yield mock


@pytest.fixture
def mock_celery_self():
    mock_self = MagicMock()
    mock_self.request.retries = 0
    return mock_self


@pytest.fixture
def mock_logger():
    with patch("app.tasks.stop_search_tasks.logger") as mock:
        yield mock


# --- Helper Functions ---


def run_celery_task(task, task_self, *args, **kwargs):
    """Helper to run a Celery task function directly, bypassing the wrapper."""
    func = task.__wrapped__

    if hasattr(func, "__func__"):
        func = func.__func__

    return func(task_self, *args, **kwargs)


# --- Tests ---


def test_insert_rows_logs_info_if_no_csv_paths(mock_logger):
    mock_db = MagicMock()
    insert_rows(mock_db, [], [], "table")
    mock_logger.info.assert_called_with("No CSV paths to process for table.")


def test_insert_rows_logs_error_if_merge_fails(mock_logger):
    mock_db = MagicMock()
    with (
        patch("app.tasks.stop_search_tasks.CSVHandler.merge_csvs"),
        patch("app.tasks.stop_search_tasks.os.path.exists", return_value=False),
    ):
        insert_rows(mock_db, ["path"], [], "table")

        mock_logger.error.assert_called()
        assert "Failed to create merged CSV file" in mock_logger.error.call_args[0][0]


def test_fetch_stop_search_task_returns_csv_paths_on_success(
    mock_db_session, mock_service, mock_celery_self
):
    mock_service.download_stop_search_data = AsyncMock(
        return_value=("/tmp/valid.csv", "/tmp/failed.csv")
    )

    result = run_celery_task(fetch_stop_search_task, mock_celery_self, "leicestershire")

    assert result == ("/tmp/valid.csv", "/tmp/failed.csv")


def test_fetch_stop_search_task_returns_none_on_max_retries_exceeded(
    mock_db_session, mock_service, mock_celery_self
):
    mock_service.download_stop_search_data = AsyncMock(
        side_effect=Exception("API Error")
    )
    mock_celery_self.retry.side_effect = MaxRetriesExceededError

    result = run_celery_task(fetch_stop_search_task, mock_celery_self, "leicestershire")

    assert result is None
    mock_celery_self.retry.assert_called()


def test_insert_data_task_merges_and_inserts_valid_and_failed_rows(
    mock_db_session, mock_service, mock_csv_handler, mock_celery_self
):
    results = [("/tmp/valid_1.csv", "/tmp/failed_1.csv"), None]

    with (
        patch("os.path.exists", return_value=True),
        patch("builtins.open", MagicMock()) as mock_open,
    ):
        mock_file = MagicMock()
        mock_file.__enter__.return_value = mock_file
        mock_file.__iter__.return_value = iter(["header", "row1"])
        mock_open.return_value = mock_file

        run_celery_task(insert_data_task, mock_celery_self, results)

        mock_csv_handler.merge_csvs.assert_called()
        mock_csv_handler.bulk_insert_from_csv.assert_called()


def test_fetch_stop_search_task_returns_none_when_no_dates_available(
    mock_db_session, mock_service, mock_celery_self
):
    mock_service.download_stop_search_data = AsyncMock(return_value=None)

    result = run_celery_task(fetch_stop_search_task, mock_celery_self, "force")

    assert result is None


def test_fetch_stop_search_task_returns_none_when_no_new_data_found(
    mock_db_session, mock_service, mock_celery_self
):
    mock_service.download_stop_search_data = AsyncMock(return_value=None)

    result = run_celery_task(fetch_stop_search_task, mock_celery_self, "force")

    assert result is None


@patch("app.tasks.stop_search_tasks.chord")
@patch("app.tasks.stop_search_tasks.group")
def test_ingest_stop_searches_orchestrates_tasks_with_chord_and_group(
    mock_group, mock_chord
):
    ingest_stop_searches()
    assert mock_group.called
    assert mock_chord.called


@patch("app.tasks.stop_search_tasks.chord")
@patch("app.tasks.stop_search_tasks.group")
def test_ingest_stop_searches_handles_setup_exceptions_gracefully(
    mock_group, mock_chord
):
    mock_group.side_effect = Exception("Setup Error")
    ingest_stop_searches()
    # Should not raise


def test_insert_data_task_handles_db_exceptions_gracefully(
    mock_db_session, mock_service, mock_csv_handler, mock_celery_self
):
    mock_csv_handler.bulk_insert_from_csv.side_effect = Exception("DB Error")
    results = [("/tmp/valid_1.csv", "/tmp/failed_1.csv")]

    with (
        patch("os.path.exists", return_value=True),
        patch("os.path.getsize", return_value=100),
        patch("builtins.open", MagicMock()) as mock_open,
        patch("os.remove"),
        patch("csv.reader", return_value=iter([["header"], ["row1"]])),
    ):
        mock_file = MagicMock()
        mock_file.__enter__.return_value = mock_file
        mock_file.__iter__.return_value = iter(["header", "row1"])
        mock_open.return_value = mock_file

        # Should catch exception
        with pytest.raises(Exception, match="DB Error"):
            run_celery_task(insert_data_task, mock_celery_self, results)


def test_fetch_stop_search_task_retries_on_api_exception(
    mock_db_session, mock_service, mock_celery_self
):
    mock_service.download_stop_search_data = AsyncMock(
        side_effect=Exception("API Error")
    )

    run_celery_task(fetch_stop_search_task, mock_celery_self, "force1")

    mock_celery_self.retry.assert_called()


def test_fetch_stop_search_task_handles_max_retries_exceeded_error(
    mock_db_session, mock_service, mock_celery_self
):
    mock_service.download_stop_search_data = AsyncMock(
        side_effect=Exception("API Error")
    )
    mock_celery_self.request.retries = 5
    mock_celery_self.retry.side_effect = MaxRetriesExceededError()

    result = run_celery_task(fetch_stop_search_task, mock_celery_self, "force1")

    assert result is None
    mock_celery_self.retry.assert_called()


def test_insert_data_task_logs_info_when_merged_file_has_no_data_rows(
    mock_db_session, mock_service, mock_logger, mock_celery_self
):
    results = [("/tmp/test_empty.csv", "/tmp/test_empty_failed.csv")]

    with (
        patch("builtins.open", MagicMock()) as mock_open,
        patch("os.path.exists", return_value=True),
        patch("os.remove"),
    ):
        mock_file = MagicMock()
        mock_file.__enter__.return_value = mock_file
        mock_file.__iter__.return_value = iter(["header"])  # Only header
        mock_open.return_value = mock_file

        run_celery_task(insert_data_task, mock_celery_self, results)

        mock_logger.info.assert_any_call(
            "No rows found in merged file for stop_searches. "
            "Input files might have been empty or missing."
        )


def test_insert_data_task_raises_exception_on_session_creation_failure(
    mock_celery_self,
):
    with patch(
        "app.tasks.stop_search_tasks.SessionLocal", side_effect=Exception("DB Error")
    ):
        with pytest.raises(Exception, match="DB Error"):
            run_celery_task(insert_data_task, mock_celery_self, [])
