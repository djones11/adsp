import csv
from unittest.mock import MagicMock, patch

from celery.exceptions import MaxRetriesExceededError

from app.tasks.populate_stop_searches import (
    fetch_force_task,
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

    # Mock return value of fetch_and_process_force
    mock_obj = MagicMock()
    mock_obj.force = "leicestershire"
    mock_obj.type = "Person search"
    
    mock_service.fetch_and_process_force.return_value = (
        [mock_obj],
        [{"raw_data": {}, "reason": "error"}],
    )

    with patch("builtins.open", new_callable=MagicMock):
        # Call the task directly (bypassing Celery binding for simple success case if possible, 
        # but since it uses bind=True, we might need to mock self if we called .run, 
        # but calling the decorated function directly in recent Celery versions works if we don't use self.
        # However, the code uses self.request.retries in exception block.
        # In success path, self is not used.
        result = fetch_force_task("leicestershire")  # type: ignore
        
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
    mock_service.fetch_and_process_force.side_effect = Exception("API Error")

    # We need to mock the task instance 'self' to handle retry
    # When calling the decorated task directly, Celery 4+ doesn't pass self automatically 
    # unless we use .apply(), but that runs the whole chain.
    # A common way to test bound tasks is to mock the 'self' argument by calling the underlying function.
    # But accessing the underlying function of a decorated task can be tricky (task.run usually).
    
    # Let's use patch.object on the task's retry method.
    with patch.object(fetch_force_task, "retry", side_effect=MaxRetriesExceededError):
         result = fetch_force_task("leicestershire") # type: ignore
         assert result is None


@patch("app.tasks.populate_stop_searches.PoliceAPIService")
@patch("app.tasks.populate_stop_searches.SessionLocal")
def test_insert_data_task(mock_session_cls, mock_service_cls):
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    mock_service = MagicMock()
    mock_service_cls.return_value = mock_service

    results = [("/tmp/valid_1.csv", "/tmp/failed_1.csv"), None] # Include a None result to test filtering

    # Mock file operations
    with patch("os.path.exists", return_value=True):
        with patch("os.path.getsize", return_value=100):
            with patch("builtins.open", new_callable=MagicMock) as mock_open:
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
                            iter([["header"], ["row1_col1", "row1_col2"]]), # First call for valid_1
                            iter([["header"], ['{"json": "data"}', "reason"]]) # Second call for failed_1
                        ]
                        
                        insert_data_task(results)  # type: ignore

                        assert mock_service.bulk_insert_from_csv.called
                        # Check if failed objects were saved
                        assert mock_session.bulk_save_objects.called
                        assert mock_service.remediate_failed_rows.called


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
                            insert_data_task(results) # type: ignore
                        except Exception:
                            pass # Expected
                        
                        assert mock_service.bulk_insert_from_csv.called

