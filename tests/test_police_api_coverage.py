from unittest.mock import MagicMock, patch
import pytest
from app.services.police_api import PoliceAPIService
from app.models.stop_search import StopSearch

def test_fetch_and_dump_to_csv_no_dates(db, mocker):
    service = PoliceAPIService(db)
    mocker.patch.object(service, "get_dates_to_process", return_value=[])
    
    result = service.fetch_and_dump_to_csv("leicestershire")
    assert result is None

def test_fetch_and_dump_to_csv_success(db, mocker, tmp_path):
    service = PoliceAPIService(db)
    mocker.patch.object(service, "get_dates_to_process", return_value=["2023-01"])
    
    mock_obj = MagicMock(spec=StopSearch)
    # Mock attributes needed by CSVHandler
    mock_obj.force = "leicestershire"
    # ... (CSVHandler handles attribute access, so we need to ensure they exist if we use real CSVHandler)
    # Or we can mock CSVHandler
    
    mocker.patch.object(service, "fetch_stop_search_data", return_value=([mock_obj], []))
    
    with patch("app.services.police_api.CSVHandler") as MockCSVHandler:
        result = service.fetch_and_dump_to_csv("leicestershire", output_dir=str(tmp_path))
        
        assert result is not None
        valid_path, failed_path = result
        assert "valid_leicestershire.csv" in valid_path
        assert "failed_leicestershire.csv" in failed_path
        
        assert MockCSVHandler.write_valid_objects.called
        assert MockCSVHandler.write_failed_rows.called

def test_bulk_insert_from_csv_exception(db, mocker):
    service = PoliceAPIService(db)
    
    # Mock db connection to raise exception
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.connection.cursor.return_value = mock_cursor
    mock_cursor.copy_expert.side_effect = Exception("DB Error")
    
    service.db.connection = MagicMock(return_value=mock_conn)
    
    with pytest.raises(Exception):
        with patch("builtins.open", new_callable=MagicMock):
            service.bulk_insert_from_csv("dummy.csv")
            
    # Verify rollback called
    assert mock_conn.connection.rollback.called
