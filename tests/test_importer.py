from unittest.mock import MagicMock, mock_open, patch

import pytest

from app.services.importer import DataImporter


@pytest.fixture
def mock_db():
    return MagicMock()


@pytest.fixture
def importer(mock_db):
    return DataImporter(mock_db)


def test_process_file(importer, mock_db):
    csv_content = "name,description,value\nItem1,Desc1,10\nItem2,Desc2,20"
    
    with patch("builtins.open", mock_open(read_data=csv_content)):
        with patch("app.services.importer.engine") as mock_engine:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_engine.raw_connection.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            
            importer.process_file("dummy.csv")
            
            # Verify bulk insert was called
            assert mock_cursor.copy_expert.called
            assert mock_conn.commit.called


def test_process_file_with_invalid_rows(importer, mock_db):
    csv_content = "name,description,value\nItem1,Desc1,10\nItem2,Desc2,invalid"
    
    with patch("builtins.open", mock_open(read_data=csv_content)):
        with patch("app.services.importer.engine") as mock_engine:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_engine.raw_connection.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            
            importer.process_file("dummy.csv")
            
            # Verify valid row inserted
            assert mock_cursor.copy_expert.called
            
            # Verify failed row inserted
            assert mock_db.bulk_insert_mappings.called
            args, _ = mock_db.bulk_insert_mappings.call_args
            assert len(args[1]) == 1
            assert args[1][0]["raw_data"]["name"] == "Item2"


def test_bulk_insert_copy_exception(importer):
    data = [{"name": "Item1", "description": "Desc1", "value": 10}]
    
    with patch("app.services.importer.engine") as mock_engine:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_engine.raw_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        mock_cursor.copy_expert.side_effect = Exception("DB Error")
        
        importer._bulk_insert_copy(data)
        
        assert mock_conn.rollback.called


def test_insert_failed_rows_exception(importer, mock_db):
    failures = [{"raw_data": {}, "reason": "error"}]
    mock_db.bulk_insert_mappings.side_effect = Exception("DB Error")
    
    importer._insert_failed_rows(failures)
    
    assert mock_db.rollback.called
