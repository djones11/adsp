import os
from unittest.mock import MagicMock, mock_open, patch

import pytest

from app.models.failed_row import FailedRow
from app.models.stop_search import StopSearch
from app.services.csv_handler import CSVHandler

TEST_COLUMNS = ["col1", "col2"]


@pytest.fixture
def mock_db():
    return MagicMock()


def test_write_rows_objects(tmp_path):
    file_path = tmp_path / "test_rows_obj.csv"

    class MockObject:
        def __init__(self, c1, c2):
            self.col1 = c1
            self.col2 = c2

    objects = [MockObject("val1", "val2"), MockObject("val3", "val4")]

    CSVHandler.write_rows(str(file_path), objects, TEST_COLUMNS)

    assert os.path.exists(file_path)

    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

        assert len(lines) == 3  # Header + 2 rows
        assert "col1,col2" in lines[0]
        assert "val1,val2" in lines[1]
        assert "val3,val4" in lines[2]


def test_write_rows_dicts(tmp_path):
    file_path = tmp_path / "test_rows_dict.csv"

    rows = [{"col1": "val1", "col2": "val2"}, {"col1": "val3", "col2": "val4"}]

    CSVHandler.write_rows(str(file_path), rows, TEST_COLUMNS)

    assert os.path.exists(file_path)

    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

        assert len(lines) == 3
        assert "col1,col2" in lines[0]
        assert "val1,val2" in lines[1]
        assert "val3,val4" in lines[2]


def test_merge_csvs(tmp_path):
    output_path = tmp_path / "merged.csv"
    input1 = tmp_path / "input1.csv"
    input2 = tmp_path / "input2.csv"

    # Create input files
    with open(input1, "w", newline="", encoding="utf-8") as f:
        f.write("col1,col2\nrow1_c1,row1_c2\n")

    with open(input2, "w", newline="", encoding="utf-8") as f:
        f.write("col1,col2\nrow2_c1,row2_c2\n")

    CSVHandler.merge_csvs(
        str(output_path), [str(input1), str(input2)], TEST_COLUMNS, cleanup=True
    )

    assert os.path.exists(output_path)
    assert not os.path.exists(input1)
    assert not os.path.exists(input2)

    with open(output_path, "r", encoding="utf-8") as f:
        content = f.read()

        assert "col1,col2" in content
        assert "row1_c1,row1_c2" in content
        assert "row2_c1,row2_c2" in content


def test_read_rows(tmp_path):
    file_path = tmp_path / "test_read.csv"

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        f.write("col1,col2\nval1,val2\n")

    rows = CSVHandler.read_rows(str(file_path))

    assert len(rows) == 1
    assert rows[0] == ["val1", "val2"]


def test_read_rows_no_file():
    rows = CSVHandler.read_rows("non_existent.csv")
    assert rows == []


def test_bulk_insert_from_csv(mocker):
    # Mock DB session
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    # Setup connection chain
    mock_db.connection.return_value.connection = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    with patch("builtins.open", mock_open(read_data="header\nrow1")):
        with patch("os.path.exists", return_value=True):
            CSVHandler.bulk_insert_from_csv(
                mock_db, "dummy.csv", TEST_COLUMNS, StopSearch.__tablename__
            )

    mock_cursor.copy_expert.assert_called_once()
    args, _ = mock_cursor.copy_expert.call_args

    assert "COPY stop_searches" in args[0]

    # Verify savepoints are used
    mock_cursor.execute.assert_any_call("SAVEPOINT full_copy_savepoint")
    mock_cursor.execute.assert_any_call("RELEASE SAVEPOINT full_copy_savepoint")

    mock_db.commit.assert_called_once()


def test_bulk_insert_from_csv_exception(mocker):
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_db.connection.return_value.connection = mock_conn

    # If open raises exception, it should be caught and re-raised
    with patch("builtins.open", side_effect=Exception("File Error")):
        with pytest.raises(Exception, match="File Error"):
            with patch("os.path.exists", return_value=True):
                CSVHandler.bulk_insert_from_csv(
                    mock_db, "dummy.csv", TEST_COLUMNS, StopSearch.__tablename__
                )

    mock_db.rollback.assert_called_once()


def test_handle_failed_row():
    mock_db = MagicMock()
    row_line = "val1,val2"
    header = "col1,col2\n"
    error_msg = "Error"
    table_name = "stop_searches"

    CSVHandler._handle_failed_row(mock_db, row_line, header, error_msg, table_name)

    mock_db.add.assert_called_once()
    call_args = mock_db.add.call_args[0][0]

    assert isinstance(call_args, FailedRow)
    assert call_args.source == "stop_searches"
    assert call_args.reason == "Error"
    assert call_args.raw_data == {"col1": "val1", "col2": "val2"}

def test_bulk_insert_file_not_found(mock_db):
    with patch("os.path.exists", return_value=False):
        CSVHandler.bulk_insert_from_csv(mock_db, "nonexistent.csv", [], "table")
        # Should just return, no DB calls
        mock_db.commit.assert_not_called()


def test_bulk_insert_empty_file(mock_db):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_db.connection.return_value.connection = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    # Simulate full copy failure for empty file
    mock_cursor.copy_expert.side_effect = Exception("Empty file")

    with patch("os.path.exists", return_value=True):
        with patch("builtins.open", new_callable=MagicMock) as mock_open:
            # Mock readline to return empty string (EOF immediately)
            mock_open.return_value.__enter__.return_value.readline.return_value = ""

            CSVHandler.bulk_insert_from_csv(mock_db, "empty.csv", [], "table")

            # Should not commit because fallback also returns early
            mock_db.commit.assert_not_called()


def test_bulk_insert_batching(mock_db):
    # Create 1005 rows + header
    lines = ["header\n"] + [f"row{i}\n" for i in range(1005)]

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_db.connection.return_value.connection = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    # Simulate full copy failure to trigger batching
    mock_cursor.copy_expert.side_effect = Exception("Copy failed")

    with patch("os.path.exists", return_value=True):
        with patch("builtins.open", new_callable=MagicMock) as mock_open:
            mock_file = mock_open.return_value.__enter__.return_value
            mock_file.readline.return_value = lines[0]
            mock_file.__iter__.return_value = iter(lines[1:])

            # Mock _insert_batch to verify it's called multiple times
            with patch.object(CSVHandler, "_insert_batch") as mock_insert_batch:
                CSVHandler.bulk_insert_from_csv(mock_db, "large.csv", ["col"], "table")

                # Should be called twice: once for first 1000, once for remaining 5
                assert mock_insert_batch.call_count == 2
                # Verify batch sizes
                args1, _ = mock_insert_batch.call_args_list[0]

                assert len(args1[1]) == 1000

                args2, _ = mock_insert_batch.call_args_list[1]

                assert len(args2[1]) == 5


def test_insert_batch_adaptive_splitting(mock_db):
    # Mock DB cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_db.connection.return_value.connection = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    rows = [f"row{i}\n" for i in range(10)]
    header = "col\n"
    columns_str = "col"
    table_name = "table"

    # Make copy_expert fail for the full batch, but succeed for sub-batches
    # The first call is with 10 rows -> Fail
    # Then it splits. 10 // 10 = 1. Chunk size 1.
    # So it will call _insert_batch 10 times with 1 row each.

    def side_effect(sql, f):
        # Check content of f to determine if it's the full batch or single row
        content = f.getvalue()

        if len(content.splitlines()) > 2:  # header + more than 1 row
            raise Exception("Batch failed")
        
        return None

    mock_cursor.copy_expert.side_effect = side_effect

    # Mock _handle_failed_row to ensure it's NOT called if sub-batches succeed
    with patch.object(CSVHandler, "_handle_failed_row") as mock_handle_failed:
        CSVHandler._insert_batch(mock_db, rows, header, columns_str, table_name)

        # Should have called copy_expert 1 (fail) + 10 (success) = 11 times
        assert mock_cursor.copy_expert.call_count == 11

        mock_handle_failed.assert_not_called()


def test_insert_batch_single_row_failure(mock_db):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_db.connection.return_value.connection = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    rows = ["bad_row"]
    header = "col\n"

    mock_cursor.copy_expert.side_effect = Exception("Copy failed")

    with patch.object(CSVHandler, "_handle_failed_row") as mock_handle_failed:
        CSVHandler._insert_batch(mock_db, rows, header, "col", "table")

        mock_handle_failed.assert_called_once_with(
            mock_db, "bad_row", header, "Copy failed", "table"
        )


def test_handle_failed_row_stop_search(mock_db):
    row_line = "val1,val2"
    header = "col1,col2\n"
    error_msg = "Error"
    table_name = StopSearch.__tablename__

    CSVHandler._handle_failed_row(mock_db, row_line, header, error_msg, table_name)

    # Should add a FailedRow
    mock_db.add.assert_called_once()
    call_args = mock_db.add.call_args[0][0]

    assert isinstance(call_args, FailedRow)
    assert call_args.reason == error_msg
    assert call_args.raw_data == {"col1": "val1", "col2": "val2"}


def test_handle_failed_row_other_table(mock_db):
    row_line = "val1"
    header = "col1\n"
    error_msg = "Error"
    table_name = "other_table"

    with patch("app.services.csv_handler.logger") as mock_logger:
        CSVHandler._handle_failed_row(mock_db, row_line, header, error_msg, table_name)

        mock_db.add.assert_not_called()
        mock_logger.error.assert_called_with(
            f"Failed to insert row into {table_name}: {error_msg}"
        )


def test_handle_failed_row_parsing_error(mock_db):
    # Malformed CSV line that causes DictReader to fail
    row_line = "val1"
    header = "col1\n"
    table_name = StopSearch.__tablename__

    with patch("csv.DictReader", side_effect=Exception("Parse error")):
        with patch("app.services.csv_handler.logger") as mock_logger:
            CSVHandler._handle_failed_row(mock_db, row_line, header, "err", table_name)

            mock_logger.error.assert_called()
            assert "Failed to process failed row" in mock_logger.error.call_args[0][0]


def test_insert_batch_rollback_failure(mock_db):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_db.connection.return_value.connection = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    rows = ["row1"]
    header = "col\n"

    # First copy_expert fails
    mock_cursor.copy_expert.side_effect = Exception("Copy failed")

    # Then rollback fails
    def execute_side_effect(sql):
        if "ROLLBACK" in sql:
            raise Exception("Rollback failed")
        
        return None

    mock_cursor.execute.side_effect = execute_side_effect

    with patch.object(CSVHandler, "_handle_failed_row") as mock_handle_failed:
        # Should not raise exception, just pass
        CSVHandler._insert_batch(mock_db, rows, header, "col", "table")

        mock_handle_failed.assert_called()
