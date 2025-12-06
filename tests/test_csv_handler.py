import json
import os
from unittest.mock import MagicMock, patch

import pytest

from app.services.csv_handler import CSVHandler

TEST_COLUMNS = ["col1", "col2"]

def test_write_valid_objects(tmp_path):
    file_path = tmp_path / "test_valid.csv"
    
    class MockObject:
        def __init__(self):
            self.col1 = "val1"
            self.col2 = "val2"

    objects = [MockObject()]
    
    CSVHandler.write_valid_objects(str(file_path), objects, TEST_COLUMNS)
    
    assert os.path.exists(file_path)
    with open(file_path, "r") as f:
        lines = f.readlines()
        assert len(lines) == 2  # Header + 1 row
        assert "col1,col2" in lines[0]
        assert "val1,val2" in lines[1]


def test_write_failed_rows(tmp_path):
    file_path = tmp_path / "test_failed.csv"
    rows = [{"raw_data": {"key": "value"}, "reason": "error"}]
    
    CSVHandler.write_failed_rows(str(file_path), rows)
    
    assert os.path.exists(file_path)
    with open(file_path, "r") as f:
        lines = f.readlines()
        assert len(lines) == 2
        assert "error" in lines[1]


def test_merge_csvs(tmp_path):
    output_path = tmp_path / "merged.csv"
    input1 = tmp_path / "input1.csv"
    input2 = tmp_path / "input2.csv"
    
    # Create input files
    with open(input1, "w") as f:
        f.write("header\nrow1_c1,row1_c2\n")
    with open(input2, "w") as f:
        f.write("header\nrow2_c1,row2_c2\n")
        
    CSVHandler.merge_csvs(str(output_path), [str(input1), str(input2)], TEST_COLUMNS, cleanup=True)
    
    assert os.path.exists(output_path)
    assert not os.path.exists(input1)
    assert not os.path.exists(input2)
    
    with open(output_path, "r") as f:
        content = f.read()
        # Header from merge_csvs + row1 + row2
        assert "col1,col2" in content
        assert "row1_c1,row1_c2" in content
        assert "row2_c1,row2_c2" in content


def test_read_failed_rows(tmp_path):
    file_path = tmp_path / "failed_read.csv"
    
    with open(file_path, "w", newline="") as f:
        f.write("raw_data,reason\n")
        f.write('{"key": "value"},error\n')
        f.write('invalid_json,error\n') # Should be skipped
        
    rows = CSVHandler.read_failed_rows(str(file_path), cleanup=True)
    
    assert len(rows) == 1
    assert rows[0]["reason"] == "error"
    assert rows[0]["raw_data"] == {"key": "value"}
    assert not os.path.exists(file_path)

def test_read_failed_rows_no_file():
    rows = CSVHandler.read_failed_rows("non_existent.csv")
    assert rows == []
