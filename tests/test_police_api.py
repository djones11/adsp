from datetime import datetime
from unittest.mock import MagicMock, mock_open, patch

import pytest
import requests

from app.services.police_api import PoliceAPIService

# Sample data based on the user provided example
SAMPLE_API_RESPONSE = [
    {
        "age_range": "18-24",
        "officer_defined_ethnicity": None,
        "involved_person": True,
        "self_defined_ethnicity": "Other ethnic group - Not stated",
        "gender": "Male",
        "legislation": None,
        "outcome_linked_to_object_of_search": None,
        "datetime": "2024-01-06T22:45:00+00:00",
        "outcome_object": {
            "id": "bu-no-further-action",
            "name": "A no further action disposal",
        },
        "location": {
            "latitude": "52.628997",
            "street": {"id": 1738518, "name": "On or near Crescent Street"},
            "longitude": "-1.130273",
        },
        "object_of_search": "Controlled drugs",
        "operation": None,
        "outcome": "A no further action disposal",
        "type": "Person and Vehicle search",
        "operation_name": None,
        "removal_of_more_than_outer_clothing": False,
    },
    {"datetime": "invalid-date-format"},
]


def test_fetch_and_process_force(db, mocker):
    # Mock requests.get
    mock_response = MagicMock()
    mock_response.json.return_value = SAMPLE_API_RESPONSE
    mock_response.raise_for_status.return_value = None

    mocker.patch("requests.get", return_value=mock_response)

    service = PoliceAPIService(db)

    # We need to mock os.getenv to ensure we test a known force
    mocker.patch("os.getenv", return_value='["leicestershire"]')

    # Mock get_latest_datetime to return None so we process everything
    mocker.patch.object(service, "get_latest_datetime", return_value=None)

    valid_objects, failed_rows = service.fetch_stop_search_data(
        "leicestershire", date="2024-01"
    )

    # Check valid objects
    assert len(valid_objects) == 1
    stop_search = valid_objects[0]
    assert stop_search.force == "leicestershire"
    assert stop_search.age_range == "18-24"
    assert stop_search.outcome_object_id == "bu-no-further-action"
    assert stop_search.street_name == "On or near Crescent Street"

    # Check failed rows
    assert len(failed_rows) == 1
    failed_row = failed_rows[0]
    assert failed_row["raw_data"]["datetime"] == "invalid-date-format"


def test_process_data_in_memory_with_remediation(db, mocker):
    # Sample data with a row that needs remediation (empty string for boolean)
    data = [
        {
            "age_range": "18-24",
            "involved_person": "",  # Should be remediated to None
            "datetime": "2024-01-06T22:45:00+00:00",
            "type": "Person search",
            "gender": "Male",
            "object_of_search": "Controlled drugs",
            "outcome": "A no further action disposal",
            "legislation": "Misuse of Drugs Act 1971 (section 23)",
            "removal_of_more_than_outer_clothing": False,
            "operation": False,
            "officer_defined_ethnicity": "White",
            "self_defined_ethnicity": "White - English/Welsh/Scottish/"
            "Northern Irish/British",
            "outcome_linked_to_object_of_search": False,
            "location": None,
            "outcome_object": None,
            "operation_name": None,
        }
    ]

    service = PoliceAPIService(db)

    # Mock get_latest_datetime
    mocker.patch.object(service, "get_latest_datetime", return_value=None)

    valid_objects, failed_rows = service._process_stop_search_data("leicestershire", data)

    # Check if valid row was returned
    assert len(valid_objects) == 1
    stop_search = valid_objects[0]
    assert stop_search.involved_person is None

    # Check that no failed rows were returned
    assert len(failed_rows) == 0


def test_bulk_insert_from_csv(db, mocker):
    service = PoliceAPIService(db)

    # Mock the DB connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    # We need to mock the chain: db.connection().connection.cursor()
    # db is a Session, db.connection() returns a Connection, .connection returns
    # the raw DBAPI connection

    # Mocking the session's connection method
    mock_sa_conn = MagicMock()
    mocker.patch.object(service.db, "connection", return_value=mock_sa_conn)
    mock_sa_conn.connection = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    # Mock open to avoid file system errors
    mocker.patch("builtins.open", mock_open(read_data="header\nrow1"))

    service.bulk_insert_from_csv("/tmp/test.csv")

    # Verify copy_expert was called
    mock_cursor.copy_expert.assert_called_once()
    args, _ = mock_cursor.copy_expert.call_args
    assert "COPY stop_searches" in args[0]
    assert "FROM STDIN" in args[0]

    # Verify commit was called on the raw connection
    mock_conn.commit.assert_called_once()


def test_fetch_and_process_force_exception(db, mocker):
    service = PoliceAPIService(db)
    mocker.patch.object(service, "_fetch_stop_search_data", side_effect=Exception("API Error"))

    valid, failed = service.fetch_stop_search_data("leicestershire", "2023-01")
    assert valid == []
    assert failed == []


def test_process_data_in_memory_exceptions(db, mocker):
    service = PoliceAPIService(db)
    mocker.patch.object(service, "get_latest_datetime", return_value=None)

    # Let's test the remediation path specifically
    item = {"bad": "data"}

    # Mock create to fail first
    with patch.object(
        service, "_create_stop_search_object", side_effect=Exception("Fail 1")
    ) as mock_create:
        # Mock clean item
        with patch("app.services.police_api.DataCleaner.clean", return_value=item):
            # Mock create to succeed second time
            mock_create.side_effect = [Exception("Fail 1"), MagicMock()]

            valid, failed = service._process_stop_search_data("force", [item])
            assert len(valid) == 1
            assert len(failed) == 0
def test_process_data_in_memory_full_failure(db, mocker):
    service = PoliceAPIService(db)
    mocker.patch.object(service, "get_latest_datetime", return_value=None)

    item = {"bad": "data"}

    # Fail both times
    with patch.object(
        service, "_create_stop_search_object", side_effect=Exception("Fail")
    ):
        valid, failed = service._process_stop_search_data("force", [item])
        assert len(valid) == 0
        assert len(failed) == 1


def test_bulk_insert_from_csv_exception(db, mocker):
    service = PoliceAPIService(db)
    mocker.patch.object(service.db, "connection", side_effect=Exception("DB Error"))

    with pytest.raises(Exception):
        service.bulk_insert_from_csv("file.csv")


def test_fetch_data_with_date(db, mocker):
    service = PoliceAPIService(db)
    mock_get = mocker.patch("requests.get")
    mock_get.return_value.json.return_value = []

    service._fetch_stop_search_data("force", date="2023-01")


def test_get_available_dates(db, mocker):
    service = PoliceAPIService(db)
    mock_response = [
        {"date": "2024-01", "stop-and-search": ["leicestershire", "metropolitan"]},
        {"date": "2024-02", "stop-and-search": ["leicestershire"]},
    ]

    mocker.patch("app.services.police_api.make_request", return_value=mock_response)

    availability = service.get_available_dates()

    assert "leicestershire" in availability
    assert "metropolitan" in availability
    assert availability["leicestershire"] == ["2024-01", "2024-02"]
    assert availability["metropolitan"] == ["2024-01"]


def test_get_available_dates_error(db, mocker):
    service = PoliceAPIService(db)
    mocker.patch("app.services.police_api.make_request", side_effect=Exception("API Error"))

    availability = service.get_available_dates()
    assert availability == {}





def test_fetch_and_process_force_no_data(db, mocker):
    service = PoliceAPIService(db)
    mocker.patch.object(service, "_fetch_stop_search_data", return_value=[])

    valid, failed = service.fetch_stop_search_data("force", "2023-01")
    assert valid == []
    assert failed == []


def test_process_data_in_memory_skip_old(db, mocker):
    # This test is no longer relevant as filtering happens before processing
    pass


def test_process_data_in_memory_remediation_skip_old(db, mocker):
    # This test is no longer relevant as filtering happens before processing
    pass


def test_remediate_failed_rows_json_error(mocker):
    mock_db = MagicMock()
    service = PoliceAPIService(mock_db)

    row = MagicMock()
    row.raw_data = "invalid json"
    row.id = 1

    mock_db.query.return_value.all.return_value = [row]

    service.remediate_failed_rows()

    # Should log warning and continue
    assert not mock_db.add.called


def test_remediate_failed_rows_exception(mocker):
    mock_db = MagicMock()
    service = PoliceAPIService(mock_db)

    row = MagicMock()
    row.raw_data = {"data": "ok"}
    row.id = 1

    mock_db.query.return_value.all.return_value = [row]

    # Mock create to raise exception
    mocker.patch.object(
        service, "_create_stop_search_object", side_effect=Exception("Error")
    )

    service.remediate_failed_rows()

    # Should catch exception
    assert not mock_db.delete.called


def test_remediate_failed_rows(mocker):
    mock_db = MagicMock()
    service = PoliceAPIService(mock_db)

    # Mock failed rows
    row1 = MagicMock()
    row1.raw_data = '{"some": "json"}'
    row1.id = 1

    row2 = MagicMock()
    row2.raw_data = {"already": "dict"}
    row2.id = 2

    row3 = MagicMock()
    row3.raw_data = "invalid json"
    row3.id = 3

    mock_db.query.return_value.all.return_value = [row1, row2, row3]

    # Mock create object
    mocker.patch.object(service, "_create_stop_search_object", return_value=MagicMock())

    service.remediate_failed_rows()

    # Should have deleted row1 and row2
    assert mock_db.delete.call_count == 2
    assert mock_db.commit.called


# --- Tests moved from test_coverage_gaps.py ---

def test_bulk_insert_from_csv_copy_expert_exception(db):
    """Test exception handling and rollback in bulk_insert_from_csv."""
    # We use the db fixture but we need to mock the connection/cursor to simulate DB error
    # The service uses self.db.connection().connection.cursor()
    
    # Create a mock session that mimics the interface needed
    mock_db_session = MagicMock()
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


def test_get_available_dates_missing_date(db):
    """Test get_available_dates with an entry missing the date field."""
    service = PoliceAPIService(db)

    mock_response = [
        {"date": "2023-01", "stop-and-search": ["force1"]},
        {"stop-and-search": ["force2"]},  # Missing date
    ]

    with patch("app.services.police_api.make_request", return_value=mock_response):
        availability = service.get_available_dates()

    assert "force1" in availability
    assert "force2" not in availability


def testget_latest_date_real(db):
    """Test get_latest_date without mocking the private method."""
    # We mock the DB query chain on the passed db session
    mock_db_session = MagicMock()
    service = PoliceAPIService(mock_db_session)

    # self.db.query(...).filter(...).scalar()
    mock_query = mock_db_session.query.return_value
    mock_filter = mock_query.filter.return_value
    mock_filter.scalar.return_value = datetime(2023, 1, 1)

    result = service.get_latest_datetime("force1")

    assert result == datetime(2023, 1, 1)
    mock_db_session.query.assert_called()


def test_create_stop_search_object_missing_fields(db):
    """Test _create_stop_search_object with missing optional fields."""
    service = PoliceAPIService(db)

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


def test_get_dates_to_process_no_dates(db, mocker):
    service = PoliceAPIService(db)
    mocker.patch.object(service, "get_available_dates", return_value={})
    
    dates = service.get_dates_to_process("leicestershire")
    assert dates == []


def test_get_dates_to_process_filtering(db, mocker):
    service = PoliceAPIService(db)
    
    # Mock available dates
    mocker.patch.object(service, "get_available_dates", return_value={
        "leicestershire": ["2023-01", "2023-02", "2023-03"]
    })
    
    # Mock latest date in DB
    mock_datetime = datetime(2023, 2, 1)
    mocker.patch.object(service, "get_latest_datetime", return_value=mock_datetime)
    
    dates = service.get_dates_to_process("leicestershire")
    
    # Should only return 2023-03, as 2023-01 and 2023-02 are <= 2023-02
    assert dates == ["2023-03"]


def test_fetch_and_dump_to_csv_no_dates(db, mocker):
    service = PoliceAPIService(db)
    mocker.patch.object(service, "get_dates_to_process", return_value=[])
    
    result = service.fetch_and_dump_to_csv("leicestershire")
    assert result is None


def test_fetch_and_dump_to_csv_success(db, mocker, tmp_path):
    service = PoliceAPIService(db)
    
    # Mock dates to process
    mocker.patch.object(service, "get_dates_to_process", return_value=["2023-03"])
    
    # Mock fetch_stop_search_data
    mock_obj = MagicMock()
    mock_obj.force = "leicestershire"
    # Add other required attributes for CSV writing if needed, or mock CSVHandler
    
    mocker.patch.object(service, "fetch_stop_search_data", return_value=([mock_obj], [{"raw": "data"}]))
    
    # Mock CSVHandler to avoid actual file writing or to verify calls
    # But we can also let it write to tmp_path
    
    # We need to patch CSVHandler used in police_api.py
    with patch("app.services.police_api.CSVHandler") as MockCSVHandler:
        result = service.fetch_and_dump_to_csv("leicestershire", output_dir=str(tmp_path))
        
        assert result is not None
        valid_path, failed_path = result
        
        assert "valid_leicestershire.csv" in valid_path
        assert "failed_leicestershire.csv" in failed_path
        
        MockCSVHandler.write_valid_objects.assert_called_once()
        MockCSVHandler.write_failed_rows.assert_called_once()


