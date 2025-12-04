from unittest.mock import MagicMock, mock_open, patch

import pytest

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

    # Mock _get_latest_datetime to return None so we process everything
    mocker.patch.object(service, "_get_latest_datetime", return_value=None)

    valid_objects, failed_rows = service.fetch_and_process_force(
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

    # Mock _get_latest_datetime
    mocker.patch.object(service, "_get_latest_datetime", return_value=None)

    valid_objects, failed_rows = service._process_data_in_memory("leicestershire", data)

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


def test_fetch_and_process_force_exception(db, mocker):
    service = PoliceAPIService(db)
    mocker.patch.object(service, "_fetch_data", side_effect=Exception("API Error"))

    valid, failed = service.fetch_and_process_force("leicestershire")
    assert valid == []
    assert failed == []


def test_process_data_in_memory_exceptions(db, mocker):
    service = PoliceAPIService(db)
    mocker.patch.object(service, "_get_latest_datetime", return_value=None)

    # Let's test the remediation path specifically
    item = {"bad": "data"}

    # Mock create to fail first
    with patch.object(
        service, "_create_stop_search_object", side_effect=Exception("Fail 1")
    ) as mock_create:
        # Mock clean item
        with patch.object(service, "_clean_item", return_value=item):
             # Mock create to succeed second time
             mock_create.side_effect = [Exception("Fail 1"), MagicMock()]

             valid, failed = service._process_data_in_memory("force", [item])
             assert len(valid) == 1
             assert len(failed) == 0


def test_process_data_in_memory_full_failure(db, mocker):
    service = PoliceAPIService(db)
    mocker.patch.object(service, "_get_latest_datetime", return_value=None)

    item = {"bad": "data"}

    # Fail both times
    with patch.object(
        service, "_create_stop_search_object", side_effect=Exception("Fail")
    ):
        valid, failed = service._process_data_in_memory("force", [item])
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

    service._fetch_data("force", date="2023-01")

    mock_get.assert_called_with(
        "https://data.police.uk/api/stops-force",
        params={"force": "force", "date": "2023-01"},
        timeout=30,
    )


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
