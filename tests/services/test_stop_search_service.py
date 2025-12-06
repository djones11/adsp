import asyncio
import pytest

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from app.services.stop_search_service import PoliceStopSearchService


def test_fetch_and_process_force(db, mocker):
    async def run_test():
        mocker.patch(
            "app.services.stop_search_service.make_request_async",
            return_value=[ # 1 valid, 1 invalid
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
            ],
        )

        service = PoliceStopSearchService(db)

        mocker.patch("os.getenv", return_value='["leicestershire"]')
        mocker.patch.object(service, "_get_latest_datetime", return_value=None)

        mock_client = MagicMock()

        valid_objects, failed_rows = await service._fetch_stop_search_data(
            "leicestershire", date="2024-01", client=mock_client
        )

        # Check valid objects
        assert len(valid_objects) == 1

        stop_search = valid_objects[0]

        assert stop_search["force"] == "leicestershire"
        assert stop_search["age_range"] == "18-24"
        assert stop_search["outcome_object_id"] == "bu-no-further-action"
        assert stop_search["street_name"] == "On or near Crescent Street"

        # Check failed rows
        assert len(failed_rows) == 1

        failed_row = failed_rows[0]

        assert failed_row["raw_data"]["datetime"] == "invalid-date-format"

    asyncio.run(run_test())


def test_process_data_in_memory_with_remediation(db, mocker):
    # Sample data with a row that needs remediation (empty string for boolean)
    data = [
        {
            "age_range": "18-24",
            "involved_person": "",  # Should be remediated to True
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

    service = PoliceStopSearchService(db)

    mocker.patch.object(service, "_get_latest_datetime", return_value=None)

    valid_objects, failed_rows = service._process_stop_search_data(
        "leicestershire", data
    )

    # Check if valid row was returned
    assert len(valid_objects) == 1

    stop_search = valid_objects[0]

    assert stop_search["involved_person"] is True

    # Check that no failed rows were returned
    assert len(failed_rows) == 0


def test_fetch_and_process_force_exception(db, mocker):
    async def run_test():
        service = PoliceStopSearchService(db)
        mocker.patch(
            "app.services.stop_search_service.make_request_async",
            side_effect=Exception("API Error"),
        )

        mock_client = MagicMock()
        valid, failed = await service._fetch_stop_search_data(
            "leicestershire", "2023-01", client=mock_client
        )

        assert valid == []
        assert failed == []

    asyncio.run(run_test())


@pytest.mark.parametrize(
    "create_side_effect, expected_valid, expected_failed",
    [
        (MagicMock(), 1, 0),
        (Exception("Fail"), 0, 1),
    ],
    ids=["success", "failure"],
)
def test_process_data_in_memory_failures(
    db, mocker, create_side_effect, expected_valid, expected_failed
):
    service = PoliceStopSearchService(db)
    mocker.patch.object(service, "_get_latest_datetime", return_value=None)

    item = {"bad": "data"}

    # Mock clean item
    with patch("app.services.stop_search_service.DataCleaner.clean", return_value=item):
        # Mock create_stop_search_dict behavior
        with patch.object(
            service, "_create_stop_search_dict", side_effect=create_side_effect
        ):
            valid, failed = service._process_stop_search_data("norfolk", [item])

            assert len(valid) == expected_valid
            assert len(failed) == expected_failed

def test_fetch_data_with_date(db, mocker):
    async def run_test():
        service = PoliceStopSearchService(db)

        mock_make_request = mocker.patch("app.services.stop_search_service.make_request_async")
        mock_make_request.return_value = []

        mock_client = MagicMock()

        await service._fetch_stop_search_data(
            "norfolk", date="2023-01", client=mock_client
        )

        mock_make_request.assert_called_once()
        call_args = mock_make_request.call_args

        assert call_args[0][0] == "https://data.police.uk/api/stops-force"
        assert call_args[0][1] == {"force": "norfolk", "date": "2023-01"}

    asyncio.run(run_test())

def test_fetch_and_process_force_no_data(db, mocker):
    async def run_test():
        service = PoliceStopSearchService(db)
        mocker.patch(
            "app.services.stop_search_service.make_request_async", return_value=[]
        )

        mock_client = MagicMock()
        valid, failed = await service._fetch_stop_search_data(
            "norfolk", "2023-01", client=mock_client
        )
        assert valid == []
        assert failed == []

    asyncio.run(run_test())

@pytest.mark.parametrize(
    "location_data, expected_lat, expected_street_id",
    [
        (None, None, None),
        ({"latitude": "1.0", "longitude": "1.0", "street": None}, "1.0", None),
        (
            {
                "latitude": "1.0",
                "longitude": "1.0",
                "street": {"id": 123, "name": "Test St"},
            },
            "1.0",
            123,
        ),
    ],
    ids=["no_location", "no_street", "with_street"]
)
def test_create_stop_search_dict_location_scenarios(
    db, location_data, expected_lat, expected_street_id
):
    service = PoliceStopSearchService(db)

    item = {
        "type": "Person search",
        "involved_person": True,
        "datetime": "2023-01-01T12:00:00+00:00",
        "operation": False,
        "operation_name": None,
        "location": location_data,
        "gender": "Male",
        "age_range": "18-24",
        "self_defined_ethnicity": "White",
        "officer_defined_ethnicity": "White",
        "legislation": "PACE",
        "object_of_search": "Drugs",
        "outcome": "Arrest",
        "outcome_linked_to_object_of_search": True,
        "removal_of_more_than_outer_clothing": False,
        "outcome_object": None,
    }

    obj = service._create_stop_search_dict(item, "suffolk")

    assert obj["latitude"] == expected_lat
    assert obj["street_id"] == expected_street_id


def test_get_dates_to_process_no_dates(db, mocker):
    service = PoliceStopSearchService(db)
    mocker.patch.object(service, "_get_available_dates", return_value={})

    dates = service._get_dates_to_process("leicestershire")
    assert dates == []

def test_download_stop_search_data_no_dates(db, mocker):
    async def run_test():
        service = PoliceStopSearchService(db)
        mocker.patch.object(service, "_get_dates_to_process", return_value=[])

        result = await service.download_stop_search_data("leicestershire")
        assert result is None

    asyncio.run(run_test())

def test_download_stop_search_data_success(db, mocker, tmp_path):
    async def run_test():
        service = PoliceStopSearchService(db)

        mocker.patch.object(service, "_get_dates_to_process", return_value=["2023-03"])

        mock_obj = {"force": "leicestershire"}

        mocker.patch.object(
            service,
            "_fetch_stop_search_data",
            new_callable=AsyncMock,
            return_value=([mock_obj], [{"raw": "data"}]),
        )

        with patch("app.services.stop_search_service.CSVHandler") as MockCSVHandler:
            result = await service.download_stop_search_data(
                "leicestershire", output_dir=str(tmp_path)
            )

            assert result is not None
            
            valid_path, failed_path = result

            assert "valid_leicestershire.csv" in valid_path
            assert "failed_leicestershire.csv" in failed_path

            assert MockCSVHandler.write_rows.call_count == 2

    asyncio.run(run_test())