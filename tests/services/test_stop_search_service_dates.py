from datetime import datetime
from unittest.mock import MagicMock, patch

from app.services.stop_search_service import PoliceStopSearchService
import pytest


def test_get_dates_to_process(db, mocker):
    service = PoliceStopSearchService(db)

    # Mock _get_available_dates
    mocker.patch.object(
        service,
        "_get_available_dates",
        return_value={"leicestershire": ["2023-01", "2023-02", "2023-03"]},
    )

    # Mock _get_latest_datetime
    mocker.patch.object(service, "_get_latest_datetime", return_value=datetime(2023, 1, 15))

    # Test with no target date
    dates = service._get_dates_to_process("leicestershire")

    assert dates == ["2023-02", "2023-03"]


def test_get_dates_to_process_no_availability(db, mocker):
    service = PoliceStopSearchService(db)
    mocker.patch.object(service, "_get_available_dates", return_value={})

    dates = service._get_dates_to_process("leicestershire")

    assert dates == []


def test_get_dates_to_process_no_latest_date(db, mocker):
    service = PoliceStopSearchService(db)

    mocker.patch.object(
        service,
        "_get_available_dates",
        return_value={"leicestershire": ["2023-01", "2023-02"]},
    )
    mocker.patch.object(service, "_get_latest_datetime", return_value=None)

    dates = service._get_dates_to_process("leicestershire")

    assert dates == ["2023-01", "2023-02"]

def test_get_available_dates(db, mocker):
    service = PoliceStopSearchService(db)
    mock_response = [
        {"date": "2024-01", "stop-and-search": ["leicestershire", "metropolitan"]},
        {"date": "2024-02", "stop-and-search": ["leicestershire"]},
    ]

    mocker.patch(
        "app.services.stop_search_service.make_request", return_value=mock_response
    )

    availability = service._get_available_dates()

    assert "leicestershire" in availability
    assert "metropolitan" in availability
    assert availability["leicestershire"] == ["2024-01", "2024-02"]
    assert availability["metropolitan"] == ["2024-01"]


def test_get_available_dates_error(db, mocker):
    service = PoliceStopSearchService(db)
    mocker.patch(
        "app.services.stop_search_service.make_request",
        side_effect=Exception("API Error"),
    )

    availability = service._get_available_dates()
    assert availability == {}

def test_get_available_dates_missing_date(db):
    """Test _get_available_dates with an entry missing the date field."""
    service = PoliceStopSearchService(db)

    mock_response = [
        {"date": "2023-01", "stop-and-search": ["suffolk"]},
        {"stop-and-search": ["essex"]},  # Missing date
    ]

    with patch(
        "app.services.stop_search_service.make_request", return_value=mock_response
    ):
        availability = service._get_available_dates()

    assert "suffolk" in availability
    assert "essex" not in availability

def test_get_latest_date_real(db):
    """Test get_latest_date without mocking the private method."""
    mock_db_session = MagicMock()
    service = PoliceStopSearchService(mock_db_session)

    mock_query = mock_db_session.query.return_value
    mock_filter = mock_query.filter.return_value
    mock_filter.scalar.return_value = datetime(2023, 1, 1)

    result = service._get_latest_datetime("suffolk")

    assert result == datetime(2023, 1, 1)
    
    mock_db_session.query.assert_called()

@pytest.mark.parametrize(
    "available_dates, latest_db_date, expected_dates",
    [
        # No previous data, should fetch all
        (["2023-01", "2023-02"], None, ["2023-01", "2023-02"]),
        # Latest data is 2023-01, should fetch 2023-02
        (["2023-01", "2023-02"], datetime(2023, 1, 15), ["2023-02"]),
        # Latest data is 2023-02, should fetch nothing
        (["2023-01", "2023-02"], datetime(2023, 2, 15), []),
        # Latest data is older than all available, fetch all
        (["2023-02", "2023-03"], datetime(2023, 1, 15), ["2023-02", "2023-03"]),
    ],
    ids=["no_previous_data", "latest_jan", "latest_feb", "latest_older"]
)
def test_get_dates_to_process_filtering(
    db, mocker, available_dates, latest_db_date, expected_dates
):
    service = PoliceStopSearchService(db)

    # Mock available dates
    mocker.patch.object(
        service,
        "_get_available_dates",
        return_value={"leicestershire": available_dates},
    )

    # Mock latest date in DB
    mocker.patch.object(service, "_get_latest_datetime", return_value=latest_db_date)

    dates = service._get_dates_to_process("leicestershire")

    assert dates == expected_dates