from unittest.mock import MagicMock, patch

import pytest

from app.models.failed_row import FailedRow
from app.models.stop_search import StopSearch
from app.services.stop_search_service import PoliceStopSearchService


@pytest.fixture
def mock_db():
    return MagicMock()


def test_remediate_failed_rows_no_rows(mock_db):
    service = PoliceStopSearchService(mock_db)
    # Mock query to return empty list
    mock_db.query.return_value.filter.return_value.all.return_value = []

    service.remediate_failed_rows()

    # Verify no further actions taken
    mock_db.add.assert_not_called()


def test_remediate_failed_rows_success(mock_db, mocker):
    service = PoliceStopSearchService(mock_db)

    # Mock failed row
    failed_row = MagicMock(spec=FailedRow)
    failed_row.id = 1
    failed_row.raw_data = {"some": "data"}
    failed_row.source = StopSearch.__tablename__

    mock_db.query.return_value.filter.return_value.all.return_value = [failed_row]

    # Mock DataCleaner
    mocker.patch(
        "app.services.stop_search_service.DataCleaner.clean",
        return_value={"some": "cleaned_data"},
    )

    # Mock StopSearch creation (since we pass **cleaned_data)
    with patch("app.services.stop_search_service.StopSearch") as MockStopSearch:
        MockStopSearch.__tablename__ = StopSearch.__tablename__
        service.remediate_failed_rows()

        MockStopSearch.assert_called_with(some="cleaned_data")
        mock_db.add.assert_called()
        mock_db.delete.assert_called_with(failed_row)
        mock_db.commit.assert_called()


def test_remediate_failed_rows_exception(mock_db, mocker):
    service = PoliceStopSearchService(mock_db)

    failed_row = MagicMock(spec=FailedRow)
    failed_row.id = 1
    failed_row.raw_data = {"some": "data"}

    mock_db.query.return_value.filter.return_value.all.return_value = [failed_row]

    # Mock DataCleaner to raise exception
    mocker.patch(
        "app.services.stop_search_service.DataCleaner.clean",
        side_effect=Exception("Cleaning failed"),
    )

    service.remediate_failed_rows()

    mock_db.rollback.assert_called()
    mock_db.add.assert_not_called()
