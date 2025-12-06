import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from app.services.police_api import PoliceAPIService

def test_get_dates_to_process(db, mocker):
    service = PoliceAPIService(db)
    
    # Mock get_available_dates
    mocker.patch.object(service, "get_available_dates", return_value={
        "leicestershire": ["2023-01", "2023-02", "2023-03"]
    })
    
    # Mock get_latest_datetime
    mocker.patch.object(service, "get_latest_datetime", return_value=datetime(2023, 1, 15))
    
    # Test with no target date
    dates = service.get_dates_to_process("leicestershire")
    assert dates == ["2023-02", "2023-03"]

def test_get_dates_to_process_no_availability(db, mocker):
    service = PoliceAPIService(db)
    mocker.patch.object(service, "get_available_dates", return_value={})
    
    dates = service.get_dates_to_process("leicestershire")
    assert dates == []

def test_get_dates_to_process_no_latest_date(db, mocker):
    service = PoliceAPIService(db)
    
    mocker.patch.object(service, "get_available_dates", return_value={
        "leicestershire": ["2023-01", "2023-02"]
    })
    mocker.patch.object(service, "get_latest_datetime", return_value=None)
    
    dates = service.get_dates_to_process("leicestershire")
    assert dates == ["2023-01", "2023-02"]
