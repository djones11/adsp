from datetime import datetime
from unittest.mock import MagicMock

from app.models.stop_search import StopSearch


def test_read_root(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to the ADSP Project API"}


def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_get_stop_searches(client, db, mocker):
    # Seed data
    item1 = StopSearch(
        force="leicestershire",
        datetime=datetime(2024, 1, 1, 12, 0, 0),
        type="Person search",
        age_range="18-24",
    )
    item2 = StopSearch(
        force="metropolitan",
        datetime=datetime(2024, 1, 2, 12, 0, 0),
        type="Vehicle search",
        age_range="25-34",
    )
    db.add(item1)
    db.add(item2)
    db.commit()

    # Test no filters
    response = client.get("/v1/stop-searches")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    assert len(data["data"]) == 2

    # Test filter by force
    response = client.get("/v1/stop-searches?force=leicestershire")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["data"][0]["force"] == "leicestershire"

    # Test filter by date
    response = client.get("/v1/stop-searches?date_start=2024-01-02")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["data"][0]["force"] == "metropolitan"

    # Test pagination
    response = client.get("/v1/stop-searches?page=1&page_size=1")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    assert len(data["data"]) == 1
    assert data["page"] == 1
    assert data["page_size"] == 1
