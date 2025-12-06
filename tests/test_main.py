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
        involved_person=True,
        operation=False,
        operation_name="Op1",
        latitude="52.0",
        longitude="0.0",
        street_id=1,
        street_name="Main St",
        gender="Male",
        self_defined_ethnicity="White",
        officer_defined_ethnicity="White",
        legislation="PACE",
        object_of_search="Drugs",
        outcome="Arrest",
        outcome_linked_to_object_of_search=True,
        removal_of_more_than_outer_clothing=False,
        outcome_object_id="1",
        outcome_object_name="Arrest",
    )
    item2 = StopSearch(
        force="metropolitan",
        datetime=datetime(2024, 1, 2, 12, 0, 0),
        type="Vehicle search",
        age_range="25-34",
        involved_person=True,
        operation=False,
        operation_name="Op2",
        latitude="51.5",
        longitude="-0.1",
        street_id=2,
        street_name="High St",
        gender="Female",
        self_defined_ethnicity="Black",
        officer_defined_ethnicity="Black",
        legislation="Misuse of Drugs Act",
        object_of_search="Weapons",
        outcome="No further action",
        outcome_linked_to_object_of_search=False,
        removal_of_more_than_outer_clothing=False,
        outcome_object_id="2",
        outcome_object_name="NFA",
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
