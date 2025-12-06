from datetime import datetime

import pytest

from app.models.stop_search import StopSearch

API_ENDPOINT = "/v1/stop-searches"


@pytest.fixture
def populate_seed_data(db):
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
    yield


def test_get_stop_searches_returns_all_records(client, db, populate_seed_data):
    response = client.get(f"{API_ENDPOINT}/")

    assert response.status_code == 200

    data = response.json()

    assert data["total"] == 2
    assert len(data["data"]) == 2


def test_get_stop_searches_filters_by_force(client, db, populate_seed_data):
    response = client.get(f"{API_ENDPOINT}/?force=leicestershire")

    assert response.status_code == 200

    data = response.json()

    assert data["total"] == 1
    assert data["data"][0]["force"] == "leicestershire"


def test_get_stop_searches_filters_by_start_date(client, db, populate_seed_data):
    # Test filter by date
    response = client.get(f"{API_ENDPOINT}/?date_start=2024-01-02")

    assert response.status_code == 200

    data = response.json()

    assert data["total"] == 1
    assert data["data"][0]["force"] == "metropolitan"


def test_stop_searches_pagination(client, db, populate_seed_data):
    # Test pagination
    response = client.get(f"{API_ENDPOINT}/?page=1&page_size=1")

    assert response.status_code == 200

    data = response.json()

    assert data["total"] == 2
    assert len(data["data"]) == 1
    assert data["page"] == 1
    assert data["page_size"] == 1
