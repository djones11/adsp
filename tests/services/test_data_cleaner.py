from app.services.data_cleaner import DataCleaner


def test_clean_outcome_false():
    item = {"outcome": False, "type": "Person search"}
    cleaned = DataCleaner.clean(item)

    assert cleaned["outcome"] == "Nothing found"
    assert cleaned["involved_person"] is True


def test_clean_vehicle_search():
    item = {"outcome": "Arrest", "type": "Vehicle search"}
    cleaned = DataCleaner.clean(item)

    assert cleaned["involved_person"] is False
    assert cleaned["outcome"] == "Arrest"


def test_clean_person_search():
    item = {"outcome": "Arrest", "type": "Person search"}
    cleaned = DataCleaner.clean(item)
    
    assert cleaned["involved_person"] is True
