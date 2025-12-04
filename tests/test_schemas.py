from datetime import datetime

import pytest
from pydantic import ValidationError

from app.schemas.item import Item, ItemCreate


def test_item_create_valid():
    item = ItemCreate(name="Test Item", value=10)
    assert item.name == "Test Item"
    assert item.value == 10
    assert item.description is None


def test_item_create_invalid():
    with pytest.raises(ValidationError):
        ItemCreate(name="Test Item", value="not an int")


def test_item_response():
    now = datetime.now()
    item = Item(id=1, name="Test Item", value=10, created_at=now)
    assert item.id == 1
    assert item.created_at == now
