from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class ItemBase(BaseModel):
    name: str
    description: Optional[str] = None
    value: int


class ItemCreate(ItemBase):
    pass


class Item(ItemBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True
