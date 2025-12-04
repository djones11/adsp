from datetime import datetime as dt_type
from typing import Generic, List, Optional, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class OutcomeObject(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None


class Street(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None


class Location(BaseModel):
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    street: Optional[Street] = None


class StopSearchBase(BaseModel):
    type: Optional[str] = None
    involved_person: Optional[bool] = None
    datetime: Optional[dt_type] = None
    operation: Optional[bool] = None
    operation_name: Optional[str] = None
    location: Optional[Location] = None
    gender: Optional[str] = None
    age_range: Optional[str] = None
    self_defined_ethnicity: Optional[str] = None
    officer_defined_ethnicity: Optional[str] = None
    legislation: Optional[str] = None
    object_of_search: Optional[str] = None
    outcome: Optional[str] = None
    outcome_linked_to_object_of_search: Optional[bool] = None
    removal_of_more_than_outer_clothing: Optional[bool] = None
    outcome_object: Optional[OutcomeObject] = None


class StopSearchCreate(StopSearchBase):
    pass


class StopSearch(StopSearchBase):
    id: int
    force: str

    class Config:
        from_attributes = True


class PaginatedResponse(BaseModel, Generic[T]):
    total: int
    page: int
    page_size: int
    data: List[T]
