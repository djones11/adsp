from datetime import datetime as dt_type
from typing import Generic, List, Optional, TypeVar

import pandas as pd
import pandera as pa
from pandera.typing import Series
from pydantic import BaseModel, ConfigDict

from app.core.config import AVAILABLE_FORCES

T = TypeVar("T")

# Expected schemas for StopSearch data coming from the police API
# Pydantic models for API responses and internal use


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
    type: str
    involved_person: bool
    datetime: dt_type
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


class StopSearch(StopSearchBase):
    id: int
    force: AVAILABLE_FORCES

    model_config = ConfigDict(from_attributes=True)


class PaginatedResponse(BaseModel, Generic[T]):
    total: int
    page: int
    page_size: int
    data: List[T]


# Pandera schema for data validation and coercion


class StopSearchDataFrameSchema(pa.DataFrameModel):
    force: Series[str] = pa.Field()
    type: Series[str] = pa.Field()
    involved_person: Series[pd.BooleanDtype] = pa.Field(coerce=True)
    datetime: Series[pd.DatetimeTZDtype] = pa.Field(
        dtype_kwargs={"unit": "ns", "tz": "UTC"}, coerce=True
    )
    operation: Series[pd.BooleanDtype] = pa.Field(nullable=True, coerce=True)
    operation_name: Series[str] = pa.Field(nullable=True)
    latitude: Series[str] = pa.Field(nullable=True)
    longitude: Series[str] = pa.Field(nullable=True)
    street_id: Series[pd.Int64Dtype] = pa.Field(nullable=True, coerce=True)
    street_name: Series[str] = pa.Field(nullable=True)
    gender: Series[str] = pa.Field(nullable=True)
    age_range: Series[str] = pa.Field(nullable=True)
    self_defined_ethnicity: Series[str] = pa.Field(nullable=True)
    officer_defined_ethnicity: Series[str] = pa.Field(nullable=True)
    legislation: Series[str] = pa.Field(nullable=True)
    object_of_search: Series[str] = pa.Field(nullable=True)
    outcome: Series[str] = pa.Field(nullable=True)
    outcome_linked_to_object_of_search: Series[pd.BooleanDtype] = pa.Field(
        nullable=True, coerce=True
    )
    removal_of_more_than_outer_clothing: Series[pd.BooleanDtype] = pa.Field(
        nullable=True, coerce=True
    )
    outcome_object_id: Series[str] = pa.Field(nullable=True)
    outcome_object_name: Series[str] = pa.Field(nullable=True)

    class Config:
        coerce = True
