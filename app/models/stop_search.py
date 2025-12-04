from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.session import Base


class StopSearch(Base):
    __tablename__ = "stop_searches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    force: Mapped[str] = mapped_column(String, index=True)

    type: Mapped[str] = mapped_column(String)
    involved_person: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    datetime: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), index=True
    )
    operation: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    operation_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Location (Flattened)
    latitude: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    longitude: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    street_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    street_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    gender: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    age_range: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    self_defined_ethnicity: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    officer_defined_ethnicity: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )

    legislation: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    object_of_search: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    outcome: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    outcome_linked_to_object_of_search: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )
    removal_of_more_than_outer_clothing: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True
    )

    # Outcome Object (Flattened)
    outcome_object_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    outcome_object_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
