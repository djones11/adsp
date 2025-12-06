from datetime import datetime as dt_type

from sqlalchemy import Boolean, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.session import Base


class StopSearch(Base):
    __tablename__ = "stop_searches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    force: Mapped[str] = mapped_column(String, index=True)

    type: Mapped[str] = mapped_column(String)
    involved_person: Mapped[bool] = mapped_column(Boolean)
    datetime: Mapped[dt_type] = mapped_column(DateTime(timezone=True), index=True)
    operation: Mapped[bool] = mapped_column(Boolean, nullable=True)
    operation_name: Mapped[str] = mapped_column(String, nullable=True)

    latitude: Mapped[str] = mapped_column(String, nullable=True)
    longitude: Mapped[str] = mapped_column(String, nullable=True)
    street_id: Mapped[int] = mapped_column(Integer, nullable=True)
    street_name: Mapped[str] = mapped_column(String, nullable=True)

    gender: Mapped[str] = mapped_column(String, nullable=True)
    age_range: Mapped[str] = mapped_column(String, nullable=True)
    self_defined_ethnicity: Mapped[str] = mapped_column(String, nullable=True)
    officer_defined_ethnicity: Mapped[str] = mapped_column(String, nullable=True)

    legislation: Mapped[str] = mapped_column(String, nullable=True)
    object_of_search: Mapped[str] = mapped_column(String, nullable=True)
    outcome: Mapped[str] = mapped_column(String, nullable=True)
    outcome_linked_to_object_of_search: Mapped[bool] = mapped_column(
        Boolean, nullable=True
    )
    removal_of_more_than_outer_clothing: Mapped[bool] = mapped_column(
        Boolean, nullable=True
    )

    outcome_object_id: Mapped[str] = mapped_column(String, nullable=True)
    outcome_object_name: Mapped[str] = mapped_column(String, nullable=True)
