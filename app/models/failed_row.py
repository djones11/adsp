from datetime import datetime
from typing import Any, Dict

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.session import Base


class FailedRow(Base):
    __tablename__ = "failed_rows"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_data: Mapped[Dict[str, Any]] = mapped_column(JSONB)  # Store the original JSON
    reason: Mapped[str] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
