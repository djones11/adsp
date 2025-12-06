import os
from unittest.mock import patch

from app.core.config import Settings


def test_settings_assemble_db_connection():
    settings = Settings(DATABASE_URL="postgresql://user:pass@host:5432/db")

    assert str(settings.DATABASE_URL) == "postgresql://user:pass@host:5432/db"

    # We use patch.dict to simulate environment variables, which is how the app runs.
    # We also need to ensure DATABASE_URL is not set in env, or is empty.
    with patch.dict(
        os.environ,
        {
            "POSTGRES_USER": "user",
            "POSTGRES_PASSWORD": "pass",
            "POSTGRES_SERVER": "host",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "db",
            "DATABASE_URL": "",
        },
    ):
        settings = Settings()
