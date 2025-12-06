import os
from unittest.mock import patch

from app.core.config import Settings


def test_settings_correctly_assembles_database_url():
    settings = Settings(DATABASE_URL="postgresql://user:pass@host:5432/db")

    assert str(settings.DATABASE_URL) == "postgresql://user:pass@host:5432/db"

    # Uses patch.dict to simulate environment variables, which is how the app runs.
    # Also need to ensure DATABASE_URL is not set in env, or is empty.
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
