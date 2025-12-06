import os
from unittest.mock import patch
from app.core.config import Settings

def test_settings_assemble_db_connection():
    """Test Settings.assemble_db_connection logic."""
    
    # Case 1: DATABASE_URL provided
    settings = Settings(
        DATABASE_URL="postgresql://user:pass@host:5432/db",
        CELERY_BROKER_URL="redis://",
        CELERY_RESULT_BACKEND="redis://",
    )
    assert str(settings.DATABASE_URL) == "postgresql://user:pass@host:5432/db"

    # Case 2: Components provided
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
        settings = Settings(
            CELERY_BROKER_URL="redis://", CELERY_RESULT_BACKEND="redis://"
        )
        assert str(settings.DATABASE_URL) == "postgresql://user:pass@host:5432/db"
