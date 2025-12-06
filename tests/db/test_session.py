from unittest.mock import MagicMock, patch

from app.db.session import get_db


def test_get_db_yields_session_and_closes_it():
    """Test the get_db dependency generator."""
    mock_session = MagicMock()

    with patch("app.db.session.SessionLocal", return_value=mock_session):
        gen = get_db()
        db = next(gen)

        assert db == mock_session

        try:
            next(gen)
        except StopIteration:
            pass

        mock_session.close.assert_called_once()
