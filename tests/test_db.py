from unittest.mock import MagicMock, patch
from app.db.session import get_db

def test_get_db():
    """Test the get_db dependency generator."""
    mock_session = MagicMock()
    
    # We mock SessionLocal to return our mock_session
    with patch("app.db.session.SessionLocal", return_value=mock_session):
        # get_db is a generator
        gen = get_db()
        db = next(gen)
        
        assert db == mock_session
        
        # When the generator finishes (e.g. dependency is done), it closes the session
        try:
            next(gen)
        except StopIteration:
            pass
            
        mock_session.close.assert_called_once()
