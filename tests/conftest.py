"""Pytest fixtures for Jsling tests."""

import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from jsling.database.models import Base
from jsling.database.schema import init_default_configs


@pytest.fixture
def temp_db():
    """Create a temporary database for testing with default configs initialized."""
    # Create temporary database file
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    # Create engine
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    
    # Create all tables
    Base.metadata.create_all(bind=engine)
    
    # Create session factory
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    # Provide session
    session = SessionLocal()
    
    # Initialize default configurations in database
    init_default_configs(session)
    
    yield session
    
    # Cleanup
    session.close()
    engine.dispose()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def temp_jsling_home(tmp_path):
    """Create temporary .jsling directory for testing."""
    jsling_home = tmp_path / ".jsling"
    jsling_home.mkdir()
    
    # Create logs directory
    (jsling_home / "logs").mkdir()
    
    return jsling_home
