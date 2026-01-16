"""Database session management for Jsling."""

import os
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from jsling.database.models import Base
from jsling.database.config import JSLING_HOME, DB_PATH


def get_db_url() -> str:
    """Get database URL."""
    return f"sqlite:///{DB_PATH}"


def ensure_jsling_home() -> None:
    """Ensure ~/.jsling directory exists with proper permissions."""
    if not JSLING_HOME.exists():
        JSLING_HOME.mkdir(parents=True, exist_ok=True)
        # Set directory permissions to 700 (owner read/write/execute only)
        os.chmod(JSLING_HOME, 0o700)
    
    # Create logs directory
    logs_dir = JSLING_HOME / "logs"
    if not logs_dir.exists():
        logs_dir.mkdir(exist_ok=True)


# Create engine
engine = None
SessionLocal = None


def init_database(force: bool = False) -> None:
    """Initialize database and create all tables.
    
    Args:
        force: If True, delete existing database file before initialization.
    """
    global engine, SessionLocal
    
    ensure_jsling_home()
    
    # If force is True, delete existing database file
    if force and DB_PATH.exists():
        DB_PATH.unlink()
    
    # Create engine
    db_url = get_db_url()
    engine = create_engine(db_url, echo=False, connect_args={"check_same_thread": False})
    
    # Create all tables
    Base.metadata.create_all(bind=engine)
    
    # Set database file permissions to 600 (owner read/write only)
    if DB_PATH.exists():
        os.chmod(DB_PATH, 0o600)
    
    # Create session factory
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_session() -> Generator[Session, None, None]:
    """Get database session.
    
    Usage:
        with next(get_session()) as session:
            # use session
            pass
    """
    if SessionLocal is None:
        init_database()
    
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db() -> Session:
    """Get database session (simple version).
    
    Returns:
        Session object. Caller is responsible for closing.
    """
    if SessionLocal is None:
        init_database()
    
    return SessionLocal()
