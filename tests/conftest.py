import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db import Base
from tests.helpers import register_sqlite_extensions


@pytest.fixture(scope="session")
def engine():
    """Single in-memory SQLite DB shared for the entire test session."""
    eng = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    register_sqlite_extensions(eng)
    Base.metadata.create_all(bind=eng)
    return eng


@pytest.fixture()
def db(engine):
    """Fresh session for each test, automatically rolled back after."""
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.rollback()
    session.close()
