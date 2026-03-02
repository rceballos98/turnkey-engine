import os

import pytest
from dotenv import load_dotenv

# Load .env before any app imports so DATABASE_URL is available.
_root = os.path.join(os.path.dirname(__file__), "..")
load_dotenv(os.path.join(_root, ".env"))

from sqlalchemy import text  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from app.main import app  # noqa: E402
from app.config import settings  # noqa: E402
from app.database import SessionLocal, engine, Base  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def setup_database():
    """Drop and recreate all tables so the schema matches current models."""
    # Drop all tables with CASCADE to handle leftover FKs from other schemas
    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
        conn.commit()
    Base.metadata.create_all(bind=engine)
    yield


@pytest.fixture()
def db():
    """Provide a DB session; truncate test tables after each test."""
    session = SessionLocal()
    try:
        yield session
    finally:
        from app.models import JobQueue, Report
        session.execute(JobQueue.__table__.delete())
        session.execute(Report.__table__.delete())
        session.commit()
        session.close()


@pytest.fixture()
def client():
    """FastAPI TestClient (synchronous, backed by httpx)."""
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def auth_headers():
    """Bearer auth headers using the configured internal API key."""
    return {"Authorization": f"Bearer {settings.internal_api_key}"}
