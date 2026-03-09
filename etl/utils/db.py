"""
SQLAlchemy connection-pool helper.
DATABASE_URL is read from the environment (set in .env / Docker secret).
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

_engine: Engine | None = None


def _build_engine() -> Engine:
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql://airflow:airflow@localhost:5432/ecommerce_dw",
    )
    return create_engine(
        url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,          # detect stale connections
        pool_recycle=3600,           # recycle every hour
        echo=False,
    )


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = _build_engine()
    return _engine


def get_session_factory() -> sessionmaker:
    return sessionmaker(bind=get_engine(), expire_on_commit=False)


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Context-manager that commits on success and rolls back on any exception."""
    SessionLocal = get_session_factory()
    session: Session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def execute_sql(sql: str, params: dict | None = None) -> list[dict]:
    """Run a raw SQL statement and return rows as list-of-dicts (read-only helper)."""
    with get_engine().connect() as conn:
        result = conn.execute(text(sql), params or {})
        if result.returns_rows:
            keys = result.keys()
            return [dict(zip(keys, row)) for row in result]
        return []
