"""Engine + session lifecycle for SQLite at data/awksion.db."""
from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from awksion.config import DB_URL, DATA_DIR
from awksion.db.models import Base

_engine: Engine | None = None
_SessionFactory: sessionmaker | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        _engine = create_engine(
            DB_URL,
            future=True,
            connect_args={"timeout": 30},  # wait up to 30s on lock
        )

        @event.listens_for(_engine, "connect")
        def _on_connect(dbapi_conn, _record):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.close()

    return _engine


def init_db() -> None:
    """Create all tables. Idempotent."""
    Base.metadata.create_all(get_engine())


@contextmanager
def get_session() -> Session:
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=get_engine(), expire_on_commit=False, future=True)
    session = _SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
