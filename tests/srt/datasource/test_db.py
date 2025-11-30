import pytest
from sqlalchemy import text

from srt.datasource.utils import DB


@pytest.fixture(autouse=True)
def reset_db_singleton():
    DB._instance = None
    yield
    DB._instance = None


def test_session_factory_executes_queries():
    db = DB("sqlite+pysqlite:///:memory:")
    session_factory = db.get_session_factory()

    session = session_factory()
    try:
        result = session.execute(text("SELECT 1")).scalar_one()
        assert result == 1
    finally:
        session.close()


def test_get_instance_returns_singleton():
    helper = DB("sqlite+pysqlite:///:memory:")

    first = helper.get_instance("sqlite+pysqlite:///:memory:")
    second = helper.get_instance("sqlite+pysqlite:///:memory:")

    assert first is second
