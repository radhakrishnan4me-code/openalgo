"""First-turn run writes work with SQLite foreign keys enabled."""

import pytest

pytest.importorskip("agno")

from agno.db.base import SessionType
from agno.run.agent import RunOutput
from agno.run.base import RunStatus
from agno.session import AgentSession
from sqlalchemy import select

from database.engine_factory import create_db_engine
from services.agent.session_store import SessionFirstSqliteDb


@pytest.fixture
def store(tmp_path):
    engine = create_db_engine(f"sqlite:///{(tmp_path / 'agent.db').as_posix()}")
    db = SessionFirstSqliteDb(db_engine=engine)
    yield db
    db.Session.remove()
    engine.dispose()


def test_first_failed_run_has_parent_session(store):
    run = RunOutput(run_id="failed-run", agent_id="agent", status=RunStatus.error)
    store.upsert_run(run, session_id="new-session", user_id="user", run_index=0)
    session = store.get_session(session_id="new-session", session_type=SessionType.AGENT)
    assert session is not None
    assert session.agent_id == "agent"
    assert session.user_id == "user"
    table = store._get_table("runs")
    with store.Session() as connection:
        row = (
            connection.execute(select(table).where(table.c.run_id == "failed-run")).mappings().one()
        )
        assert row["session_id"] == "new-session"
        assert row["status"] == "ERROR"


def test_existing_session_and_run_index_are_preserved(store):
    store.upsert_session(
        AgentSession(
            session_id="existing",
            agent_id="agent",
            user_id="user",
            session_data={"session_state": {"keep": "me"}},
            metadata={"custom": True},
            created_at=10,
            updated_at=20,
        )
    )
    table = store._get_table("sessions")
    with store.Session() as connection:
        before = dict(connection.execute(select(table)).mappings().one())
    run = RunOutput(run_id="retry", agent_id="agent", status=RunStatus.error)
    store.upsert_run(run, session_id="existing", user_id="user", run_index=2)
    store.upsert_run(run, session_id="existing", user_id="user", run_index=99)
    with store.Session() as connection:
        after = dict(connection.execute(select(table)).mappings().one())
        assert after == before
        runs = store._get_table("runs")
        assert connection.execute(select(runs.c.run_index)).scalar_one() == 2
