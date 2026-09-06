"""Agno SQLite compatibility for runs saved before their first session write."""

from __future__ import annotations

from typing import Any

from agno.db.sqlite import SqliteDb
from agno.db.utils import build_single_run_row
from sqlalchemy.dialects.sqlite import insert


class SessionFirstSqliteDb(SqliteDb):
    """Keep Agno 3.0's independent run writes valid with foreign keys enabled.

    A first-turn error can reach upsert_run before Agno persists the session.
    Insert only the missing parent; an existing session's state, metadata and
    timestamps must never be replaced by this minimal record. The normal Agno
    session write subsequently fills in the complete session data.
    """

    def upsert_run(
        self,
        run: Any,
        session_id: str,
        user_id: str | None = None,
        run_index: int | None = None,
    ) -> None:
        """Create a missing session before delegating run persistence to Agno."""
        row = build_single_run_row(
            run=run, session_id=session_id, user_id=user_id, run_index=run_index
        )
        table = self._get_table(table_type="sessions", create_table_if_not_found=True)
        if table is None:
            raise RuntimeError("The agent session table could not be created")
        statement = (
            insert(table)
            .values(
                session_id=session_id,
                session_type=row["run_type"],
                agent_id=row.get("agent_id"),
                team_id=row.get("team_id"),
                workflow_id=row.get("workflow_id"),
                user_id=row.get("user_id"),
                created_at=row.get("created_at"),
                updated_at=row.get("updated_at"),
            )
            .on_conflict_do_nothing(index_elements=[table.c.session_id])
        )
        with self.Session() as session, session.begin():
            session.execute(statement)
        super().upsert_run(run, session_id, user_id=user_id, run_index=run_index)
