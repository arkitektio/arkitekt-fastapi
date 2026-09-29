import aiosqlite
import json
from collections.abc import Sequence
from datetime import datetime, UTC
from typing import Any

from arkitekt_runtime.agents.journal import Fold, JournalEntry

from arkitekt_fastapi.retriever.protocol import (
    PatchEvent,
    SessionBoundary,
    Snapshot,
    TaskBoundary,
)
from arkitekt_fastapi.retriever.replay import (
    boundary_from_aggregates,
    build_patch_document,
    merge_state_ids,
    replay,
)
from arkitekt_fastapi.sql_lite.schema import ensure_sqlite_schema


# ==========================================
# 2. Helpers
# ==========================================
def dt_to_epoch_ms(dt: datetime) -> int:
    """Converts a timezone-aware datetime to epoch milliseconds."""
    return int(dt.timestamp() * 1000)


def epoch_ms_to_dt(ms: int) -> datetime:
    """Converts epoch milliseconds back to a timezone-aware datetime."""
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC)


# ==========================================
# 3. The Unified Store Class
# ==========================================
class SQLLiteRetriever:
    def __init__(self, db_path: str = "ff.db"):
        self.db_path = db_path
        self.current_session_id: str | None = None

    # --- INITIALIZATION & SESSION MANAGEMENT ---
    async def ainitialize(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await ensure_sqlite_schema(db)
            await db.commit()

        return None

    # --- READ / RETRIEVE METHODS ---
    async def _aboundaries(
        self,
        column: str,
        key: str,
        state_id: str | None,
        *,
        session: bool,
    ) -> TaskBoundary | SessionBoundary | None:
        # `column` is one of two literals chosen below; values stay parameterized.
        state_filter = "AND state_id = ?" if state_id is not None else ""
        params: tuple[object, ...] = (key,) if state_id is None else (key, state_id)
        query = f"""
        SELECT MIN(global_current_rev), MAX(global_future_rev), MIN(event_time), MAX(event_time)
        FROM state_patches
        WHERE {column} = ? {state_filter};
        """

        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(query, params) as cursor:
                row = await cursor.fetchone()

        if not row or row[0] is None:
            return None
        return boundary_from_aggregates(
            key,
            row[0],
            row[1],
            epoch_ms_to_dt(row[2]),
            epoch_ms_to_dt(row[3]),
            session=session,
        )

    async def aget_task_boundaries(
        self,
        correlation_id: str,
        state_id: str | None = None,
    ) -> TaskBoundary | None:
        boundary = await self._aboundaries(
            "correlation_id", correlation_id, state_id, session=False
        )
        return boundary if isinstance(boundary, TaskBoundary) else None

    async def aget_session_boundaries(
        self, session_id: str, state_id: str | None = None
    ) -> SessionBoundary | None:
        boundary = await self._aboundaries(
            "session_id", session_id, state_id, session=True
        )
        return boundary if isinstance(boundary, SessionBoundary) else None

    async def aget_state_at_global_rev(
        self,
        global_revision: int,
        state_id: str | None = None,
        session_id: str | None = None,
    ) -> Snapshot | list[Snapshot] | None:
        return await self._aget_state_at_revision(
            target_revision=global_revision,
            state_id=state_id,
            session_id=session_id,
        )

    async def aget_forward_events_after_rev(
        self,
        global_revision: int,
        state_id: str | None = None,
        session_id: str | None = None,
        count: int = 100,
    ) -> list[PatchEvent]:
        session_filter = "AND session_id = ?" if session_id is not None else ""
        state_filter = "AND state_id = ?" if state_id is not None else ""
        params: tuple[object, ...] = (global_revision, count)
        if state_id is not None and session_id is not None:
            params = (global_revision, state_id, session_id, count)
        elif state_id is not None:
            params = (global_revision, state_id, count)
        elif session_id is not None:
            params = (global_revision, session_id, count)

        query = f"""
        SELECT state_id, global_current_rev, global_future_rev,
               event_time, correlation_id, session_id, op, path, value
        FROM state_patches
        WHERE global_current_rev >= ? {state_filter} {session_filter}
        ORDER BY global_current_rev ASC, state_id ASC
        LIMIT ?
        """

        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()

        return [self._row_to_patch_event(row) for row in rows]

    async def aget_patch_events_between_global_revs(
        self,
        from_global_revision: int,
        to_global_revision: int,
        state_ids: list[str] | None = None,
        session_id: str | None = None,
    ) -> list[PatchEvent]:
        if to_global_revision < from_global_revision:
            return []

        state_filter = ""
        params: list[object] = [from_global_revision, to_global_revision]

        if state_ids:
            placeholders = ", ".join("?" for _ in state_ids)
            state_filter = f"AND state_id IN ({placeholders})"
            params.extend(state_ids)

        session_filter = "AND session_id = ?" if session_id is not None else ""
        if session_id is not None:
            params.append(session_id)

        query = f"""
        SELECT state_id, global_current_rev, global_future_rev,
               event_time, correlation_id, session_id, op, path, value
        FROM state_patches
        WHERE global_current_rev >= ? AND global_future_rev <= ?
              {state_filter} {session_filter}
        ORDER BY global_current_rev ASC, state_id ASC
        """

        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(query, tuple(params)) as cursor:
                rows = await cursor.fetchall()

        return [self._row_to_patch_event(row) for row in rows]

    async def aget_snapshots_around_rev(
        self,
        revision: int,
        state_id: str | None = None,
        session_id: str | None = None,
        before: int = 1,
        after: int = 1,
    ) -> list[Snapshot]:
        state_ids = (
            [state_id]
            if state_id is not None
            else await self._aget_state_ids(session_id)
        )
        collected: list[Snapshot] = []

        for candidate_state_id in state_ids:
            before_query = """
        SELECT global_revision, event_time, session_id, state_data
        FROM state_snapshots
        WHERE state_id = ? AND global_revision <= ? {session_filter}
        ORDER BY global_revision DESC
        LIMIT ?
        """
            after_query = """
        SELECT global_revision, event_time, session_id, state_data
        FROM state_snapshots
        WHERE state_id = ? AND global_revision > ? {session_filter}
        ORDER BY global_revision ASC
        LIMIT ?
        """
            session_filter = "AND session_id = ?" if session_id is not None else ""
            before_params: tuple[object, ...] = (candidate_state_id, revision, before)
            after_params: tuple[object, ...] = (candidate_state_id, revision, after)
            if session_id is not None:
                before_params = (candidate_state_id, revision, session_id, before)
                after_params = (candidate_state_id, revision, session_id, after)

            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute(
                    before_query.format(session_filter=session_filter), before_params
                ) as cursor:
                    before_rows = await cursor.fetchall()
                async with db.execute(
                    after_query.format(session_filter=session_filter), after_params
                ) as cursor:
                    after_rows = await cursor.fetchall()

            collected.extend(
                self._row_to_snapshot(row)
                for row in [*reversed(before_rows), *after_rows]
            )

        return collected

    async def _aget_state_at_revision(
        self,
        target_revision: int,
        state_id: str | None,
        session_id: str | None,
    ) -> Snapshot | list[Snapshot] | None:
        if state_id is None:
            state_ids = await self._aget_state_ids(session_id)
            snapshots = [
                snapshot
                for snapshot in [
                    await self._aget_state_at_revision(
                        target_revision=target_revision,
                        state_id=candidate_state_id,
                        session_id=session_id,
                    )
                    for candidate_state_id in state_ids
                ]
                if isinstance(snapshot, Snapshot)
            ]
            return snapshots

        session_filter = "AND session_id = ?" if session_id is not None else ""

        anchor_query = f"""
        SELECT global_revision, event_time, session_id, state_data
        FROM state_snapshots
        WHERE state_id = ? AND global_revision <= ? {session_filter}
        ORDER BY global_revision DESC
        LIMIT 1
        """
        patch_query = f"""
         SELECT state_id, global_current_rev, global_future_rev,
               event_time, correlation_id, session_id, op, path, value
        FROM state_patches
        WHERE state_id = ? AND global_current_rev >= ? AND global_future_rev <= ? {session_filter}
        ORDER BY global_current_rev ASC
        """

        anchor_params: tuple[object, ...] = (state_id, target_revision)
        patch_start_revision = 0
        if session_id is not None:
            anchor_params = (state_id, target_revision, session_id)

        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(anchor_query, anchor_params) as cursor:
                anchor_row = await cursor.fetchone()

            if anchor_row is None:
                return None

            anchor_snapshot = self._row_to_snapshot(anchor_row)
            patch_start_revision = anchor_snapshot.global_revision or 0

            patch_params: tuple[object, ...] = (
                state_id,
                patch_start_revision,
                target_revision,
            )
            if session_id is not None:
                patch_params = (
                    state_id,
                    patch_start_revision,
                    target_revision,
                    session_id,
                )

            async with db.execute(patch_query, patch_params) as cursor:
                patch_rows = await cursor.fetchall()

        return replay(
            anchor_snapshot, (self._row_to_patch_event(row) for row in patch_rows)
        )

    async def _aget_state_ids(self, session_id: str | None) -> list[str]:
        session_filter = "WHERE session_id = ?" if session_id is not None else ""
        params: tuple[object, ...] = (session_id,) if session_id is not None else ()

        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                f"SELECT DISTINCT state_id FROM state_snapshots {session_filter}",
                params,
            ) as cursor:
                snapshot_state_ids = [row[0] for row in await cursor.fetchall()]
            async with db.execute(
                f"SELECT DISTINCT state_id FROM state_patches {session_filter}", params
            ) as cursor:
                patch_state_ids = [row[0] for row in await cursor.fetchall()]

        return merge_state_ids(snapshot_state_ids, patch_state_ids)

    def _row_to_snapshot(self, row: tuple[Any, ...]) -> Snapshot:
        global_revision, event_time, session_id, state_data = row
        return Snapshot(
            timepoint=epoch_ms_to_dt(event_time),
            data=json.loads(state_data),
            global_revision=global_revision,
            session_id=session_id,
        )

    def _row_to_patch_event(self, row: tuple[Any, ...]) -> PatchEvent:
        (
            state_id,
            global_current_rev,
            global_future_rev,
            event_time,
            correlation_id,
            session_id,
            op,
            path,
            value,
        ) = row
        patch_document = build_patch_document(
            op, path, json.loads(value) if value is not None else None
        )

        return PatchEvent(
            timepoint=epoch_ms_to_dt(event_time),
            state_id=state_id,
            global_current_rev=global_current_rev,
            global_future_rev=global_future_rev,
            correlation_id=correlation_id or "",
            session_id=session_id,
            patch=patch_document,
        )

    # --- JOURNAL ---
    async def _aquery_entries(
        self, sql: str, params: Sequence[object]
    ) -> list[JournalEntry]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(sql, tuple(params)) as cursor:
                rows = await cursor.fetchall()
        return [_entry_from_row(row) for row in rows]

    async def aget_journal_entries(
        self,
        session_id: str,
        after: int = 0,
        until: int | None = None,
        limit: int | None = None,
        kinds: Sequence[str] | None = None,
        task_id: str | None = None,
        action_keys: Sequence[str] | None = None,
        state_keys: Sequence[str] | None = None,
        lock_keys: Sequence[str] | None = None,
    ) -> list[JournalEntry]:
        """Journal entries of a session, in order. Key filters route as the websocket
        does: patches by state, locks by key, session-wide entries always, the rest
        by action key."""
        params: list[object] = [session_id, after]
        sql = f"SELECT {JOURNAL_COLUMNS} FROM journal WHERE session_id = ? AND pos > ?"
        if until is not None:
            sql += " AND pos <= ?"
            params.append(until)
        if kinds:
            sql += f" AND {_in_list('kind', kinds, params)}"
        if task_id is not None:
            sql += " AND task_id = ?"
            params.append(task_id)
        if action_keys is not None or state_keys is not None or lock_keys is not None:

            def keyed(column: str, keys: Sequence[str] | None) -> str:
                return _in_list(column, keys, params) if keys else "1"

            state = keyed("subject", state_keys)
            lock = keyed("subject", lock_keys)
            action = keyed("action_key", action_keys)
            sql += (
                f" AND ((kind = 'STATE_PATCH' AND {state}) OR (kind IN ('LOCK', 'UNLOCK') AND {lock})"
                f" OR kind IN ({SESSION_KINDS}) OR (kind NOT IN ('STATE_PATCH', 'LOCK', 'UNLOCK', {SESSION_KINDS})"
                f" AND (action_key IS NULL OR {action})))"
            )
        sql += " ORDER BY pos ASC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return await self._aquery_entries(sql, params)

    async def aget_journal_task_entries(self, task_id: str) -> list[JournalEntry]:
        """Every entry of one task, in order."""
        return await self._aquery_entries(
            f"SELECT {JOURNAL_COLUMNS} FROM journal WHERE task_id = ? ORDER BY session_id, pos",
            [task_id],
        )

    async def aget_journal_pos_at_time(self, session_id: str, ms: int) -> int | None:
        """The last position of a session at or before ``ms`` (epoch milliseconds)."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT MAX(pos) FROM journal WHERE session_id = ? AND event_time <= ?",
                (session_id, ms),
            ) as cursor:
                row = await cursor.fetchone()
        return row[0] if row and row[0] is not None else None

    async def aget_journal_world(
        self, session_id: str, pos: int
    ) -> tuple[JournalEntry, Fold] | None:
        """The entry at ``pos`` and the world as of it: states replayed from the last
        snapshot entry, tasks and locks folded from every entry before."""
        at = await self._aquery_entries(
            f"SELECT {JOURNAL_COLUMNS} FROM journal WHERE session_id = ? AND pos = ?",
            [session_id, pos],
        )
        if not at:
            return None
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                f"SELECT COALESCE(MAX(pos), 0) FROM journal WHERE session_id = ? AND pos <= ? AND kind IN ({SESSION_KINDS})",
                (session_id, pos),
            ) as cursor:
                row = await cursor.fetchone()
        anchor = row[0] if row else 0
        entries = await self._aquery_entries(
            f"SELECT {JOURNAL_COLUMNS} FROM journal WHERE session_id = ? AND pos <= ? "
            f"AND ((kind IN ({STATE_KINDS}) AND pos >= ?) OR kind NOT IN ({STATE_KINDS})) ORDER BY pos ASC",
            [session_id, pos, anchor],
        )
        return at[0], Fold.from_entries(entries)

    async def ateardown(self) -> None:
        """Cleans up resources, such as database connections."""
        return None


JOURNAL_COLUMNS = (
    "session_id, pos, global_rev, event_time, kind, task_id, action_key, subject, message_id, payload, step"
)
#: Kinds that belong to the whole session rather than to a task, state or lock.
SESSION_KINDS = "'SESSION_INIT', 'STATE_SNAPSHOT'"
#: Kinds that carry state values.
STATE_KINDS = "'SESSION_INIT', 'STATE_SNAPSHOT', 'STATE_PATCH'"


def _in_list(column: str, values: Sequence[str], params: list[object]) -> str:
    params.extend(values)
    return f"{column} IN ({', '.join('?' for _ in values)})"


def _entry_from_row(row: Sequence[Any]) -> JournalEntry:
    (
        session_id,
        pos,
        global_rev,
        event_time,
        kind,
        task_id,
        action_key,
        subject,
        message_id,
        payload,
        step,
    ) = row
    try:
        decoded = json.loads(payload)
    except (TypeError, ValueError):
        decoded = None
    return JournalEntry(
        session_id=session_id,
        pos=pos,
        global_rev=global_rev,
        event_time=event_time,
        kind=kind,
        task_id=task_id,
        action_key=action_key,
        subject=subject,
        message_id=message_id,
        payload=decoded if isinstance(decoded, dict) else {},
        step=step,
    )
