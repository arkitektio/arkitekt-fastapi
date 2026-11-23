import asyncio
import json
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

import aiosqlite
from arkitekt_runtime import messages
from arkitekt_runtime.agents.journal import JournalEntry
from arkitekt_spec.declare.protocol.types import AnyState

from arkitekt_fastapi.sink.protocol import StateChange
from arkitekt_fastapi.sql_lite.schema import ensure_sqlite_schema


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
class SQLLiteSink:
    """A sink implementation that uses a SQLite database to store state snapshots and patches. This sink is designed for durability and can be used in production environments where persistence is required. It ensures that patches are written in order and provides methods for checking if the sink is caught up to a certain revision."""

    def __init__(self, db_path: str = "ff.db") -> None:
        """Initializes the SQLLiteSink with the path to the SQLite database file. The sink will use this database to store snapshots and patches. If the file does not exist, it will be created automatically."""
        self.db_path = db_path
        self.current_session_id: str | None = None

    # --- INITIALIZATION & SESSION MANAGEMENT ---
    async def ainitialize(self) -> None:
        """Initializes the SQLLiteSink by creating necessary tables and indexes if they don't exist. This should be called once at the start of the agent's lifecycle."""
        self._write_lock = asyncio.Lock()

        async with aiosqlite.connect(self.db_path) as db:
            await ensure_sqlite_schema(db)
            await db.commit()

    async def acreate_session(
        self, states: list[AnyState], implementations: list
    ) -> str:
        """Create a new session and return its ID. Should be called at the start of a new logical session. By default this will be called automatically on each agent startup, but can also be called manually if the agent wants to manage sessions itself (e.g., create a new session for each user interaction)."""
        new_session = str(uuid.uuid4())
        created_at_ms = dt_to_epoch_ms(datetime.now(UTC))

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO sessions (session_id, created_at) VALUES (?, ?)",
                (new_session, created_at_ms),
            )
            await db.commit()

        self.current_session_id = new_session
        return new_session

    # --- WRITE METHODS ---
    async def adump_snapshot(self, snapshot: messages.StateSnapshot) -> None:
        """Store a full snapshot of all states at a given revision."""
        target_session = snapshot.session_id or self.current_session_id
        epoch_ms = dt_to_epoch_ms(datetime.now(UTC))
        async with self._write_lock:
            async with aiosqlite.connect(self.db_path) as db:
                for state_id, state_data in snapshot.snapshots.items():
                    await db.execute(
                        """
                        INSERT OR IGNORE INTO state_snapshots (
                            state_id, global_revision, event_time, session_id, state_data
                        ) 
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            state_id,
                            snapshot.global_rev,
                            epoch_ms,
                            target_session,
                            json.dumps(state_data),
                        ),
                    )
                await db.commit()

    async def awrite_update(self, change: StateChange) -> None:
        """Write one update of a state to the store: one row for its revision."""
        global_current_rev = change.global_rev - 1
        global_future_rev = change.global_rev

        target_session = change.session_id or self.current_session_id
        epoch_ms = dt_to_epoch_ms(datetime.fromtimestamp(change.ts, tz=UTC))

        async with self._write_lock:
            async with aiosqlite.connect(self.db_path) as db:
                try:
                    # ``op`` and ``path`` are what a row held before a row was an update;
                    # they cannot be dropped from files written then, so they say so.
                    await db.execute(
                        """
                        INSERT INTO state_patches (
                            state_id, global_current_rev, global_future_rev, event_time, 
                            correlation_id, session_id, op, path, value, ops
                        ) 
                        VALUES (?, ?, ?, ?, ?, ?, 'update', '', NULL, ?)
                    """,
                        (
                            change.state_name,
                            global_current_rev,
                            global_future_rev,
                            epoch_ms,
                            change.task_id,
                            target_session,
                            json.dumps(change.ops),
                        ),
                    )
                    await db.commit()
                except aiosqlite.IntegrityError as e:
                    raise RuntimeError(
                        f"Database Integrity Violation on patch {global_current_rev}->{global_future_rev}: {e}"
                    ) from e

    async def awrite_journal(self, entries: Sequence[JournalEntry]) -> None:
        """Persist journal entries in one transaction. A re-sent entry is a no-op."""
        if not entries:
            return
        rows = [
            (
                entry.session_id,
                entry.pos,
                entry.global_rev,
                entry.event_time,
                entry.kind,
                entry.task_id,
                entry.action_key,
                entry.subject,
                entry.message_id,
                json.dumps(entry.payload, separators=(",", ":")),
                entry.step,
            )
            for entry in entries
        ]
        async with self._write_lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.executemany(
                    """
                    INSERT OR IGNORE INTO journal (
                        session_id, pos, global_rev, event_time, kind,
                        task_id, action_key, subject, message_id, payload, step
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                await db.commit()

    async def ateardown(self) -> None:
        """Cleans up resources, such as the background processing task."""
        return None

    async def is_cought_up_to(self, revision: int) -> bool:
        """Returns True if the sink has received patches/snapshots up to at least the given revision for the specified state_id."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT MAX(global_future_rev) FROM state_patches
                """
            ) as cursor:
                row = await cursor.fetchone()
                max_future_rev = row[0] if row and row[0] is not None else 0
                return max_future_rev >= revision
