import aiosqlite


async def ensure_sqlite_schema(db: aiosqlite.Connection) -> None:
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL
        );
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS state_snapshots (
            state_id TEXT NOT NULL,
            global_revision INTEGER NOT NULL,
            event_time INTEGER NOT NULL,
            session_id TEXT NOT NULL,
            state_data TEXT NOT NULL,
            PRIMARY KEY (state_id, global_revision, session_id),
            FOREIGN KEY (session_id) REFERENCES sessions(session_id)
        );
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS state_patches (
            state_id TEXT NOT NULL,
            global_current_rev INTEGER NOT NULL,
            global_future_rev INTEGER NOT NULL,
            event_time INTEGER NOT NULL,
            correlation_id TEXT,
            session_id TEXT NOT NULL,
            op TEXT NOT NULL,
            path TEXT NOT NULL,
            value TEXT,
            PRIMARY KEY (state_id, global_current_rev, session_id),
            FOREIGN KEY (session_id) REFERENCES sessions(session_id),
            CHECK (global_future_rev = global_current_rev + 1)
        );
        """
    )

    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_patches_state_time ON state_patches(state_id, event_time);"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_snapshots_state_time ON state_snapshots(state_id, event_time);"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_patches_correlation ON state_patches(state_id, correlation_id);"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_patches_session ON state_patches(state_id, session_id);"
    )

    # The agent's journal (docs/journal.md): every report in one order.
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS journal (
            session_id TEXT NOT NULL,
            pos INTEGER NOT NULL,
            global_rev INTEGER NOT NULL,
            event_time INTEGER NOT NULL,
            kind TEXT NOT NULL,
            task_id TEXT,
            action_key TEXT,
            subject TEXT,
            message_id TEXT NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY (session_id, pos),
            FOREIGN KEY (session_id) REFERENCES sessions(session_id)
        );
        """
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_journal_task ON journal(task_id, session_id, pos);"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_journal_time ON journal(session_id, event_time);"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_journal_kind ON journal(session_id, kind, pos);"
    )

    await ensure_column(
        db,
        table_name="state_snapshots",
        column_name="global_revision",
        column_definition="INTEGER NOT NULL DEFAULT 0",
    )
    await ensure_column(
        db,
        table_name="state_patches",
        column_name="global_current_rev",
        column_definition="INTEGER NOT NULL DEFAULT 0",
    )
    await ensure_column(
        db,
        table_name="state_patches",
        column_name="global_future_rev",
        column_definition="INTEGER NOT NULL DEFAULT 0",
    )


async def ensure_column(
    db: aiosqlite.Connection,
    table_name: str,
    column_name: str,
    column_definition: str,
) -> None:
    async with db.execute(f"PRAGMA table_info({table_name})") as cursor:
        existing_columns = [row[1] for row in await cursor.fetchall()]

    if column_name not in existing_columns:
        await db.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"
        )
