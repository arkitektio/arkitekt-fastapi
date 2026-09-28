"""Journal route builders: every report in order, and the world at any position.

See ``docs/journal.md``. ``{session_id}`` may be ``current``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Path, Query
from fastapi.responses import JSONResponse

from arkitekt_runtime.agents.journal import FLUSH_TIMEOUT, Fold, JournalReader
from arkitekt_fastapi.agent import FastApiAgent

from .common import normalize_filter_values


def _detail(status_code: int, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"detail": detail})


def _parse_timestamp(raw: str) -> int | None:
    """``?timestamp=`` as epoch milliseconds or RFC 3339."""
    raw = raw.strip()
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        moment = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return int(moment.timestamp() * 1000)


def build_journal_router(
    agent: FastApiAgent,
    tasks_path: str = "/tasks",
) -> APIRouter:
    """Build the journal routes (``/journal…`` and ``{tasks_path}/{task_id}/events``)."""
    router = APIRouter(tags=["Journal"])

    def reader() -> JournalReader | None:
        retriever = agent.retriever
        return retriever if isinstance(retriever, JournalReader) else None

    def resolve(session_id: str) -> str:
        return agent.current_session if session_id == "current" else session_id

    async def flush_if_current(session: str, pos: int | None = None) -> None:
        """Wait (bounded) until the journal is persisted up to what is read."""
        watermark = agent.journal.watermark()
        if watermark is not None and watermark.session_id == session:
            # Never beyond the watermark: nothing more is coming to wait for.
            await agent.journal.aflush_to(
                watermark.pos if pos is None else min(pos, watermark.pos), FLUSH_TIMEOUT
            )

    async def world_response(session: str, pos: int) -> JSONResponse:
        store = reader()
        if store is None:
            return _detail(404, "This agent does not keep a journal")
        await flush_if_current(session, pos)
        found = await store.aget_journal_world(session, pos)
        if found is None:
            return _detail(404, "No journal entry at that position")
        entry, fold = found
        world = fold.world()
        world["session_id"] = session
        world["pos"] = entry.pos
        world["global_rev"] = entry.global_rev
        world["timepoint"] = entry.timepoint
        world["entry"] = entry.to_json()
        return JSONResponse(content=world)

    async def journal_info() -> Any:  # noqa: ANN401
        """The journal's current position: ``{session_id, pos, global_rev}``."""
        watermark = agent.journal.watermark()
        if watermark is None:
            return _detail(404, "No active session")
        return watermark.to_json()

    async def journal_entries(
        session_id: str,
        after: int = Query(default=0, ge=0),
        until: int | None = Query(default=None, ge=0),
        limit: int = Query(default=1000, ge=1),
        kinds: list[str] | None = Query(default=None),
        task_id: str | None = Query(default=None),
        action_keys: list[str] | None = Query(default=None),
        state_keys: list[str] | None = Query(default=None),
        lock_keys: list[str] | None = Query(default=None),
    ) -> Any:  # noqa: ANN401
        """Journal entries in ``pos`` order. Key filters route like the websocket:
        patches by state, locks by key, session-wide entries always, the rest by
        action key."""
        store = reader()
        if store is None:
            return _detail(404, "This agent does not keep a journal")
        session = resolve(session_id)
        await flush_if_current(session)
        entries = await store.aget_journal_entries(
            session,
            after=after,
            until=until,
            limit=limit,
            kinds=normalize_filter_values(kinds),
            task_id=task_id,
            action_keys=normalize_filter_values(action_keys),
            state_keys=normalize_filter_values(state_keys),
            lock_keys=normalize_filter_values(lock_keys),
        )
        return {
            "session_id": session,
            "after": after,
            "last_pos": entries[-1].pos if entries else None,
            "entries": [entry.to_json() for entry in entries],
        }

    async def journal_at(session_id: str, pos: int = Path(ge=1)) -> Any:  # noqa: ANN401
        """States, tasks and locks as of a position."""
        return await world_response(resolve(session_id), pos)

    async def journal_at_time(session_id: str, timestamp: str = Query()) -> Any:  # noqa: ANN401
        """States, tasks and locks as of the last entry at or before a time
        (epoch milliseconds or RFC 3339)."""
        ms = _parse_timestamp(timestamp)
        if ms is None:
            return JSONResponse(
                status_code=422,
                content={
                    "detail": [
                        {
                            "type": "datetime_parsing",
                            "loc": ["query", "timestamp"],
                            "msg": "Input should be a valid datetime",
                            "input": timestamp,
                        }
                    ]
                },
            )
        store = reader()
        if store is None:
            return _detail(404, "This agent does not keep a journal")
        session = resolve(session_id)
        await flush_if_current(session)
        pos = await store.aget_journal_pos_at_time(session, ms)
        if pos is None:
            return _detail(404, "No journal entry at or before that time")
        return await world_response(session, pos)

    async def task_events(task_id: str) -> Any:  # noqa: ANN401
        """Every journal entry of a task, and the task folded from them. Works after
        the task has ended, unlike ``/tasks/{task_id}``."""
        store = reader()
        if store is None:
            return _detail(404, "This agent does not keep a journal")
        await agent.journal.aflush(FLUSH_TIMEOUT)
        entries = await store.aget_journal_task_entries(task_id)
        if not entries:
            return JSONResponse(
                status_code=404,
                content={"error": "Task not found", "task_id": task_id},
            )
        task = Fold.from_entries(entries).tasks.get(task_id)
        return {
            "task_id": task_id,
            "task": task.to_json() if task is not None else None,
            "entries": [entry.to_json() for entry in entries],
        }

    router.add_api_route(
        "/journal",
        journal_info,
        methods=["GET"],
        summary="The journal's current position",
    )
    router.add_api_route(
        "/journal/{session_id}",
        journal_entries,
        methods=["GET"],
        summary="Journal entries in order",
    )
    router.add_api_route(
        "/journal/{session_id}/at",
        journal_at_time,
        methods=["GET"],
        summary="States, tasks and locks at a time",
    )
    router.add_api_route(
        "/journal/{session_id}/at/{pos}",
        journal_at,
        methods=["GET"],
        summary="States, tasks and locks at a position",
    )
    router.add_api_route(
        f"{tasks_path}/{{task_id}}/events",
        task_events,
        methods=["GET"],
        tags=["Journal", "Tasks"],
        summary="Every journal entry of a task",
    )
    return router
