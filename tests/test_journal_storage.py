"""The served journal's storage: the in-memory and sqlite journals answer alike.

The served journal end to end is ``test_fastapi_journal``.
"""


import pytest
from arkitekt_runtime import messages
from arkitekt_runtime.agents.journal import (
    Journal,
    JournalEntry,
)


def _session_init(session: str) -> messages.SessionInit:
    return messages.SessionInit(session_id=session, states={"Camera": {"exposure": 1, "tags": []}})


def _patch(rev: int, task: str | None, op: str = "replace", path: str = "/exposure", value: object = None) -> messages.StatePatch:
    return messages.StatePatch(
        session_id="s",
        global_rev=rev,
        state_name="Camera",
        ts=0.0,
        op=op,
        path=path,
        value=rev + 1 if value is None else value,
        old_value=None,
        task_id=task,
    )


def _assign(task: str, token: str | None = "secret") -> messages.Assign:
    return messages.Assign(
        interface="set",
        task=task,
        reference="r",
        args={},
        user="u",
        org="o",
        action="a",
        implementation="i",
        token=token,
    )


# ------------------------------------------------------------------ storage --


def _scenario() -> tuple[list[JournalEntry], Journal]:
    """Two tasks, a lock, list appends across a snapshot, and a second session."""
    recorded: list[JournalEntry] = []
    journal = Journal()
    journal.add_listener(lambda entry, message: recorded.append(entry))
    journal.append(_session_init("s"))
    journal.record_assign(_assign("t"), "set")
    journal.record_assign(_assign("u").model_copy(update={"interface": "other"}), "other")
    journal.append(messages.Progress(task="t", progress=5), "set")
    journal.append(messages.Lock(key="cam", task="t"))
    journal.append(_patch(1, "t", op="add", path="/tags/-", value="a"))
    journal.append(messages.Log(task="u", message="hi"), "other")
    journal.append(
        messages.StateSnapshot(
            session_id="s", global_rev=2, snapshots={"Camera": {"exposure": 1, "tags": ["a", "b"]}}
        )
    )
    journal.append(_patch(2, "t", op="add", path="/tags/-", value="b"))
    journal.append(messages.Yield(task="t", returns={"return0": 1}), "set")
    journal.append(messages.Completed(task="t"), "set")
    journal.append(messages.Unlock(key="cam"))
    journal.append(messages.Critical(task="orphan", error="x"))
    journal.append(messages.Failed(task="u", error="nope"), "other")
    journal.append(_session_init("s2"))
    journal.append(messages.Log(task="t", message="later"))
    return recorded, journal


@pytest.mark.asyncio
async def test_the_memory_and_sqlite_journals_answer_alike(tmp_path) -> None:  # noqa: ANN001
    from arkitekt_fastapi.retriever.memory_retriever import MemoryRetriever
    from arkitekt_fastapi.sink.memory_sink import MemorySink
    from arkitekt_fastapi.sql_lite.retriever import SQLLiteRetriever
    from arkitekt_fastapi.sql_lite.sink import SQLLiteSink

    entries, journal = _scenario()
    memory_sink = MemorySink()
    memory = MemoryRetriever(memory_sink.store)
    sqlite_sink = SQLLiteSink(db_path=str(tmp_path / "journal.db"))
    sqlite = SQLLiteRetriever(db_path=str(tmp_path / "journal.db"))
    await sqlite_sink.ainitialize()
    await sqlite.ainitialize()
    for sink in (memory_sink, sqlite_sink):
        await sink.awrite_journal(entries[:7])
        await sink.awrite_journal(entries[5:])  # a re-sent entry is a no-op

    def dump(found: list[JournalEntry]) -> list[dict[str, object]]:
        return [entry.to_json() for entry in found]

    queries: list[dict[str, object]] = [
        {},
        {"after": 3, "until": 9},
        {"limit": 4},
        {"kinds": ["STATE_PATCH", "LOCK"]},
        {"task_id": "t"},
        {"action_keys": ["set"]},
        {"action_keys": ["other"], "state_keys": ["Nope"], "lock_keys": ["cam"]},
        {"state_keys": ["Camera"]},
    ]
    for query in queries:
        expected = dump(await sqlite.aget_journal_entries("s", **query))  # type: ignore[arg-type]
        assert dump(await memory.aget_journal_entries("s", **query)) == expected, query  # type: ignore[arg-type]
        assert expected, query
    only_other = dump(
        await sqlite.aget_journal_entries(
            "s", action_keys=["other"], state_keys=["Nope"], lock_keys=["nope"]
        )
    )
    assert [e["kind"] for e in only_other] == [
        "SESSION_INIT",
        "ASSIGN",
        "LOG",
        "STATE_SNAPSHOT",
        "CRITICAL",
        "FAILED",
    ], "session-wide entries and reports without an action key always pass"

    assert dump(await memory.aget_journal_task_entries("t")) == dump(
        await sqlite.aget_journal_task_entries("t")
    )
    assert [e.session_id for e in await sqlite.aget_journal_task_entries("t")][-1] == "s2"

    last = entries[-3]  # the last entry of session "s"
    for ms in (0, entries[0].event_time, last.event_time + 1):
        assert await memory.aget_journal_pos_at_time("s", ms) == await sqlite.aget_journal_pos_at_time("s", ms)
    assert await sqlite.aget_journal_pos_at_time("s", last.event_time + 1) == last.pos

    for pos in range(1, last.pos + 2):
        in_memory = await memory.aget_journal_world("s", pos)
        stored = await sqlite.aget_journal_world("s", pos)
        if pos > last.pos:
            assert in_memory is None and stored is None
            continue
        assert in_memory is not None and stored is not None
        assert in_memory[0].to_json() == stored[0].to_json()
        assert in_memory[1].world() == stored[1].world(), pos

    _, world = await sqlite.aget_journal_world("s", last.pos)  # type: ignore[misc]
    assert world.states["Camera"]["tags"] == ["a", "b"], "the snapshot's patch counts once"
    assert world.locks == {}
    assert world.tasks["t"].status == "COMPLETED" and world.tasks["u"].status == "FAILED"
    assert world.tasks["u"].interface == "other"
