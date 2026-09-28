"""The journal of a served agent: one gap-free order of everything it reports,
resumable over the websocket and replayable at any position.

Mirrors the Rust agent's ``tests/journal.rs``; the contract is ``docs/journal.md``.
"""

import json
import threading
import time
from collections.abc import Generator, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import anyio
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.testclient import WebSocketTestSession

from arkitekt_spec.declare.app import AppRegistry
from arkitekt_fastapi.agent import FastApiAgent
from arkitekt_fastapi.routes import configure_fastapi

TERMINAL = {"COMPLETED", "FAILED", "CRITICAL", "CANCELLED", "INTERRUPTED"}


def _registry(spun: threading.Event) -> AppRegistry:
    registry = AppRegistry()

    @registry.state(name="CameraState", required_locks=["camera"])
    @dataclass
    class CameraState:
        connected: bool = False
        exposure_ms: float = 10.0
        tags: list[str] = field(default_factory=list)

    @registry.startup
    def init_camera() -> CameraState:
        return CameraState(connected=True)

    def set_exposure(exposure_ms: float, camera: CameraState) -> float:
        """Set Exposure"""
        camera.exposure_ms = exposure_ms
        return exposure_ms

    def add_tag(tag: str, camera: CameraState) -> int:
        """Add Tag"""
        camera.tags.append(tag)
        return len(camera.tags)

    def count_up(until: int) -> Generator[int, None, None]:
        """Count Up"""
        yield from range(until)

    def explode() -> int:
        """Explode"""
        raise ValueError("boom")

    def spin(camera: CameraState) -> int:
        """Change the camera (from a worker thread) until cancelled."""
        n = 0
        stop_at = time.monotonic() + 20
        while time.monotonic() < stop_at:
            camera.exposure_ms += 1
            n += 1
            if n % 3 == 0:
                spun.set()
                time.sleep(0.0005)
        return n

    for function in (set_exposure, add_tag, count_up, explode, spin):
        registry.register(function)
    return registry


@pytest.fixture()
def served(tmp_path: Path) -> Iterator[tuple[TestClient, FastApiAgent, threading.Event]]:
    spun = threading.Event()
    app = FastAPI()
    agent = configure_fastapi(app, _registry(spun), db_file=str(tmp_path / "agent.db"))
    with TestClient(app) as client:
        _wait_for(lambda: agent.journal.watermark() is not None, "the session to start")
        yield client, agent, spun


def _wait_for(condition: Any, what: str, timeout: float = 10.0) -> None:  # noqa: ANN401
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(0.01)


async def _receive(ws: WebSocketTestSession, timeout: float) -> Any:  # noqa: ANN401
    with anyio.fail_after(timeout):
        return await ws._send_rx.receive()


def next_frame(ws: WebSocketTestSession, timeout: float = 10.0) -> dict[str, Any]:
    message = ws.portal.call(_receive, ws, timeout)
    return json.loads(message["text"])


def drain(
    ws: WebSocketTestSession, quiet: float = 0.3, at_most: float = 3.0
) -> list[dict[str, Any]]:
    """The frames arriving until it goes quiet (or ``at_most`` seconds have passed)."""
    frames = []
    deadline = time.monotonic() + at_most
    while time.monotonic() < deadline:
        try:
            frames.append(next_frame(ws, quiet))
        except TimeoutError:
            break
    return frames


def until_terminal(ws: WebSocketTestSession, task: str) -> list[dict[str, Any]]:
    """Frames up to ``task``'s end, plus what follows within a moment (UNLOCK)."""
    frames = []
    while True:
        frame = next_frame(ws)
        frames.append(frame)
        if frame.get("task") == task and frame["type"] in TERMINAL:
            break
    return frames + drain(ws)


def connect(client: TestClient, init: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
    session = client.websocket_connect("/ws")
    ws = session.__enter__()
    ws.send_text(json.dumps(init))
    first = next_frame(ws)
    assert first["type"] == "INIT"
    return session, first


def assign(client: TestClient, interface: str, args: dict[str, Any]) -> str:
    response = client.post(f"/assign/{interface}", json={"args": args})
    assert response.status_code == 200, response.text
    return response.json()["task"]


def positions(frames: list[dict[str, Any]]) -> list[int]:
    return [frame["pos"] for frame in frames]


def kinds(frames: list[dict[str, Any]]) -> list[str]:
    return [frame["type"] for frame in frames]


def assert_contiguous(pos: list[int]) -> None:
    assert pos == list(range(pos[0], pos[0] + len(pos))), (
        f"positions have no gaps and no repeats: {pos}"
    )


def test_one_order_for_tasks_and_state_and_replay_at_any_position(served) -> None:  # noqa: ANN001
    client, agent, _ = served
    ws, init = connect(client, {"type": "INIT", "journal": True})
    legacy, legacy_init = connect(client, {"type": "INIT"})
    try:
        session = init["journal"]["session_id"]
        assert init["journal"]["pos"] == 1, "SESSION_INIT is the first entry"
        assert init["journal"]["states"]["CameraState"]["exposure_ms"] == 10.0
        assert "journal" not in legacy_init
        assert set(legacy_init) == {"type", "tasks", "states", "locks"}

        task = assign(client, "set_exposure", {"exposure_ms": 20.0})
        frames = until_terminal(ws, task)
        assert kinds(frames) == [
            "ASSIGN",
            "PROGRESS",
            "LOCK",
            "STATE_PATCH",
            "YIELD",
            "COMPLETED",
            "UNLOCK",
        ]
        pos = positions(frames)
        assert pos[0] == 2
        assert_contiguous(pos)
        assert all(frame["journal_session"] == session for frame in frames)
        assert "token" not in frames[0]

        # The legacy subscriber gets the frames it always got: no ASSIGN, no positions.
        legacy_frames = until_terminal(legacy, task)
        assert kinds(legacy_frames) == ["PROGRESS", "LOCK", "STATE_PATCH", "YIELD", "COMPLETED", "UNLOCK"]
        assert all("pos" not in f and "journal_session" not in f for f in legacy_frames)
        assert [f["id"] for f in legacy_frames] == [f["id"] for f in frames[1:]], (
            "the same messages, with the same ids"
        )
        assert [f.get("seq") for f in legacy_frames] == [f.get("seq") for f in frames[1:]]
        for plain, stamped in zip(legacy_frames, frames[1:]):
            assert {k: v for k, v in stamped.items() if k not in ("pos", "journal_session")} == plain

        # The stored journal is the same sequence.
        listing = client.get("/journal/current").json()
        entries = listing["entries"]
        assert len(entries) == 8
        assert_contiguous([e["pos"] for e in entries])
        assert entries[0]["kind"] == "SESSION_INIT"
        assert (entries[7]["kind"], entries[7]["task_id"]) == ("UNLOCK", task)
        assert listing["last_pos"] == 8
        assert client.get("/journal").json() == {"session_id": session, "pos": 8, "global_rev": 1}

        # Time travel: before the patch, between the end and the unlock, after.
        lock_pos, done_pos = frames[2]["pos"], frames[5]["pos"]
        at = client.get(f"/journal/{session}/at/{lock_pos}").json()
        assert at["states"]["CameraState"]["exposure_ms"] == 10.0
        assert at["global_rev"] == 0
        assert at["tasks"][task]["status"] == "RUNNING"
        assert at["locks"]["camera"] == task
        at = client.get(f"/journal/{session}/at/{done_pos}").json()
        assert at["states"]["CameraState"]["exposure_ms"] == 20.0
        assert at["global_rev"] == 1
        assert at["tasks"][task]["status"] == "COMPLETED"
        assert at["tasks"][task]["last_returns"]["return0"] == 20.0
        assert at["locks"]["camera"] == task, "UNLOCK comes after the end"
        assert at["entry"]["kind"] == "COMPLETED"
        at = client.get(f"/journal/{session}/at/{done_pos + 1}").json()
        assert at["locks"] == {}
        assert client.get(f"/journal/{session}/at/99").status_code == 404

        now = int(time.time() * 1000) + 1000
        assert client.get(f"/journal/{session}/at?timestamp={now}").json()["pos"] == done_pos + 1
        assert client.get(f"/journal/{session}/at?timestamp=nope").status_code == 422
        assert client.get(f"/journal/{session}/at?timestamp=2000-01-01T00:00:00Z").status_code == 404

        events = client.get(f"/tasks/{task}/events").json()
        assert events["task"]["status"] == "COMPLETED"
        assert [e["kind"] for e in events["entries"]] == [
            "ASSIGN",
            "PROGRESS",
            "LOCK",
            "STATE_PATCH",
            "YIELD",
            "COMPLETED",
            "UNLOCK",
        ]
        assert client.get("/tasks/nope/events").status_code == 404

        assert client.get("/session_info").json()["current_pos"] == 8

        # Key filters route like the websocket.
        only_locks = client.get("/journal/current", params={"lock_keys": "camera", "action_keys": "none", "state_keys": "none"}).json()
        assert [e["kind"] for e in only_locks["entries"]] == ["SESSION_INIT", "LOCK", "UNLOCK"]
        patches = client.get("/journal/current", params={"kinds": "STATE_PATCH"}).json()
        assert [e["kind"] for e in patches["entries"]] == ["STATE_PATCH"]
        page = client.get("/journal/current", params={"after": 2, "limit": 2}).json()
        assert [e["pos"] for e in page["entries"]] == [3, 4]
    finally:
        ws.__exit__(None, None, None)
        legacy.__exit__(None, None, None)


def test_delivery_order_is_journal_order_under_concurrency(served) -> None:  # noqa: ANN001
    client, agent, _ = served
    ws, init = connect(client, {"type": "INIT", "journal": True})
    try:
        start_pos = init["journal"]["pos"]
        ids: list[str] = []
        lock = threading.Lock()

        def submit(i: int) -> None:
            task = (
                assign(client, "add_tag", {"tag": f"t{i}"})
                if i % 2 == 0
                else assign(client, "count_up", {"until": 5})
            )
            with lock:
                ids.append(task)

        threads = [threading.Thread(target=submit, args=(i,)) for i in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        frames: list[dict[str, Any]] = []
        ended = 0
        while ended < len(ids):
            frame = next_frame(ws)
            if frame["type"] in TERMINAL:
                ended += 1
            frames.append(frame)
        frames.extend(drain(ws))
        pos = positions(frames)
        assert pos[0] == start_pos + 1
        assert_contiguous(pos)

        # Per task: ASSIGN first, the end last (its UNLOCK carries no task).
        for task in ids:
            own = [f for f in frames if f.get("task") == task or f.get("task_id") == task]
            assert own[0]["type"] == "ASSIGN"
            end = next(i for i, f in enumerate(own) if f["type"] == "COMPLETED")
            assert end == len(own) - 1, "nothing of a task after its end"
            # A task's patches come before its YIELD.
            kinds_of_task = kinds(own)
            if "STATE_PATCH" in kinds_of_task:
                assert kinds_of_task.index("STATE_PATCH") < kinds_of_task.index("YIELD")
        revs = [f["global_rev"] for f in frames if f["type"] == "STATE_PATCH"]
        assert revs == list(range(1, len(revs) + 1))
    finally:
        ws.__exit__(None, None, None)


def test_resume_after_a_disconnect(served) -> None:  # noqa: ANN001
    client, agent, _ = served
    ws, init = connect(client, {"type": "INIT", "journal": True})
    session = init["journal"]["session_id"]
    task = assign(client, "add_tag", {"tag": "a"})
    seen = until_terminal(ws, task)
    last = positions(seen)[-1]
    ws.__exit__(None, None, None)

    # Missed while away.
    missed = assign(client, "count_up", {"until": 3})

    def missed_done() -> bool:
        with agent.journal.locked() as view:
            folded = view.fold.tasks.get(missed)
            return folded is not None and folded.done

    _wait_for(missed_done, "the missed task to end")
    time.sleep(0.05)
    watermark = agent.journal.watermark()
    assert watermark is not None

    ws, init = connect(
        client, {"type": "INIT", "journal": True, "resume_after": last, "session_id": session}
    )
    try:
        assert init["journal"]["resync"] is False
        assert init["journal"]["pos"] == watermark.pos
        assert init["journal"]["tasks"][missed]["yields"] == 3
        frames = [next_frame(ws) for _ in range(last, watermark.pos)]
        assert frames[0]["type"] == "ASSIGN"
        assert frames[0]["task"] == missed
        assert all("seq" not in f for f in frames), "replayed frames have no stream seq"

        # Then live, continuing the same order.
        live = assign(client, "explode", {})
        frames.extend(until_terminal(ws, live))
        pos = positions(frames)
        assert pos[0] == last + 1
        assert_contiguous(pos)
        assert frames[-1]["type"] == "CRITICAL"
    finally:
        ws.__exit__(None, None, None)

    # Another session's position cannot be resumed.
    ws, init = connect(
        client, {"type": "INIT", "journal": True, "resume_after": 1, "session_id": "elsewhere"}
    )
    ws.__exit__(None, None, None)
    assert init["journal"]["resync"] is True

    # Nor can a position without its session: it may be from before a restart.
    ws, init = connect(client, {"type": "INIT", "journal": True, "resume_after": 1})
    try:
        assert init["journal"]["resync"] is True
        assert drain(ws) == [], "a resync comes with no backlog"
    finally:
        ws.__exit__(None, None, None)

    # Nor one ahead of the watermark.
    ws, init = connect(
        client,
        {"type": "INIT", "journal": True, "resume_after": 10_000, "session_id": session},
    )
    ws.__exit__(None, None, None)
    assert init["journal"]["resync"] is True


def test_resume_from_storage_beyond_memory(served) -> None:  # noqa: ANN001
    client, agent, _ = served
    agent.journal.set_ring_capacity(8)
    task = assign(client, "count_up", {"until": 30})

    def done() -> bool:
        with agent.journal.locked() as view:
            folded = view.fold.tasks.get(task)
            return folded is not None and folded.done

    _wait_for(done, "the task to end")
    time.sleep(0.05)
    watermark = agent.journal.watermark()
    assert watermark is not None and watermark.pos > 8

    # From the very start, no session needed.
    ws, init = connect(client, {"type": "INIT", "journal": True, "resume_after": 0})
    try:
        assert init["journal"]["resync"] is False
        frames = [next_frame(ws) for _ in range(watermark.pos)]
        pos = positions(frames)
        assert pos[0] == 1 and pos[-1] == watermark.pos
        assert_contiguous(pos)
        assert frames[0]["type"] == "SESSION_INIT"
    finally:
        ws.__exit__(None, None, None)

    # Filters apply to the backlog too.
    ws, init = connect(
        client, {"type": "INIT", "journal": True, "resume_after": 0, "action_keys": ["nothing"]}
    )
    try:
        backlog = drain(ws)
        assert kinds(backlog) == ["SESSION_INIT"]
        assert init["journal"]["tasks"] == {}
    finally:
        ws.__exit__(None, None, None)


def test_nothing_of_a_cancelled_task_after_its_end(served) -> None:  # noqa: ANN001
    client, agent, spun = served
    ws, init = connect(client, {"type": "INIT", "journal": True})
    try:
        session = init["journal"]["session_id"]
        task = assign(client, "spin", {})
        patches = 0
        while patches < 50:
            if next_frame(ws)["type"] == "STATE_PATCH":
                patches += 1
        assert spun.is_set()
        client.post("/cancel", json={"task": task})
        frames = until_terminal(ws, task)
        cancelled = kinds(frames).index("CANCELLED")
        assert kinds(frames[cancelled + 1 :]) == ["UNLOCK"], (
            "only the lock release follows the end"
        )

        assert agent.journal.watermark() is not None
        listing = client.get(
            f"/journal/{session}", params={"kinds": "STATE_PATCH", "limit": 100000}
        ).json()
        revs = [e["global_rev"] for e in listing["entries"]]
        assert revs == list(range(1, len(revs) + 1)), "no revision was skipped"
        assert all(e["task_id"] == task for e in listing["entries"])

        # The live state is exactly the recorded one.
        time.sleep(0.1)
        last = agent.journal.watermark()
        assert last is not None
        assert agent.global_revision == last.global_rev
        at = client.get(f"/journal/{session}/at/{last.pos}").json()
        live = client.get("/states").json()["states"]["CameraState"]["value"]
        assert at["states"]["CameraState"] == live
        assert at["tasks"][task]["status"] == "CANCELLED"
    finally:
        ws.__exit__(None, None, None)


def test_the_world_at_a_position_counts_every_patch_once(served) -> None:  # noqa: ANN001
    """A snapshot at N contains the patch reaching N, which follows it on the wire."""
    client, agent, _ = served
    agent.snapshot_interval = 2
    ws, init = connect(client, {"type": "INIT", "journal": True})
    try:
        for i in range(5):
            until_terminal(ws, assign(client, "add_tag", {"tag": f"t{i}"}))
        watermark = agent.journal.watermark()
        assert watermark is not None
        live = client.get("/states").json()["states"]["CameraState"]["value"]
        assert live["tags"] == [f"t{i}" for i in range(5)]
        at = client.get(f"/journal/current/at/{watermark.pos}").json()
        assert at["states"]["CameraState"] == live
        with agent.journal.locked() as view:
            assert view.fold.states["CameraState"] == live
        snapshots = client.get("/journal/current", params={"kinds": "STATE_SNAPSHOT"}).json()
        assert [e["global_rev"] for e in snapshots["entries"]] == [2, 4]
    finally:
        ws.__exit__(None, None, None)
