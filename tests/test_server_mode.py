"""Server mode stands on the execution core alone, and says what it cannot do."""

import ast
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI

from arkitekt_fastapi.routes import configure_fastapi
from arkitekt_fastapi.testing import AsyncAgentTestClient
from arkitekt_spec.declare.app import AppRegistry
from arkitekt_spec.declare.task import Task

DISTRIBUTED = {"rekuest", "rath", "websockets", "graphql", "fakts"}


def test_serving_loads_nothing_of_the_distributed_runtime() -> None:
    """Checked in a subprocess: the test session may have loaded anything."""
    code = (
        "import sys, arkitekt_fastapi, arkitekt_fastapi.testing, arkitekt_fastapi.sql_lite.sink, "
        "arkitekt_fastapi.sql_lite.retriever; "
        "print(sorted({m.split('.')[0] for m in sys.modules}))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert not set(ast.literal_eval(out.stdout.strip())) & DISTRIBUTED


async def calls_another(task: Task) -> str:
    """Calls another action"""
    await task.acall("somewhere", {})  # type: ignore[attr-defined]
    return "never"


@pytest.mark.asyncio
async def test_calling_another_action_fails_at_once(tmp_path: Path) -> None:
    """A served app has no one to route a call through, so the task fails instead of hanging."""
    registry = AppRegistry()
    registry.register(calls_another)
    app = FastAPI()
    configure_fastapi(app, registry, db_file=str(tmp_path / "agent.db"))

    async with AsyncAgentTestClient(app, as_user="tester") as client:
        result = await client.assign("calls_another", {})
        events = await client.collect_until_error(result.task_id, timeout=5)

    assert events and events[-1].event_type == "CRITICAL", [e.event_type for e in events]
    assert "cannot call other actions" in events[-1].data["error"]
