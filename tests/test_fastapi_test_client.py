"""The sync test client reports the task id whichever assign route it uses."""

from pathlib import Path

import pytest
from fastapi import FastAPI

from arkitekt_spec.declare.app import AppRegistry
from arkitekt_fastapi.routes import configure_fastapi
from arkitekt_fastapi.testing import AgentTestClient


def _build_app(tmp_path: Path) -> FastAPI:
    registry = AppRegistry()

    def echo(item: str) -> str:
        """Echo"""
        return item

    registry.register(echo)
    app = FastAPI()
    configure_fastapi(app, registry, db_file=str(tmp_path / "agent.db"))
    return app


@pytest.mark.parametrize("use_implementation_route", [True, False])
def test_assign_reports_the_task_id(tmp_path: Path, use_implementation_route: bool) -> None:
    with AgentTestClient(_build_app(tmp_path), as_user="tester") as client:
        result = client.assign(
            "echo", {"item": "hi"}, use_implementation_route=use_implementation_route
        )
    assert result.task_id
