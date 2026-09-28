"""A served app refuses to start without the app context it declares."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from arkitekt_fastapi.routes import configure_fastapi
from arkitekt_spec.declare.app import AppRegistry
from arkitekt_spec.declare.errors import AppContextError


class Config:
    def __init__(self, label: str = "cfg") -> None:
        self.label = label


def test_a_served_app_fails_at_startup_without_its_context(tmp_path) -> None:  # noqa: ANN001
    registry = AppRegistry()
    registry.app_context(Config)
    app = FastAPI()
    configure_fastapi(app=app, app_registry=registry, db_file=str(tmp_path / "c.db"))

    with pytest.raises(AppContextError, match="none was given"):
        with TestClient(app):
            pass
