"""The FastAPI runtime serves a snapshot, as ``run(app)`` does.

When the app starts, the agent's registry is replaced by a validated, frozen
copy: the declaration is left alone, what is served cannot change under a
running server, and a declaration that does not validate fails at startup
rather than at the first assignment.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from arkitekt_spec.declare.app import AppRegistry
from arkitekt_fastapi.routes import configure_fastapi
from arkitekt_fastapi.testing import AsyncAgentTestClient
from arkitekt_spec.declare.errors import RegistryFrozenError
from arkitekt_spec.declare.structures.errors import StructureRegistryError


@pytest.mark.asyncio
async def test_the_agent_serves_a_frozen_copy_and_the_declaration_stays_open(
    tmp_path,  # noqa: ANN001
) -> None:
    registry = AppRegistry()
    app = FastAPI()
    agent = configure_fastapi(app=app, app_registry=registry, db_file=str(tmp_path / "a.db"))

    # Declared after configuring, before starting: still served.
    @registry.register
    def double(x: int) -> int:
        """Double."""
        return x * 2

    assert agent.app_registry is registry
    async with AsyncAgentTestClient(app, as_user="tester") as client:
        served = agent.app_registry
        assert served is not registry
        assert "double" in served.implementations
        def later(y: int) -> int:
            """Declared too late: the run serves what was declared when it started."""
            return y

        with pytest.raises(RegistryFrozenError):
            served.register(later)
        result = await client.assign("double", {"x": 5})
        events = await client.collect_until_done(result.task_id)
        assert any(e.is_done() for e in events)

    # The declaration is untouched and can be declared into again.
    @registry.register
    def triple(x: int) -> int:
        """Triple."""
        return x * 3

    assert "triple" in registry.implementations


def test_a_declaration_that_does_not_validate_fails_at_startup(
    tmp_path,  # noqa: ANN001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = AppRegistry()
    app = FastAPI()
    configure_fastapi(app=app, app_registry=registry, db_file=str(tmp_path / "b.db"))

    def refuse(self: AppRegistry) -> None:
        raise StructureRegistryError("This app is not valid: a port names nothing")

    monkeypatch.setattr(AppRegistry, "validate", refuse)

    with pytest.raises(StructureRegistryError, match="not valid"):
        with TestClient(app):
            pass


def test_without_a_lifespan_the_caller_owns_the_agent(tmp_path) -> None:  # noqa: ANN001
    """A runtime hands in its snapshot and drives the agent itself."""
    registry = AppRegistry()

    @registry.register
    def double(x: int) -> int:
        """Double."""
        return x * 2

    snapshot = registry.snapshot()
    app = FastAPI()

    class Bound:
        """What an agent is bound to: something answering the BoundApp protocol."""

        services: dict[str, object] = {}
        clients: dict[str, object] = {}

        def get(self, key: type) -> object | None:
            return None

    bound = Bound()
    agent = configure_fastapi(
        app=app, app_registry=snapshot, db_file=str(tmp_path / "c.db"), bound_app=bound, lifespan=False
    )

    assert agent.app_registry is snapshot and agent.bound_app is bound
    assert app.state.agent is agent
    assert any(getattr(route, "path", "") == "/schemas/states" for route in app.routes), "routes added now"
    assert app.router.lifespan_context is not None  # FastAPI's default, not ours
