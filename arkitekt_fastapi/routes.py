"""FastAPI route orchestration for the FastAPI runtime."""

from __future__ import annotations
from arkitekt_spec.actions import StateImplementationInput

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Callable
from typing import Any

from fastapi import APIRouter, FastAPI, Request

from arkitekt_spec.declare.app import AppRegistry
from arkitekt_fastapi.agent import FastApiAgent
from arkitekt_fastapi.detail_routes import (
    build_state_detail_router,
    build_task_detail_router,
)
from arkitekt_fastapi.openapi_utils import (
    configure_openapi,
    create_json_schema_from_ports,
    port_to_json_schema,
    register_router_custom_schemas,
)
from arkitekt_fastapi.route_groups import (
    add_implementation_route,
    build_core_router,
    build_implementation_router,
    build_journal_router,
    build_lock_router,
    build_schema_router,
    build_state_router,
    build_task_router,
)
from arkitekt_fastapi.auth import ExpandUserFromRequest

logger = logging.getLogger(__name__)


def _default_user_from_request(_: Request) -> str:
    return "anonymous"


def _include_router(app: FastAPI, router: APIRouter) -> None:
    app.include_router(router)
    register_router_custom_schemas(app, router)


def _log_provide_task_failure(task: "asyncio.Task[None]") -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("Error in agent provide task: %s", exc, exc_info=exc)


def create_lifespan(
    agent: FastApiAgent,
    app_context: Any | None = None,
    on_startup: Callable[[FastAPI], None] | None = None,
) -> Callable[[FastAPI], contextlib.AbstractAsyncContextManager[None]]:
    """Create a FastAPI lifespan manager for a configured agent.

    When the app starts, the agent takes a snapshot of its registry -- validated,
    copied and frozen -- and serves that, exactly as ``run(app)`` does: what a
    server offers is what was declared when it started, and a port naming a
    class the app cannot resolve fails here rather than mid-assignment. The
    declaration registry handed to ``configure_fastapi`` is left as it was, so
    it can be declared into up to the moment the app starts, and served again.

    Args:
        agent: The agent to enter and provide for the lifetime of the app.
        app_context: Optional context passed to ``agent.aprovide``.
        on_startup: Optional hook run once with the app before the agent is
            entered (used to register routes that depend on a started agent).
    """

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        agent.app_registry = agent.app_registry.snapshot()
        # Refused here, out of the lifespan, so a server started without the app
        # context its app declares does not start at all (the provide task's own
        # failure would only be logged).
        agent.app_registry.require_app_context(app_context, whose="This served app")
        if on_startup is not None:
            on_startup(app)
        app.state.agent = agent
        async with agent:
            provide_task = asyncio.create_task(agent.aprovide(context=app_context))
            provide_task.add_done_callback(_log_provide_task_failure)
            yield
            provide_task.cancel()
            try:
                await provide_task
            except asyncio.CancelledError:
                logger.info("Provide task cancelled during shutdown")
            except Exception as exc:
                logger.error(
                    "Error during provide task shutdown: %s", exc, exc_info=True
                )

    return lifespan


def add_agent_routes(
    app: FastAPI,
    agent: FastApiAgent,
    get_user_from_request: Callable[[Request], object] | None = None,
    expand_user_from_request: ExpandUserFromRequest | None = None,
    ws_path: str = "/ws",
    assign_path: str = "/assign",
    cancel_path: str = "/cancel",
    pause_path: str = "/pause",
    resume_path: str = "/resume",
    step_path: str = "/step",
) -> None:
    """Include core agent routes."""
    user_getter = get_user_from_request or _default_user_from_request
    if expand_user_from_request is not None and get_user_from_request is not None:
        logger.warning(
            "Both get_user_from_request and expand_user_from_request were given; "
            "expand_user_from_request wins."
        )
    _include_router(
        app,
        build_core_router(
            agent,
            user_getter,
            expand_user_from_request=expand_user_from_request,
            ws_path=ws_path,
            assign_path=assign_path,
            cancel_path=cancel_path,
            pause_path=pause_path,
            resume_path=resume_path,
            step_path=step_path,
        ),
    )


def add_task_routes(
    app: FastAPI,
    agent: FastApiAgent,
    tasks_path: str = "/tasks",
) -> None:
    """Include task overview routes."""
    _include_router(
        app,
        build_task_router(agent, tasks_path=tasks_path),
    )


def add_task_detail_routes(
    app: FastAPI,
    agent: FastApiAgent,
    tasks_path: str = "/tasks",
) -> None:
    """Include conditional task detail routes."""
    _include_router(app, build_task_detail_router(agent, tasks_path=tasks_path))


def add_journal_routes(
    app: FastAPI,
    agent: FastApiAgent,
    tasks_path: str = "/tasks",
) -> None:
    """Include the journal routes: every report in order, and the world at any position."""
    _include_router(app, build_journal_router(agent, tasks_path=tasks_path))


def add_state_routes(
    app: FastAPI,
    agent: FastApiAgent,
    states_path: str = "/states",
) -> None:
    """Include state overview routes."""
    _include_router(
        app,
        build_state_router(agent, states_path=states_path),
    )


def add_state_detail_routes(
    app: FastAPI,
    agent: FastApiAgent,
    states_path: str = "/states",
    state_schemas: dict[str, StateImplementationInput] | None = None,
) -> None:
    """Include conditional state detail and retriever routes."""
    state_schemas = state_schemas or agent.get_state_schemas()
    _include_router(
        app, build_state_detail_router(agent, state_schemas, states_path=states_path)
    )


def add_lock_routes(
    app: FastAPI,
    agent: FastApiAgent,
    locks_path: str = "/locks",
) -> None:
    """Include lock overview routes."""
    _include_router(
        app,
        build_lock_router(agent, locks_path=locks_path),
    )


def add_implementation_routes(
    app: FastAPI,
    agent: FastApiAgent,
    get_user_from_request: Callable[[Request], object] | None = None,
    expand_user_from_request: ExpandUserFromRequest | None = None,
) -> None:
    """Include all implementation execution routes.

    They authenticate with the same hooks as :func:`add_agent_routes`.
    """
    _include_router(
        app,
        build_implementation_router(
            agent,
            get_user_from_request=get_user_from_request,
            expand_user_from_request=expand_user_from_request,
        ),
    )


def add_schema_routes(
    app: FastAPI,
    agent: FastApiAgent,
) -> None:
    """Include schema inspection routes."""
    _include_router(app, build_schema_router(agent))


def configure_fastapi(
    app: FastAPI,
    app_registry: AppRegistry,
    get_user_from_request: Callable[[Request], object] | None = None,
    expand_user_from_request: ExpandUserFromRequest | None = None,
    add_implementations: bool = True,
    add_schema: bool = True,
    add_states: bool = True,
    add_state_details: bool = True,
    add_locks: bool = True,
    add_tasks: bool = True,
    add_task_details: bool = True,
    add_journal: bool = True,
    ws_path: str = "/ws",
    tasks_path: str = "/tasks",
    assign_path: str = "/assign",
    states_path: str = "/states",
    locks_path: str = "/locks",
    db_file: str = "agent_data.db",
    app_context: Any | None = None,
    bound_app: Any | None = None,
    lifespan: bool = True,
) -> FastApiAgent:
    """Configure a FastAPI app with a refactored set of agent route groups.

    With ``lifespan`` (the default) the agent serves a snapshot of
    ``app_registry`` taken when the app starts (see :func:`create_lifespan`), so
    ``app_registry`` itself is never frozen and may be declared into until then,
    and the agent is provided for the lifetime of the app.

    With ``lifespan=False`` nothing is installed on the app: ``app_registry`` is
    served as handed in (a runtime hands in its snapshot), every route is added
    now, and whoever called this enters and provides the agent -- what
    ``arkitekt.serve`` does, so the agent is the runtime's like any other.

    Args:
        bound_app: The running app the agent belongs to (a runtime), which
            supplies the clients actions ask for by annotation. ``None`` for an
            agent serving a registry that uses no services.
        lifespan: Whether to install the lifespan that snapshots, enters and
            provides the agent.
        expand_user_from_request: Unified authentication hook. It is handed a
            Starlette `Request` for HTTP routes and a `WebSocketSubscriptionInit`
            for the websocket handshake, so one callable covers both transports.
            Raise `AuthenticationError` to reject: HTTP answers 401, the websocket
            closes with code 1008.
        get_user_from_request: Deprecated, HTTP-only predecessor of
            `expand_user_from_request`. Still honoured when the newer hook is
            absent, in which case the websocket stays unauthenticated as before.
        add_journal: Serve the journal routes (``/journal…`` and
            ``{tasks_path}/{task_id}/events``, see ``docs/journal.md``).
    """
    # Imported here: the sqlite retriever reads the protocol under this
    # package, so a module-level import would be circular.
    from arkitekt_fastapi.sql_lite.retriever import SQLLiteRetriever
    from arkitekt_fastapi.sql_lite.sink import SQLLiteSink

    agent: FastApiAgent = FastApiAgent(  # type: ignore
        app_registry=app_registry,
        bound_app=bound_app,
        retriever=SQLLiteRetriever(db_path=db_file),
        sink=SQLLiteSink(db_path=db_file),
    )

    add_agent_routes(
        app,
        agent,
        get_user_from_request=get_user_from_request,
        expand_user_from_request=expand_user_from_request,
        ws_path=ws_path,
        assign_path=assign_path,
    )

    def _register_deferred_routes(fastapi_app: FastAPI) -> None:
        if add_tasks:
            add_task_routes(fastapi_app, agent, tasks_path=tasks_path)
        if add_task_details:
            add_task_detail_routes(fastapi_app, agent, tasks_path=tasks_path)
        if add_journal:
            add_journal_routes(fastapi_app, agent, tasks_path=tasks_path)
        if add_states:
            add_state_routes(fastapi_app, agent, states_path=states_path)
        if add_state_details:
            add_state_detail_routes(
                fastapi_app,
                agent,
                states_path=states_path,
                state_schemas=dict(agent.app_registry.states),
            )
        if add_locks:
            add_lock_routes(fastapi_app, agent, locks_path=locks_path)
        if add_implementations:
            add_implementation_routes(
                fastapi_app,
                agent,
                get_user_from_request=get_user_from_request,
                expand_user_from_request=expand_user_from_request,
            )
        if add_schema:
            add_schema_routes(fastapi_app, agent)
        configure_openapi(fastapi_app)

    if not lifespan:
        app.state.agent = agent
        _register_deferred_routes(app)
        return agent

    app.router.lifespan_context = create_lifespan(
        agent, app_context=app_context, on_startup=_register_deferred_routes
    )
    return agent


__all__ = [
    "add_agent_routes",
    "add_implementation_route",
    "add_implementation_routes",
    "add_journal_routes",
    "add_lock_routes",
    "add_schema_routes",
    "add_state_detail_routes",
    "add_state_routes",
    "add_task_detail_routes",
    "add_task_routes",
    "configure_fastapi",
    "configure_openapi",
    "create_json_schema_from_ports",
    "create_lifespan",
    "port_to_json_schema",
]
