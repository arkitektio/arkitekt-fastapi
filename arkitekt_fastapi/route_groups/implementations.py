"""Implementation route builders."""

from __future__ import annotations

from typing import Any
from collections.abc import Callable

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from arkitekt_fastapi.auth import (
    ExpandUserFromRequest,
    expand_http_user_or_401,
    resolve_expand_user_from_request,
)

from arkitekt_spec.actions import ImplementationInput
from arkitekt_fastapi.agent import FastApiAgent
from arkitekt_fastapi.openapi_utils import (
    create_json_schema_from_ports,
)


def add_implementation_route(
    router: APIRouter,
    agent: FastApiAgent,
    implementation: ImplementationInput,
    expand_user_from_request: ExpandUserFromRequest | None = None,
) -> None:
    """Register a single implementation execution route.

    With ``expand_user_from_request`` the route authenticates like the core assign
    routes; without one every caller runs as the ``"fastapi"`` user.
    """
    route_path = f"/{implementation.interface or implementation.definition.name}"
    request_schema_name = f"{implementation.definition.name}Request"
    response_schema_name = f"{implementation.definition.name}Response"
    args_schema_name = f"{implementation.definition.name}Args"
    args_schema = create_json_schema_from_ports(
        implementation.definition.args, args_schema_name
    )
    request_schema = {
        "type": "object",
        "title": request_schema_name,
        "properties": {
            "args": args_schema,
            "reference": {"type": "string", "description": "A reference string"},
            "capture": {"type": "boolean", "default": False},
            "step": {
                "type": "boolean",
                "description": "Whether to step through the task",
            },
        },
        # Exactly what AssignInput accepts: it forbids extra keys, so a field
        # advertised here that it does not know would be rejected on submit.
        "required": ["args"],
    }
    response_schema = create_json_schema_from_ports(
        implementation.definition.returns,
        response_schema_name,
    )

    async def implementation_endpoint(request: Request) -> JSONResponse:
        user = (
            expand_http_user_or_401(expand_user_from_request, request)
            if expand_user_from_request is not None
            else "fastapi"
        )
        payload = await request.json()
        assign_input = agent.build_assign_input(
            payload,
            interface=implementation.interface or implementation.definition.name,
        )
        assign = agent.build_assign_message(
            assign_input,
            user=str(user),
        )
        result = await agent.transport.asubmit(assign)
        return JSONResponse(content={"status": "submitted", "task_id": result})

    router.add_api_route(
        route_path,
        implementation_endpoint,
        methods=["POST"],
        summary=implementation.definition.name,
        description=implementation.definition.description
        or f"Execute {implementation.definition.name} action",
        tags=list(implementation.definition.collections)
        if implementation.definition.collections
        else [],
        response_class=JSONResponse,
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {
                        "schema": {
                            "$ref": f"#/components/schemas/{request_schema_name}"
                        }
                    }
                },
            },
            "responses": {
                "200": {
                    "description": "Successful Response",
                    "content": {
                        "application/json": {
                            "schema": {
                                "$ref": f"#/components/schemas/{response_schema_name}"
                            }
                        }
                    },
                }
            },
        },
    )
    # schema registration happens on the app once router is included
    router.__dict__.setdefault("_custom_schemas", {})[request_schema_name] = (
        request_schema
    )
    router.__dict__.setdefault("_custom_schemas", {})[response_schema_name] = (
        response_schema
    )


def build_implementation_router(
    agent: FastApiAgent,
    get_user_from_request: Callable[[Request], Any] | None = None,
    expand_user_from_request: ExpandUserFromRequest | None = None,
) -> APIRouter:
    """Build routes for all static implementations."""
    expand_user = (
        resolve_expand_user_from_request(expand_user_from_request, get_user_from_request)
        if expand_user_from_request is not None or get_user_from_request is not None
        else None
    )
    router = APIRouter()
    for implementation in agent.app_registry.get_implementations():
        add_implementation_route(router, agent, implementation, expand_user)
    return router
