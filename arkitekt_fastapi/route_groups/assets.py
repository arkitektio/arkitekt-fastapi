"""Asset route builders: the files an app registered, served by name."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse, Response

from arkitekt_spec.declare.app import AssetDeclaration

from arkitekt_fastapi.agent import FastApiAgent

logger = logging.getLogger(__name__)

#: What a file is served as, by its extension. A file with another extension (its
#: kind was said with ``kind=``) is served as bytes.
_CONTENT_TYPES: dict[str, str] = {
    ".glb": "model/gltf-binary",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
}


def _still_as_registered(asset: AssetDeclaration, path: Path) -> bool:
    """Whether the file at ``path`` still has the bytes its reference was computed from."""
    try:
        digest = hashlib.sha256()
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(1 << 20), b""):
                digest.update(chunk)
    except OSError:
        return False
    return "sha256:" + digest.hexdigest() == asset.ref


def _same_size(asset: AssetDeclaration, path: Path) -> bool:
    try:
        return path.stat().st_size == asset.size
    except OSError:
        return False


def build_asset_router(
    agent: FastApiAgent,
    assets_path: str = "/assets",
) -> APIRouter:
    """Build the routes serving the app's registered assets.

    A blok names an asset as ``@self.assets.<name>``; ``{assets_path}/<name>`` is
    that file. A file the app brings is served from where it was registered, with
    its reference as the ETag. A file the app only names a source for is answered
    with a redirect to that source: nothing is fetched here.

    Every held file is hashed once, now. One that no longer has the bytes it was
    registered with is refused when asked for, since whoever reads it checks it
    against the reference and would only see that check fail.
    """
    router = APIRouter(tags=["Assets"])
    assets = dict(agent.app_registry.registered_assets)
    changed: set[str] = set()
    for name, asset in assets.items():
        if asset.path is not None and not _still_as_registered(asset, asset.path):
            changed.add(name)
            logger.warning(
                "The asset '%s' (%s) changed since it was registered: it is not served",
                name,
                asset.where,
            )

    async def get_asset(name: str, request: Request) -> Response:
        asset = assets.get(name)
        if asset is None:
            raise HTTPException(status_code=404, detail=f"No asset is registered as '{name}'")
        if asset.path is None:
            return RedirectResponse(str(asset.source), status_code=307)
        if name in changed or not _same_size(asset, asset.path):
            raise HTTPException(
                status_code=409,
                detail=(
                    f"The asset '{name}' ({asset.where}) changed since it was "
                    f"registered: restart the app"
                ),
            )
        etag = f'"{asset.ref}"'
        headers = {"ETag": etag, "Cache-Control": "no-cache"}
        asked = request.headers.get("if-none-match", "")
        if etag in (tag.strip().removeprefix("W/") for tag in asked.split(",")):
            return Response(status_code=304, headers=headers)
        return FileResponse(
            asset.path,
            media_type=_CONTENT_TYPES.get(
                asset.path.suffix.lower(), "application/octet-stream"
            ),
            headers=headers,
        )

    router.add_api_route(
        f"{assets_path}/{{name}}",
        get_asset,
        methods=["GET"],
        response_class=Response,
        responses={
            200: {"description": "The file."},
            304: {"description": "The file is the one the caller already has."},
            307: {"description": "The app does not hold the file: its source."},
            404: {"description": "No asset is registered under this name."},
            409: {"description": "The file changed since it was registered."},
        },
    )
    # HEAD is the same answer without the body; one entry in the OpenAPI schema.
    router.add_api_route(
        f"{assets_path}/{{name}}", get_asset, methods=["HEAD"], include_in_schema=False
    )
    return router
