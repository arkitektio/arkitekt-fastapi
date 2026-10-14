"""A served app hosts the files it registered.

Under ``run(app)`` the distributed agent uploads an app's assets; a served app
registers with no one, so it serves them itself, by the name its bloks call
them by (``@self.assets.<name>``).
"""

import hashlib
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from arkitekt_spec.declare.app import AppRegistry
from arkitekt_fastapi.routes import configure_fastapi

PNG = b"\x89PNG\r\n\x1a\n" + b"pixels" * 20
GLB = b"glTF" + b"\x02\x00\x00\x00" + b"model" * 20
SOURCE = "https://example.org/models/ur5.glb"
SOURCE_REF = "sha256:" + "a" * 64


def _ref(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _registry(root: Path) -> AppRegistry:
    (root / "models").mkdir()
    (root / "models" / "scope.glb").write_bytes(GLB)
    (root / "logo.png").write_bytes(PNG)
    registry = AppRegistry(root=root)
    registry.register_asset("scope", "models/scope.glb")
    registry.register_asset("logo", "logo.png")
    registry.register_asset("robot", source=SOURCE, ref=SOURCE_REF)
    return registry


def _served(tmp_path: Path, **options: object) -> FastAPI:
    app = FastAPI()
    configure_fastapi(
        app=app,
        app_registry=_registry(tmp_path),
        db_file=str(tmp_path / "assets.db"),
        **options,  # type: ignore[arg-type]
    )
    return app


def test_the_assets_are_listed_as_they_are_declared(tmp_path: Path) -> None:
    with TestClient(_served(tmp_path)) as client:
        listed = client.get("/schemas/assets").json()

    assert listed["count"] == 3
    by_name = {asset["name"]: asset for asset in listed["assets"]}
    assert by_name["scope"]["ref"] == _ref(GLB)
    assert by_name["scope"]["size"] == len(GLB)
    assert by_name["logo"]["kind"] == "IMAGE"
    assert by_name["robot"] == {
        "name": "robot",
        "kind": "MODEL_3D",
        "ref": SOURCE_REF,
        "source": SOURCE,
    }
    # Where a file is on the server's machine is nobody else's business.
    assert all("path" not in asset for asset in listed["assets"])


def test_a_held_asset_is_served_with_its_type_and_reference(tmp_path: Path) -> None:
    with TestClient(_served(tmp_path)) as client:
        model = client.get("/assets/scope")
        image = client.get("/assets/logo")
        head = client.head("/assets/logo")

    assert model.status_code == 200
    assert model.content == GLB
    assert model.headers["content-type"] == "model/gltf-binary"
    assert model.headers["etag"] == f'"{_ref(GLB)}"'
    assert image.content == PNG
    assert image.headers["content-type"] == "image/png"
    assert head.status_code == 200
    assert head.headers["etag"] == f'"{_ref(PNG)}"'
    assert head.content == b""


def test_a_caller_that_has_the_file_is_told_so(tmp_path: Path) -> None:
    with TestClient(_served(tmp_path)) as client:
        same = client.get("/assets/logo", headers={"If-None-Match": f'"{_ref(PNG)}"'})
        other = client.get("/assets/logo", headers={"If-None-Match": f'"{_ref(GLB)}"'})

    assert same.status_code == 304
    assert same.content == b""
    assert other.status_code == 200


def test_an_unknown_name_is_not_found(tmp_path: Path) -> None:
    with TestClient(_served(tmp_path)) as client:
        answer = client.get("/assets/nothing")

    assert answer.status_code == 404
    assert "nothing" in answer.json()["detail"]


def test_an_asset_the_app_does_not_hold_redirects_to_its_source(tmp_path: Path) -> None:
    with TestClient(_served(tmp_path), follow_redirects=False) as client:
        answer = client.get("/assets/robot")

    assert answer.status_code == 307
    assert answer.headers["location"] == SOURCE


def test_a_file_changed_before_the_server_started_is_refused(tmp_path: Path) -> None:
    app = _served(tmp_path)
    # Same size, other bytes: only the hash tells.
    (tmp_path / "logo.png").write_bytes(PNG[:-1] + b"!")

    with TestClient(app) as client:
        changed = client.get("/assets/logo")
        untouched = client.get("/assets/scope")

    assert changed.status_code == 409
    assert "'logo'" in changed.json()["detail"]
    assert "changed since it was registered" in changed.json()["detail"]
    assert untouched.status_code == 200


def test_a_file_changed_under_a_running_server_is_refused(tmp_path: Path) -> None:
    with TestClient(_served(tmp_path)) as client:
        assert client.get("/assets/logo").status_code == 200
        (tmp_path / "logo.png").write_bytes(PNG + b"more")
        changed = client.get("/assets/logo")
        (tmp_path / "models" / "scope.glb").unlink()
        gone = client.get("/assets/scope")

    assert changed.status_code == 409
    assert gone.status_code == 409


def test_without_a_lifespan_the_assets_are_served_too(tmp_path: Path) -> None:
    """What ``arkitekt.serve`` does: the routes are added at once, from the registry handed in."""
    app = _served(tmp_path, lifespan=False)

    client = TestClient(app)
    assert client.get("/assets/scope").content == GLB
    assert client.get("/schemas/assets").json()["count"] == 3


def test_the_asset_routes_can_be_left_out_or_moved(tmp_path: Path) -> None:
    (tmp_path / "off").mkdir()
    (tmp_path / "moved").mkdir()
    with TestClient(_served(tmp_path / "off", add_assets=False)) as client:
        assert client.get("/assets/scope").status_code == 404
    with TestClient(_served(tmp_path / "moved", assets_path="/files")) as client:
        assert client.get("/files/scope").content == GLB
        assert client.get("/assets/scope").status_code == 404
