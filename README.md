# arkitekt-fastapi

Server mode for an Arkitekt app: serve an app declared with [arkitekt-spec](../arkitekt-spec)
as an HTTP service.

```python
from fastapi import FastAPI
from arkitekt_fastapi import configure_fastapi

app = FastAPI()
configure_fastapi(app=app, app_registry=registry, db_file="journal.db")
```

Every declared action becomes a route with an OpenAPI schema generated from its ports; states,
locks, tasks and the journal are served alongside. It runs actions on
[arkitekt-runtime](../arkitekt-runtime)'s execution core, the same one
[rekuest](../rekuest)'s distributed agent uses, but with an in-process transport: no
websockets, no rath, no rekuest server.

A served app registers with no one and so cannot call other actions: `Task.call` raises
`NoCallerError` at once.

## Assets

The files an app registers with `register_asset` are served by the name its bloks call them
by (`@self.assets.<name>`):

- `GET /schemas/assets` lists them as they are declared: name, kind, reference, and the size
  or the source.
- `GET /assets/<name>` is the file, with its reference as the `ETag` (`If-None-Match` answers
  304). An asset registered with a `source` is not held by the app: the route redirects (307)
  to that source and fetches nothing.

A file that changed on disk since it was registered is refused with 409, since a reader checks
the bytes against the reference. Like the schema routes, the asset routes ask for no user.
`configure_fastapi(..., add_assets=False)` leaves them out; `assets_path=` moves them.

`AsyncAgentTestClient` drives a served app in tests without a network.
