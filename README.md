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

`AsyncAgentTestClient` drives a served app in tests without a network.
