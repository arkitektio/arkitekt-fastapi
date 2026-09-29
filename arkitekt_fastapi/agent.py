"""FastAPI agent transport and websocket helpers.

The FastAPI integration exposes one websocket endpoint. Clients send an init
payload after connecting to declare which task action keys, state keys, and
lock keys they want to receive. Outgoing agent messages are then filtered by
message type and the matching subscription set.

A client that sends ``"journal": true`` opts into the agent's journal (see
``docs/journal.md``): its INIT carries the journal's watermark and the world as of
it, every frame after it carries ``pos`` and ``journal_session``, it also gets the
session-wide frames and ``ASSIGN``, and with ``resume_after`` it first gets what it
missed. A client that does not opt in gets exactly the frames it always got.
"""

import asyncio
import copy
import json
import logging
import uuid
from dataclasses import dataclass
from types import TracebackType
from typing import (
    Any,
    ClassVar,
    Self,
)
from collections.abc import AsyncIterator, Awaitable, Callable
from fastapi import WebSocket, WebSocketDisconnect
from pydantic import ConfigDict, Field, PrivateAttr

from arkitekt_runtime import messages
from arkitekt_spec.actions import StateImplementationInput
from arkitekt_runtime.types import AssignInput
from arkitekt_runtime.agents.base import BaseAgent
from arkitekt_runtime.agents.dataclasses import QueuedAssign, RevisedState
from arkitekt_runtime.agents.journal import (
    FLUSH_TIMEOUT,
    JournalEntry,
    JournalReader,
    JournalSink,
    Route,
)
from arkitekt_runtime.agents.transport.base import AgentTransport
from arkitekt_runtime.agents.backend import LocalAgentBackend
from arkitekt_fastapi.sink.backend import SinkAgentBackend
from arkitekt_fastapi.retriever.memory_retriever import MemoryRetriever
from arkitekt_fastapi.retriever.protocol import StateRetriever
from arkitekt_fastapi.sink.memory_sink import MemorySink
from arkitekt_fastapi.sink.protocol import StateSink
from arkitekt_fastapi.auth import (
    AuthenticationError,
    ExpandUserFromRequest,
)
from arkitekt_fastapi.models import (
    LockView,
    StateCollectionResponse,
    StateView,
    TaskCollectionResponse,
    TaskView,
    WebSocketSubscriptionInit,
)

logger = logging.getLogger(__name__)


def _is_state_message(message: messages.FromAgentMessage) -> bool:
    """Return whether a message belongs to the state update stream."""
    return isinstance(message, messages.StatePatch)


def _is_lock_message(message: messages.FromAgentMessage) -> bool:
    """Return whether a message belongs to the lock update stream."""
    return isinstance(message, (messages.Lock, messages.Unlock))


def _task_routing_key(message: messages.FromAgentMessage) -> str | None:
    """Resolve a default task routing key from a task-scoped message."""
    if _is_state_message(message) or _is_lock_message(message):
        return None
    return getattr(message, "task", None)


def _state_routing_key(message: messages.FromAgentMessage) -> str | None:
    """Resolve the state name used for state websocket subscriptions."""
    if not isinstance(message, messages.StatePatch):
        return None
    return message.state_name


def _lock_routing_key(message: messages.FromAgentMessage) -> str | None:
    """Resolve the lock key used for lock websocket subscriptions."""
    if not isinstance(message, (messages.Lock, messages.Unlock)):
        return None
    return message.key


@dataclass(frozen=True)
class _WebSocketSubscriptions:
    """Normalized websocket subscription filters."""

    action_keys: set[str] | None = None
    state_keys: set[str] | None = None
    lock_keys: set[str] | None = None

    @classmethod
    def from_init(cls, payload: WebSocketSubscriptionInit) -> "_WebSocketSubscriptions":
        """Build normalized subscription sets from an init payload."""

        def _normalize(values: list[str] | None) -> set[str] | None:
            if values is None:
                return None
            normalized = {value for value in values if value}
            return normalized or None

        return cls(
            action_keys=_normalize(payload.action_keys),
            state_keys=_normalize(payload.state_keys),
            lock_keys=_normalize(payload.lock_keys),
        )

    def admits(self, route: Route) -> bool:
        """Whether a journal subscriber gets an entry routed this way."""
        kind, key = route
        if kind == "state":
            return self.state_keys is None or key in self.state_keys
        if kind == "lock":
            return self.lock_keys is None or key in self.lock_keys
        if kind == "action":
            return self.action_keys is None or key in self.action_keys
        return True


def _dump(frame: dict[str, Any]) -> str:
    return json.dumps(frame, separators=(",", ":"), ensure_ascii=False)


class _JournalSubscriber:
    """A websocket client that opted into the journal.

    Its frames are queued in ``pos`` order as the journal records them (under the
    journal's lock), and sent by its own handler after its INIT and backlog, so it
    misses nothing after the watermark its INIT carries, and gets nothing twice.
    """

    def __init__(self, subscriptions: _WebSocketSubscriptions) -> None:
        self.subscriptions = subscriptions
        self.frames: asyncio.Queue[str] = asyncio.Queue()



@dataclass
class _ManagedWebSocketConnection:
    """Connection state for a single websocket client."""

    subscriptions: _WebSocketSubscriptions


class FastAPIConnectionManager:
    """Manage websocket connections for multiplexed task, state, and lock updates."""

    def __init__(self) -> None:
        """Initialize empty connection and subscription registries."""
        self._active_connections: set[WebSocket] = set()
        self._connections: dict[WebSocket, _ManagedWebSocketConnection] = {}
        self._lock = asyncio.Lock()
        self.task_routing_key_resolver: (
            Callable[[messages.FromAgentMessage], str | None] | None
        ) = None

    def get_task_routing_key(self, message: messages.FromAgentMessage) -> str | None:
        """Resolve the task routing key for an outgoing message."""
        if self.task_routing_key_resolver is not None:
            return self.task_routing_key_resolver(message)
        return _task_routing_key(message)

    async def connect(
        self,
        websocket: WebSocket,
        subscriptions: _WebSocketSubscriptions,
    ) -> None:
        """Register an accepted websocket with its subscriptions.

        Args:
            websocket: The websocket connection to register.
            subscriptions: The normalized subscription filters for the websocket.
        """
        async with self._lock:
            self._active_connections.add(websocket)
            self._connections[websocket] = _ManagedWebSocketConnection(
                subscriptions=subscriptions
            )
        logger.info(
            f"WebSocket connected. Total connections: {len(self._active_connections)}"
        )

    async def disconnect(self, websocket: WebSocket) -> None:
        """Remove a websocket connection and its subscriptions.

        Args:
            websocket: The websocket connection to remove.
        """
        async with self._lock:
            self._active_connections.discard(websocket)
            self._connections.pop(websocket, None)
        logger.info(
            f"WebSocket disconnected. Total connections: {len(self._active_connections)}"
        )

    async def broadcast_model(self, message: messages.FromAgentMessage) -> None:
        """Broadcast a message to websocket clients that subscribed to it.

        Args:
            message: The outgoing agent message to distribute.
        """

        message_json = message.model_dump_json()
        async with self._lock:
            disconnected: list[WebSocket] = []
            for connection in self._active_connections:
                connection_state = self._connections.get(connection)
                if connection_state is None or not self._matches_subscription(
                    connection_state.subscriptions, message
                ):
                    continue
                try:
                    await connection.send_text(message_json)
                except Exception as e:
                    logger.warning(f"Failed to send message to WebSocket: {e}")
                    disconnected.append(connection)

        for connection in disconnected:
            await self.disconnect(connection)

    def _matches_subscription(
        self,
        subscriptions: _WebSocketSubscriptions,
        message: messages.FromAgentMessage,
    ) -> bool:
        """Return whether a message matches a websocket subscription set."""
        if _is_state_message(message):
            state_key = _state_routing_key(message)
            return state_key is not None and (
                subscriptions.state_keys is None
                or state_key in subscriptions.state_keys
            )

        if _is_lock_message(message):
            lock_key = _lock_routing_key(message)
            return lock_key is not None and (
                subscriptions.lock_keys is None or lock_key in subscriptions.lock_keys
            )

        action_key = self.get_task_routing_key(message)
        return action_key is not None and (
            subscriptions.action_keys is None or action_key in subscriptions.action_keys
        )

    async def send_personal(self, websocket: WebSocket, message: str) -> None:
        """Send a raw message to one websocket client.

        Args:
            websocket: The target websocket connection.
            message: The JSON message payload.
        """
        try:
            await websocket.send_text(message)
        except Exception as e:
            logger.warning(f"Failed to send personal message to WebSocket: {e}")

    @property
    def connection_count(self) -> int:
        """Return the number of active connections."""
        return len(self._active_connections)


class FastApiTransport(AgentTransport):
    """Transport for FastAPI-based agents.

    This transport accepts incoming command messages from HTTP routes through
    `asubmit()` and forwards outgoing agent messages through a single websocket
    manager. Each websocket connection announces the task, state, and lock keys
    it wants to receive during its init handshake.

    There is no control channel here to lose. The websockets this manages are
    *observers* — a UI subscribing to updates — and the actual command path is an
    in-process queue fed by HTTP routes. So both halves of the disconnect story are
    inert for this transport: ``ConnectionPolicy`` has nothing to reconnect, and an
    action's ``DisconnectPolicy`` never fires, however it is declared. A subscriber
    going away is not a loss of control.
    """

    connection_manager: FastAPIConnectionManager = Field(
        default_factory=FastAPIConnectionManager,
        description="The websocket connection manager for multiplexed updates.",
    )

    _receive_queue: asyncio.Queue[messages.ToAgentMessage] | None = PrivateAttr(
        default=None
    )
    _connected: bool = PrivateAttr(default=False)
    _journal_subscribers: set[_JournalSubscriber] = PrivateAttr(default_factory=set)
    _on_submit: Callable[[messages.ToAgentMessage], None] | None = PrivateAttr(
        default=None
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def connected(self) -> bool:
        """Return True if the transport is connected."""
        return self._connected

    async def asubmit(self, message: messages.ToAgentMessage) -> str:
        """Submit a message to the agent from an API route.

        This method is called from FastAPI route handlers to send messages
        to the agent for processing. Responses are sent via WebSocket.

        Args:
            message: The message to send to the agent.

        Returns:
            The task ID for tracking the request.
        """
        if self._receive_queue is None:
            raise RuntimeError("Transport not connected. Call aconnect first.")

        if self._on_submit is not None:
            # Before the agent sees it: an ASSIGN is its task's first journal entry.
            self._on_submit(message)
        # Put the message on the queue for the agent to process
        await self._receive_queue.put(message)
        logger.info(f"Submitted message to agent: {message}")

        # Return the task ID for tracking
        if isinstance(message, messages.Assign):
            return message.task
        return getattr(message, "task", getattr(message, "id", "unknown"))

    def set_submit_hook(
        self, hook: Callable[[messages.ToAgentMessage], None] | None
    ) -> None:
        """Be told (synchronously) about every submitted message before the agent is."""
        self._on_submit = hook

    async def asend(
        self,
        message: messages.FromAgentMessage,
        entry: JournalEntry | None = None,
    ) -> None:
        """Route an outgoing agent message by message type and subscriptions.

        Task messages are matched against `action_keys`, state patch messages
        against `state_keys`, and lock lifecycle messages against `lock_keys`.
        The connection manager handles the actual per-socket filtering.

        Journal subscribers got a journaled message (``entry``) already, from
        :meth:`journal_fanout`; they get a message the journal did not record here.

        Args:
            message: The message to send to subscribed websocket clients.
            entry: The journal entry recording the message, if it was recorded.
        """
        message_json = message.model_dump_json()
        logger.info(f"Agent sending message: {message_json}")
        if entry is None and self._journal_subscribers:
            for subscriber in list(self._journal_subscribers):
                if self.connection_manager._matches_subscription(
                    subscriber.subscriptions, message
                ):
                    subscriber.frames.put_nowait(message_json)
        await self.connection_manager.broadcast_model(message)

    def journal_fanout(
        self, entry: JournalEntry, message: messages.Message | None
    ) -> None:
        """Queue a new journal entry for the journal subscribers it is routed to.

        Called by the journal under its lock, in ``pos`` order. The frame is the
        message as every client gets it (with its stream ``seq``) plus ``pos`` and
        ``journal_session``; an ``ASSIGN`` is its recorded payload.
        """
        if not self._journal_subscribers:
            return
        route = entry.route()
        text: str | None = None
        for subscriber in list(self._journal_subscribers):
            if not subscriber.subscriptions.admits(route):
                continue
            if text is None:
                frame = (
                    message.model_dump(mode="json")
                    if message is not None
                    else dict(entry.payload)
                )
                frame["pos"] = entry.pos
                frame["journal_session"] = entry.session_id
                if entry.step is not None:
                    frame["task_step"] = entry.step
                text = _dump(frame)
            subscriber.frames.put_nowait(text)

    def add_journal_subscriber(self, subscriber: _JournalSubscriber) -> None:
        """Start queueing journal frames for a subscriber (call under the journal lock)."""
        self._journal_subscribers.add(subscriber)

    def remove_journal_subscriber(self, subscriber: _JournalSubscriber) -> None:
        self._journal_subscribers.discard(subscriber)

    async def aconnect(self) -> None:
        """Connect the transport."""
        self._receive_queue = asyncio.Queue()
        self._connected = True
        logger.info("FastAPI transport connected")

    async def areceive(self) -> AsyncIterator[messages.ToAgentMessage]:
        """Receive messages from the queue.

        This is an async generator that yields messages as they arrive
        in the receive queue (submitted via API routes).

        Yields:
            Messages to be processed by the agent.
        """
        if self._receive_queue is None:
            raise RuntimeError("Transport not connected. Call aconnect first.")

        while True:
            try:
                message = await self._receive_queue.get()
                yield message
            except asyncio.CancelledError:
                logger.info("Receive loop cancelled")
                raise

    async def adisconnect(self) -> None:
        """Disconnect the transport."""
        self._connected = False
        self._receive_queue = None
        logger.info("FastAPI transport disconnected")

    async def handle_websocket(
        self,
        websocket: WebSocket,
        build_initial_payload: Callable[
            [WebSocketSubscriptionInit], Awaitable[dict[str, Any] | None]
        ]
        | None = None,
        expand_user_from_request: ExpandUserFromRequest | None = None,
        open_journal: Callable[
            [WebSocketSubscriptionInit, _WebSocketSubscriptions],
            Awaitable[tuple[_JournalSubscriber, dict[str, Any], list[JournalEntry]]],
        ]
        | None = None,
    ) -> None:
        """Serve the unified websocket endpoint.

        The websocket must send a JSON init payload immediately after connect.
        The payload may contain `action_keys`, `state_keys`, and `lock_keys`
        arrays that define which updates should be delivered.

        With `"journal": true` (and an `open_journal` callback) the client is a
        journal subscriber: its INIT gets a `journal` object, it first gets the
        entries it asked to resume, then every journaled frame after the watermark.

        Args:
            websocket: The accepted websocket connection.
            build_initial_payload: Optional callback used to construct the first
                snapshot message after the init payload has been received.
            expand_user_from_request: Optional hook used to authenticate the
                handshake from the init payload. Raising `AuthenticationError`
                closes the socket with code 1008 before any subscription is made.
        """
        logger.debug("WebSocket connection received, waiting for init payload...")
        await websocket.accept()
        subscriber: _JournalSubscriber | None = None
        pump: asyncio.Task[None] | None = None
        try:
            init_data = await websocket.receive_json()
            if not isinstance(init_data, dict):
                raise ValueError("Websocket init payload must be a JSON object")

            init_payload = WebSocketSubscriptionInit.model_validate(init_data)

            if expand_user_from_request is not None:
                try:
                    expand_user_from_request(init_payload)
                except AuthenticationError as auth_error:
                    # Closed *after* accept on purpose. Closing before accept makes the
                    # handshake fail as an HTTP error, which browsers surface as an
                    # opaque code 1006 that a client cannot tell apart from a dropped
                    # connection. A post-accept 1008 is a code the client can act on.
                    logger.info("WebSocket rejected: %s", auth_error)
                    await websocket.close(code=1008, reason="unauthorized")
                    return

            subscriptions = _WebSocketSubscriptions.from_init(init_payload)
            journal_info: dict[str, Any] | None = None
            backlog: list[JournalEntry] = []
            if init_payload.journal and open_journal is not None:
                subscriber, journal_info, backlog = await open_journal(
                    init_payload, subscriptions
                )
            else:
                await self.connection_manager.connect(websocket, subscriptions)

            if build_initial_payload is not None:
                initial_message = await build_initial_payload(init_payload)
                if initial_message is not None:
                    if journal_info is not None:
                        initial_message["journal"] = journal_info
                    await websocket.send_json(initial_message)

            if subscriber is not None:
                for entry in backlog:
                    await websocket.send_text(_dump(entry.frame()))
                pump = asyncio.create_task(self._apump(websocket, subscriber))

            # Keep the socket open for outbound events; nothing after INIT is
            # acted on. ``receive_text`` (unlike the raw ``receive``) raises
            # ``WebSocketDisconnect`` when the client goes away, which is what ends
            # this loop cleanly.
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            logger.info("WebSocket client disconnected")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            if pump is not None:
                pump.cancel()
            if subscriber is not None:
                self.remove_journal_subscriber(subscriber)
            else:
                await self.connection_manager.disconnect(websocket)

    async def _apump(self, websocket: WebSocket, subscriber: _JournalSubscriber) -> None:
        """Send a journal subscriber's frames, in order, until the socket goes away."""
        try:
            while True:
                text = await subscriber.frames.get()
                await websocket.send_text(text)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — the client went away mid-send
            logger.info(f"Journal subscriber gone: {e}")
            self.remove_journal_subscriber(subscriber)

    async def __aenter__(self) -> Self:
        """Enter the context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the context manager."""
        await self.adisconnect()


class FastApiAgent(BaseAgent):
    """An Agent that uses FastAPI as its web framework.

    This agent uses `FastApiTransport` to receive messages from HTTP
    API routes and to publish multiplexed websocket updates for tasks,
    states, and locks over a single `/ws` endpoint.

    Example usage:

    ```python
    from fastapi import FastAPI, WebSocket
    from arkitekt_fastapi.agent import FastApiAgent

    app = FastAPI()
    agent = FastApiAgent()


    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await agent.handle_websocket(websocket)

    @app.post("/assign")
    async def assign_action(action: dict):
        message = Assign(...)
        result = await agent.transport.asubmit(message)
        return result
    ```
    """

    name: str = Field(default="FastApiAgent", description="The name of the agent.")
    transport: FastApiTransport = Field(
        default_factory=FastApiTransport,
        description="The FastAPI transport for this agent.",
    )
    sink: StateSink = Field(default_factory=lambda: MemorySink())
    retriever: StateRetriever = Field(default_factory=lambda: MemoryRetriever())

    sink_catch_up_timeout: float | None = Field(
        default=5.0,
        description="Maximum number of seconds to wait for the sink to report that it has caught up during shutdown. Use `None` to wait indefinitely.",
    )
    sink_catch_up_poll_interval: float = Field(
        default=0.05,
        description="Polling interval in seconds used while waiting for the sink to catch up during shutdown.",
    )

    # Websocket clients that opt into the journal get ``pos`` on their frames; the
    # frames themselves stay exactly what every other client gets.
    stamps_frames: ClassVar[bool] = False

    def model_post_init(self, __context: Any) -> None:
        """Wire task routing so websocket subscriptions use action keys."""
        super().model_post_init(__context)
        # The journal is persisted with the state history, when the sink keeps one,
        # and fanned out to the websocket clients that opted into it.
        self._journal.sink = self.sink if isinstance(self.sink, JournalSink) else None
        self._journal.add_listener(self.transport.journal_fanout)
        self.transport.set_submit_hook(self._record_submitted)
        # This agent's control plane is its sink, not a remote server.
        if isinstance(self.backend, LocalAgentBackend):
            self.backend = SinkAgentBackend(sink=self.sink)
        self.transport.connection_manager.task_routing_key_resolver = (
            self.get_task_action_key_for_message
        )
        if isinstance(self.sink, MemorySink) and isinstance(
            self.retriever, MemoryRetriever
        ):
            self.retriever.store = self.sink.store

    def get_state_schemas(self) -> dict[str, StateImplementationInput]:
        """Return the registered state schemas from the app registry."""
        return dict(self.app_registry.states)

    def build_assign_input(
        self,
        payload: dict[str, Any],
        interface: str | None = None,
    ) -> AssignInput:
        """Normalize FastAPI request payloads to the current AssignInput model.

        A REST assign against this in-process agent is always a provenance root: there
        is no server-side task tree to attach to, which is the same reason the backend
        made ``AssignInput`` root-only.
        """
        normalized_payload = dict(payload)

        if interface is not None:
            normalized_payload["interface"] = interface

        # `cached`, `log` and `ephemeral` were dropped from AssignInput by the backend,
        # and `parent` with them (a GraphQL assign is a root by definition). A payload
        # still carrying any of them would now be rejected as extra input.
        for field_name in ("cached", "log", "ephemeral", "parent"):
            normalized_payload.pop(field_name, None)
        normalized_payload.setdefault("capture", False)

        return AssignInput.model_validate(normalized_payload)

    def build_assign_message(
        self,
        assign_input: AssignInput,
        user: str,
        action: str = "api_call",
        app: str = "fastapi",
        org: str = "fastapi",
        implementation: str | None = None,
        task: str | None = None,
    ) -> messages.Assign:
        """Build an Assign message from a normalized FastAPI request payload.

        The in-process FastAPI agent has no real org/implementation routing, so it stamps
        placeholder values (the ``org``/``implementation`` args) onto the Assign — the
        message model requires both. ``parent`` is left unset for the same reason there
        is none to read off the ``AssignInput``: a REST assign here is always a root.
        """
        if assign_input.interface is None:
            raise ValueError("FastAPI assign requests require an interface")

        return messages.Assign(
            interface=assign_input.interface,
            task=task or str(uuid.uuid4()),
            reference=assign_input.reference,
            args=assign_input.args,
            capture=assign_input.capture,
            user=user,
            org=org,
            action=action,
            implementation=implementation or assign_input.implementation or app,
            step=assign_input.step,
        )

    async def abuild_websocket_init_message(
        self,
        init_payload: WebSocketSubscriptionInit,
    ) -> dict[str, Any]:
        """Build the initial websocket snapshot for a newly connected client."""
        tasks = await self.aget_task_views(init_payload.action_keys)
        states = await self.aget_state_views(init_payload.state_keys)
        locks = await self.aget_lock_views(init_payload.lock_keys)
        return {
            "type": "INIT",
            "tasks": tasks.model_dump(mode="json"),
            "states": states.model_dump(mode="json"),
            "locks": {
                "count": len(locks),
                "locks": {
                    key: value.model_dump(mode="json") for key, value in locks.items()
                },
            },
        }

    async def handle_websocket(
        self,
        websocket: WebSocket,
        expand_user_from_request: ExpandUserFromRequest | None = None,
    ) -> None:
        """Serve the unified websocket endpoint for this agent.

        Args:
            websocket: The websocket connection to serve.
            expand_user_from_request: Optional hook authenticating the handshake
                from the init payload the client sends after connecting.
        """
        await self.transport.handle_websocket(
            websocket,
            build_initial_payload=self.abuild_websocket_init_message,
            expand_user_from_request=expand_user_from_request,
            open_journal=self.aopen_journal,
        )

    def _record_submitted(self, message: messages.ToAgentMessage) -> None:
        """Record a submitted assignment as its task's first journal entry.

        Queued in the agent's ordered queue before the agent sees the assignment,
        so it precedes everything the task reports.
        """
        queue = self._event_queue
        if not isinstance(message, messages.Assign) or queue is None:
            return
        # Thread-safe: routes may submit from another event loop (the test clients do).
        queue.sync_q.put_nowait(
            QueuedAssign(assign=message, action_key=self.build_task_action_key(message))
        )

    async def aopen_journal(
        self,
        init_payload: WebSocketSubscriptionInit,
        subscriptions: _WebSocketSubscriptions,
    ) -> tuple[_JournalSubscriber, dict[str, Any], list[JournalEntry]]:
        """Subscribe a journal client at the current watermark.

        Returns the subscription, the INIT's ``journal`` object (the world exactly as
        of the watermark) and the entries to replay (those after ``resume_after``).
        The subscription, the watermark and the world are taken under the journal's
        lock: live frames after the watermark queue up for the subscriber, so
        nothing is missed or sent twice.
        """
        subscriber = _JournalSubscriber(subscriptions)
        after = init_payload.resume_after
        with self._journal.locked() as view:
            watermark = view.watermark
            recent = (
                view.recent(after, watermark.pos)
                if watermark is not None and after is not None and after <= watermark.pos
                else None
            )
            self.transport.add_journal_subscriber(subscriber)
            fold = view.fold.copy()

        if watermark is None:
            info: dict[str, Any] = {
                "session_id": None,
                "pos": 0,
                "global_rev": 0,
                "resync": after is not None,
            }
            return subscriber, info, []

        # A position alone may be from before a restart: resuming needs the session
        # it counts in, unless it resumes from the very start.
        resync = after is not None and (
            after > watermark.pos
            or (after > 0 and init_payload.session_id != watermark.session_id)
        )
        backlog: list[JournalEntry] = []
        if after is not None and not resync:
            if recent is not None:
                backlog = recent
            elif isinstance(self.retriever, JournalReader):
                await self._journal.aflush_to(watermark.pos, FLUSH_TIMEOUT)
                try:
                    backlog = await self.retriever.aget_journal_entries(
                        watermark.session_id, after=after, until=watermark.pos
                    )
                except Exception:
                    logger.error(
                        "Could not read the journal to resume a subscriber", exc_info=True
                    )
        backlog = [entry for entry in backlog if subscriptions.admits(entry.route())]

        info = fold.world(
            action_keys=subscriptions.action_keys,
            state_keys=subscriptions.state_keys,
            lock_keys=subscriptions.lock_keys,
        )
        info["session_id"] = watermark.session_id
        info["pos"] = watermark.pos
        info["global_rev"] = watermark.global_rev
        info["resync"] = resync
        return subscriber, info, backlog

    def build_task_action_key(self, assign_message: messages.Assign) -> str:
        """Build the routing key used for task websocket subscriptions."""
        return assign_message.interface or assign_message.action or assign_message.task

    def get_task_action_key_for_message(self, message: messages.Message) -> str | None:
        """Resolve the task action key for an outgoing message."""
        task = getattr(message, "task", None)
        if task is None:
            return None
        assign_message = self.managed_assignments.get(task)
        if assign_message is None:
            return None
        return self.build_task_action_key(assign_message)

    async def aget_task_views(
        self,
        action_keys: list[str] | None = None,
    ) -> TaskCollectionResponse:
        """Return the current task overview filtered by action keys."""
        normalized_action_keys = set(action_keys) if action_keys else None
        tasks: dict[str, TaskView] = {}

        for task_id, assign_message in self.managed_assignments.items():
            action_key = self.build_task_action_key(assign_message)
            if (
                normalized_action_keys is not None
                and action_key not in normalized_action_keys
            ):
                continue

            tasks[task_id] = TaskView(
                task=task_id,
                action_key=action_key,
                interface=assign_message.interface,
                user=assign_message.user,
                app=assign_message.org,
                action=assign_message.action,
                running=task_id in self.running_assignments,
                actor_id=self.running_assignments.get(task_id),
            )

        return TaskCollectionResponse(count=len(tasks), tasks=tasks)

    async def aget_state_views(
        self,
        state_keys: list[str] | None = None,
    ) -> StateCollectionResponse:
        """Return current state values and revisions filtered by state keys."""
        selected_keys = set(state_keys) if state_keys else None
        states: dict[str, StateView] = {}

        for interface, state_schema in self._collected_state_schemas.items():
            if selected_keys is not None and interface not in selected_keys:
                continue
            states[interface] = StateView(
                interface=interface,
                name=state_schema.name,
                initialized=interface in self.states,
                value=copy.deepcopy(self._current_shrunk_states.get(interface)),
            )

        return StateCollectionResponse(
            current_session=self.current_session,
            current_global_revision=self.global_revision,
            count=len(states),
            states=states,
        )

    async def aget_checkout_state_views(
        self,
        global_revision_id: int,
        state_keys: list[str] | None = None,
        session_id: str | None = None,
    ) -> StateCollectionResponse:
        """Return reconstructed state values for a historical global revision."""
        selected_keys = set(state_keys) if state_keys else None
        resolved_session_id = session_id or self.current_session
        states: dict[str, StateView] = {}

        for interface, state_schema in self._collected_state_schemas.items():
            if selected_keys is not None and interface not in selected_keys:
                continue

            snapshot = await self.retriever.aget_state_at_global_rev(
                global_revision_id,
                state_id=interface,
                session_id=resolved_session_id,
            )

            if snapshot is None or isinstance(snapshot, list):
                states[interface] = StateView(
                    interface=interface,
                    name=state_schema.name,
                    initialized=False,
                    value=None,
                )
                continue

            states[interface] = StateView(
                interface=interface,
                name=state_schema.name,
                initialized=True,
                value=copy.deepcopy(snapshot.data),
            )

        return StateCollectionResponse(
            current_session=resolved_session_id,
            current_global_revision=global_revision_id,
            count=len(states),
            states=states,
        )

    async def aget_lock_views(
        self,
        lock_keys: list[str] | None = None,
    ) -> dict[str, LockView]:
        """Return current lock values filtered by lock key or interface."""
        selected_keys = set(lock_keys) if lock_keys else None
        locks: dict[str, LockView] = {}

        for interface, lock in self.locks.items():
            if (
                selected_keys is not None
                and interface not in selected_keys
                and lock.definition.key not in selected_keys
            ):
                continue
            locking_task = lock.locking_task
            locks[interface] = LockView(
                interface=interface,
                key=lock.definition.key,
                task_id=str(locking_task) if locking_task else None,
            )

        return locks

    async def _asend_out(
        self, message: messages.FromAgentMessage, entry: JournalEntry | None = None
    ) -> None:
        """Broadcast to websocket clients (journal subscribers already have ``entry``)."""
        await self.transport.asend(message, entry)

    async def apublish_patch(
        self, patch: messages.StatePatch, entry: JournalEntry | None = None
    ) -> None:
        """Publish a state patch event: broadcast to websocket clients and persist to sink."""
        await self._asend_out(patch, entry)
        await self.sink.awrite_patch(patch)

    async def apublish_snapshot(
        self, snapshot: messages.StateSnapshot, entry: JournalEntry | None = None
    ) -> None:
        await self._asend_out(snapshot, entry)
        return await self.sink.adump_snapshot(snapshot)

    async def apublish_session_init(
        self, session_init: messages.SessionInit, entry: JournalEntry | None = None
    ) -> None:
        """Announce the session and give the sink its baseline snapshot.

        The sink stores snapshots, not session messages, so the states carried by the
        session init are handed to it in the shape it persists.
        """
        await self._asend_out(session_init, entry)
        await self.sink.adump_snapshot(
            messages.StateSnapshot(
                session_id=session_init.session_id,
                global_rev=self.global_revision,
                snapshots=dict(session_init.states),
            )
        )

    async def aget_revised_state(self, interface: str) -> RevisedState:
        """Get the current agent-owned shrunk state and revision for an interface."""
        from arkitekt_spec.declare.agents.errors import AgentException

        if interface not in self._current_shrunk_states:
            raise AgentException(f"No shrunk state found for interface {interface}")

        return RevisedState(
            revision=self.global_revision,
            data=copy.deepcopy(self._current_shrunk_states[interface]),
        )

    async def _await_persistence_caught_up(self) -> None:
        poll_interval = max(self.sink_catch_up_poll_interval, 0.0)

        async def _wait() -> None:
            while not await self.sink.is_cought_up_to(self.global_revision):
                await asyncio.sleep(poll_interval)

        if self.sink_catch_up_timeout is None:
            await _wait()
            return None

        try:
            await asyncio.wait_for(_wait(), timeout=self.sink_catch_up_timeout)
        except TimeoutError:
            logger.warning(
                "Timed out waiting for persistence to catch up during shutdown after %.2fs",
                self.sink_catch_up_timeout,
            )

    async def _await_acknowledged(self) -> None:
        """Mark the agent connected immediately.

        The server-side FastAPI transport is queue-based and has no Init
        handshake, so there is no acknowledgement to wait for. ``aloop`` then
        drains the queue as messages are submitted via API routes.
        """
        self._connected_event.set()

    async def astart(self, app_context: Any = None) -> None:
        """Start the agent and initialize the sink and retriever."""
        await self.sink.ainitialize()
        await self.retriever.ainitialize()
        # Before anything is recorded: a doomed start is refused here, with the sink
        # initialised so the teardown that follows has something to close.
        self.app_registry.require_app_context(app_context, whose="This app")
        if isinstance(self.backend, SinkAgentBackend):
            # The session records what the agent implements and its states (the latter
            # exist only once started).
            self.backend.implementations = list(self.app_registry.implementations.values())
            self.backend.states = list(self.states.values())
        # The baseline snapshot reaches the sink through apublish_session_init below,
        # which super().astart() triggers once states are initialized. (This used to dump
        # a second, identical snapshot here.)
        await super().astart(app_context=app_context)

    async def atear_down(self) -> None:
        """Tear down the agent and clean up the sink and retriever."""
        await super().atear_down()
        if self._ran_startup_hooks:
            # An agent that never started persisted nothing to catch up on.
            await self._await_persistence_caught_up()
        await self._journal.aclose(
            self.sink_catch_up_timeout if self.sink_catch_up_timeout is not None else FLUSH_TIMEOUT
        )
        await self.sink.ateardown()
        await self.retriever.ateardown()
