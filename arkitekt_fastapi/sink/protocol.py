from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from arkitekt_runtime import messages
from arkitekt_spec.declare.protocol.types import AnyState


@dataclass
class StateChange:
    """One update of a state, as a sink keeps it: the agent's ``STATE_UPDATE`` with the
    session it was made in and when (which the frame leaves to its journal stamp)."""

    state_name: str
    global_rev: int
    """The agent's state revision once the update is applied."""
    session_id: str
    ts: float
    """When the update was made, in seconds since the epoch."""
    task_id: str | None
    ops: list[dict[str, Any]]
    """The update as an RFC 6902 document: its operations, applied in order."""


@runtime_checkable
class StateSink(Protocol):
    # --- INITIALIZATION & SESSION MANAGEMENT ---
    async def ainitialize(self):
        """Should be called once to set up the sink (e.g., create tables, indexes)."""
        ...

    async def ateardown(self):
        """Cleans up resources, such as database connections."""
        ...

    async def acreate_session(
        self, states: list[AnyState], implementations: list
    ) -> str:
        """Creates a new session and returns its ID."""
        ...

    # --- WRITE METHODS ---
    async def adump_snapshot(self, snapshot: messages.StateSnapshot):
        """Store a full snapshot of all states at a given revision."""
        ...

    async def awrite_update(self, change: StateChange):
        """Write one update of a state to the store."""
        ...

    async def is_cought_up_to(self, revision: int) -> bool:
        """Returns True if the sink has received all events up to at least the given revision."""
        ...
