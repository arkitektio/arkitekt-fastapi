import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field

from arkitekt_runtime import messages
from arkitekt_runtime.agents.journal import JournalEntry
from arkitekt_spec.declare.protocol.types import AnyState

from arkitekt_fastapi.sink.protocol import StateChange


@dataclass
class MemoryStore:
    patches: list[StateChange] = field(default_factory=list)
    """The updates of every state, one per revision."""
    snapshots: list[messages.StateSnapshot] = field(default_factory=list)
    journal: dict[tuple[str, int], JournalEntry] = field(default_factory=dict)
    """The agent's journal, by ``(session_id, pos)`` (in insertion order)."""


class MemorySink:
    """In-memory sink that stores patches and snapshots as transport messages."""

    def __init__(self, memory: MemoryStore | None = None):
        self.store = memory or MemoryStore()
        self._session_id: str | None = None

    async def ainitialize(self):
        return None

    async def ateardown(self):
        return None

    async def acreate_session(
        self, states: list[AnyState], implementations: list
    ) -> str:
        self._session_id = str(uuid.uuid4())
        return self._session_id

    async def adump_snapshot(self, snapshot: messages.StateSnapshot):
        self.store.snapshots.append(snapshot)

    async def awrite_update(self, change: StateChange):
        self.store.patches.append(change)

    async def awrite_journal(self, entries: Sequence[JournalEntry]) -> None:
        for entry in entries:
            # INSERT OR IGNORE: a re-sent entry is a no-op.
            self.store.journal.setdefault((entry.session_id, entry.pos), entry)

    async def is_cought_up_to(self, revision: int) -> bool:
        return True
