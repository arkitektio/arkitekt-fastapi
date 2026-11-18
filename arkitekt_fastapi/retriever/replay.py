"""Storage-agnostic patch/snapshot algebra shared by all state retrievers."""

from __future__ import annotations

import copy
from datetime import datetime
from typing import cast
from collections.abc import Iterable

import jsonpatch  # type: ignore[import-untyped]

from arkitekt_fastapi.retriever.protocol import (
    PatchEvent,
    SessionBoundary,
    Snapshot,
    TaskBoundary,
)
from arkitekt_runtime.messages import JSONSerializable


def build_patch_operation(
    op: str, path: str, value: JSONSerializable | None
) -> dict[str, JSONSerializable]:
    """Build one RFC-6902 operation; ``remove`` carries no value."""
    operation: dict[str, JSONSerializable] = {"op": op, "path": path}
    if op != "remove":
        operation["value"] = value
    return operation


def build_patch_document(ops: Iterable[dict[str, JSONSerializable]]) -> list[JSONSerializable]:
    """An update as an RFC-6902 patch document: its operations, in order."""
    return [
        build_patch_operation(str(op.get("op") or ""), str(op.get("path") or ""), op.get("value"))
        for op in ops
    ]


def apply_patch_document(
    state_data: JSONSerializable, patch_document: JSONSerializable
) -> JSONSerializable:
    """Apply one patch document (an update's operations, in order) to a state value
    without mutating the input."""
    return cast(
        JSONSerializable,
        jsonpatch.apply_patch(state_data, patch_document, in_place=False),
    )


def replay(anchor: Snapshot, patches: Iterable[PatchEvent]) -> Snapshot:
    """Replay patch events (already ordered by revision) on top of an anchor.

    Returns a new snapshot whose revision/timepoint/session come from the last
    applied patch, or the anchor itself when there are no patches.
    """
    state_data = cast(JSONSerializable, copy.deepcopy(anchor.data))
    last = anchor
    for event in patches:
        state_data = apply_patch_document(state_data, event.patch)
        last = Snapshot(
            timepoint=event.timepoint,
            data=state_data,
            global_revision=event.global_future_rev,
            session_id=event.session_id,
        )
    if last is anchor:
        return Snapshot(
            timepoint=anchor.timepoint,
            data=state_data,
            global_revision=anchor.global_revision,
            session_id=anchor.session_id,
        )
    return last


def merge_state_ids(*groups: Iterable[str]) -> list[str]:
    """Union state ids from several sources into a sorted list."""
    merged: set[str] = set()
    for group in groups:
        merged.update(group)
    return sorted(merged)


def boundary_from_aggregates(
    key: str,
    start_global_revision: int,
    end_global_revision: int,
    start_time: datetime,
    end_time: datetime,
    *,
    session: bool,
) -> TaskBoundary | SessionBoundary:
    """Build a task or session boundary from MIN/MAX aggregates."""
    if session:
        return SessionBoundary(
            session_id=key,
            start_global_revision=start_global_revision,
            end_global_revision=end_global_revision,
            start_time=start_time,
            end_time=end_time,
        )
    return TaskBoundary(
        correlation_id=key,
        start_global_revision=start_global_revision,
        end_global_revision=end_global_revision,
        start_time=start_time,
        end_time=end_time,
    )
