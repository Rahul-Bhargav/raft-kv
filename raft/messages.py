"""Raft messages and their JSON-safe dict form. Pure data: no I/O."""

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class Entry:
    """One log entry. Its index is implied by its position in the log."""

    term: int
    command: dict


@dataclass(frozen=True, slots=True)
class _Envelope:
    """Fields every message carries."""

    src: str
    dst: str
    term: int


@dataclass(frozen=True, slots=True)
class RequestVote(_Envelope):
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True, slots=True)
class RequestVoteReply(_Envelope):
    granted: bool


@dataclass(frozen=True, slots=True)
class AppendEntries(_Envelope):
    prev_log_index: int
    prev_log_term: int
    entries: tuple[Entry, ...]
    leader_commit: int


@dataclass(frozen=True, slots=True)
class AppendEntriesReply(_Envelope):
    # Replies are one-way, so they describe themselves: on success
    # match_index = prev_log_index + len(entries); last_log_index is the
    # follower's last index, a backtracking hint for the leader on failure.
    success: bool
    match_index: int
    last_log_index: int


Message = RequestVote | RequestVoteReply | AppendEntries | AppendEntriesReply

_MESSAGE_TYPES: dict[str, type] = {
    cls.__name__: cls
    for cls in (RequestVote, RequestVoteReply, AppendEntries, AppendEntriesReply)
}


def to_dict(msg: Message) -> dict[str, Any]:
    """Plain dict of JSON-native types, tagged with `"type": <class name>`."""
    d = {"type": type(msg).__name__, **asdict(msg)}  # asdict copies commands
    if isinstance(msg, AppendEntries):
        d["entries"] = list(d["entries"])  # a list, so it equals its JSON round trip
    return d


def from_dict(d: dict[str, Any]) -> Message:
    """Inverse of `to_dict`. An unknown or missing `type` raises ValueError.

    Missing or unexpected fields raise KeyError or TypeError.
    """
    cls = _MESSAGE_TYPES.get(d.get("type"))
    if cls is None:
        raise ValueError(f"unknown message type: {d.get('type')!r}")
    fields = {k: v for k, v in d.items() if k != "type"}
    if cls is AppendEntries:
        fields["entries"] = tuple(
            Entry(e["term"], dict(e["command"])) for e in fields["entries"]
        )
    return cls(**fields)
