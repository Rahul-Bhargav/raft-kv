"""Raft messages and their JSON-safe dict form. Pure data: no I/O.

There are two exchanges, both one-way (a reply is a separate message, not a return value):
  - Leader election:   candidate --RequestVote-->   every node --RequestVoteReply-->   candidate
  - Normal operation:  leader    --AppendEntries--> each follower --AppendEntriesReply--> leader

Every class here is `@dataclass(frozen=True, slots=True)`.
"""

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class Entry:
    """One log entry: a client command plus the term of the leader that created it.

    Not a message itself, but AppendEntries carries entries from leader to followers.
    Its index is implied by its position in the log, so it isn't stored.
    """

    term: int
    command: dict


@dataclass(frozen=True, slots=True)
class _Envelope:
    """Fields every message carries: sender, receiver, and the sender's current term.

    The term is how stale nodes are detected. A receiver that sees a higher term adopts it and
    becomes a follower (rule R2). A message with a lower term than the receiver's is out of date.
    """

    src: str
    dst: str
    term: int

@dataclass(frozen=True, slots=True)
class RequestVote(_Envelope):
    """Election, step 1: a candidate asks for a vote.

    Sent to every other node when a node's election timer runs out and it becomes a candidate
    for `term`. last_log_index/last_log_term describe the candidate's log, so a voter can refuse
    a candidate whose log is behind its own (rule R3).
    """

    last_log_index: int
    last_log_term: int


@dataclass(frozen=True, slots=True)
class RequestVoteReply(_Envelope):
    """Election, step 2: a node answers a RequestVote.

    granted=True means this node voted for the candidate in `term` (at most one vote per term).
    A candidate that collects grants from a majority becomes the leader.
    """

    granted: bool


@dataclass(frozen=True, slots=True)
class AppendEntries(_Envelope):
    """Normal operation: the leader sends new log entries, or a heartbeat, to one follower.

    The leader sends it when a client write is proposed (entries to replicate) and every
    heartbeat_interval ticks (`entries` empty), so followers know the leader is alive and don't
    start an election. prev_log_index/prev_log_term are the consistency check: the follower
    accepts only if its own entry at prev_log_index has that term. leader_commit tells the
    follower how far it may apply.
    """

    prev_log_index: int
    prev_log_term: int
    entries: tuple[Entry, ...]
    leader_commit: int


@dataclass(frozen=True, slots=True)
class AppendEntriesReply(_Envelope):
    """Normal operation: a follower answers an AppendEntries.

    success=True: the follower's log now matches the leader's up to match_index
    (= prev_log_index + len(entries)). The leader counts these to decide what is committed.
    success=False: the consistency check failed. last_log_index (the follower's last index)
    tells the leader where to retry from.
    Messages are one-way, so the reply carries everything the leader needs without the
    leader having to remember which request it answers.
    """

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
