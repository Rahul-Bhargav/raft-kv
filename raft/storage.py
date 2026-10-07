"""Persistent state a Raft node must not lose: term, vote and log. No I/O here.

Contract: when a mutating method returns, the change is durable. The node relies on
this to save state before it queues any message that depends on it (rule R1).
"""

from collections.abc import Sequence
from typing import Protocol

from raft.messages import Entry


class Storage(Protocol):
    @property
    def term(self) -> int: ...

    @property
    def voted_for(self) -> str | None: ...

    def set_term_and_vote(self, term: int, voted_for: str | None) -> None: ...

    def last_index(self) -> int:
        """Index of the last entry; 0 when the log is empty."""
        ...

    def term_at(self, index: int) -> int:
        """Term of the entry at `index`; `term_at(0) == 0`."""
        ...

    def entry_at(self, index: int) -> Entry:
        """The entry at `index` (1-based)."""
        ...

    def entries_from(self, index: int) -> list[Entry]:
        """Entries `index..last_index`; empty if `index` is past the end."""
        ...

    def append(self, entries: Sequence[Entry]) -> None:
        """Add entries after `last_index`."""
        ...

    def truncate_from(self, index: int) -> None:
        """Delete entries `index..last_index`; a no-op if `index` is past the end."""
        ...


class MemoryStorage:
    """In-memory `Storage` for tests. The log is 1-based, so a sentinel sits at index 0.

    The simulator keeps a crashed node's instance and hands it back on restart,
    which is how a disk survives a crash in tests.
    """

    def __init__(self) -> None:
        self._term = 0
        self._voted_for: str | None = None
        self._log: list[Entry] = [Entry(0, {"op": "noop"})]

    @property
    def term(self) -> int:
        return self._term

    @property
    def voted_for(self) -> str | None:
        return self._voted_for

    def set_term_and_vote(self, term: int, voted_for: str | None) -> None:
        self._term = term
        self._voted_for = voted_for

    def last_index(self) -> int:
        return len(self._log) - 1

    # Out-of-range indexes raise rather than wrap, because a negative index would
    # silently read from the end of the list and index 0 is the sentinel.
    def term_at(self, index: int) -> int:
        self._check_in_log(index, lowest=0)
        return self._log[index].term

    def entry_at(self, index: int) -> Entry:
        self._check_in_log(index, lowest=1)
        return self._log[index]

    def entries_from(self, index: int) -> list[Entry]:
        self._check_positive(index)
        return self._log[index:]

    def append(self, entries: Sequence[Entry]) -> None:
        self._log.extend(entries)

    def truncate_from(self, index: int) -> None:
        self._check_positive(index)
        del self._log[index:]

    def _check_in_log(self, index: int, lowest: int) -> None:
        if not lowest <= index <= self.last_index():
            raise IndexError(f"log index {index} not in {lowest}..{self.last_index()}")

    def _check_positive(self, index: int) -> None:
        if index < 1:
            raise IndexError(f"log index must be >= 1, got {index}")
