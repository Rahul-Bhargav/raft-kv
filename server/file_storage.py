"""`Storage` that survives a restart: one JSON file per node, rewritten on every change."""

import json
import os
from collections.abc import Sequence
from dataclasses import asdict

from raft.messages import Entry
from raft.storage import MemoryStorage


class FileStorage(MemoryStorage):
    def __init__(self, path: str) -> None:
        super().__init__()
        self._path = path
        if os.path.exists(path):  # only `path` is read: a leftover .tmp is an unfinished save
            with open(path) as f:
                d = json.load(f)
            self._term, self._voted_for = d["term"], d["voted_for"]
            self._log += [Entry(e["term"], e["command"]) for e in d["log"]]

    def set_term_and_vote(self, term: int, voted_for: str | None) -> None:
        super().set_term_and_vote(term, voted_for)
        self._save()

    def append(self, entries: Sequence[Entry]) -> None:
        super().append(entries)
        self._save()

    def truncate_from(self, index: int) -> None:
        super().truncate_from(index)
        self._save()

    def _save(self) -> None:
        """Durable on return (R1): write a temp file, fsync it, then swap it in atomically."""
        log = [asdict(e) for e in self._log[1:]]  # without the index-0 sentinel
        with open(self._path + ".tmp", "w") as f:
            json.dump({"term": self._term, "voted_for": self._voted_for, "log": log}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(self._path + ".tmp", self._path)
