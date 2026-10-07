"""A seeded, in-memory network for the simulator. Time is an integer tick passed in by the caller."""

import heapq
import random

from raft.messages import Message


class SimNetwork:
    """Delays, drops, duplicates and partitions messages. Everything random comes from `rng`.

    Each message gets an independent random delay in [min_delay, max_delay] ticks,
    so messages can arrive out of order.
    """

    def __init__(
        self,
        rng: random.Random,
        min_delay: int = 1,
        max_delay: int = 3,
        drop_rate: float = 0.0,
        dup_rate: float = 0.0,
    ) -> None:
        if not 0 <= min_delay <= max_delay:
            raise ValueError(f"need 0 <= min_delay <= max_delay, got {min_delay}, {max_delay}")
        self._rng = rng
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._drop_rate = drop_rate
        self._dup_rate = dup_rate
        # Heap of (due tick, send order, message). The send order is unique, so
        # ties on the due tick are broken first-sent-first and messages are never compared.
        self._queue: list[tuple[int, int, Message]] = []
        self._sent = 0
        self._group_of: dict[str, int] = {}  # empty = no partition

    def send(self, msg: Message, now: int) -> None:
        """Queue `msg` for delivery, unless it is dropped. May queue a second copy.

        Partitions are not checked here, only at delivery.
        """
        if self._rng.random() < self._drop_rate:
            return
        copies = 2 if self._rng.random() < self._dup_rate else 1
        for _ in range(copies):
            due = now + self._rng.randint(self._min_delay, self._max_delay)
            heapq.heappush(self._queue, (due, self._sent, msg))
            self._sent += 1

    def deliver_due(self, now: int) -> list[Message]:
        """Remove and return messages due at or before `now`, by (due tick, send order).

        A message whose sender and receiver are in different partition groups
        right now is dropped for good, even if it was sent before the partition.
        """
        delivered = []
        while self._queue and self._queue[0][0] <= now:
            _, _, msg = heapq.heappop(self._queue)
            if self._group(msg.src) == self._group(msg.dst):
                delivered.append(msg)
        return delivered

    def partition(self, groups: list[set[str]]) -> None:
        """Replace any current partition. Nodes in different groups cannot talk.

        Nodes not listed in any group form one more group together.
        """
        group_of: dict[str, int] = {}
        for i, group in enumerate(groups):
            for node in group:
                if node in group_of:
                    raise ValueError(f"{node} is in more than one group")
                group_of[node] = i
        self._group_of = group_of

    def heal(self) -> None:
        self._group_of = {}

    def _group(self, node: str) -> int:
        return self._group_of.get(node, -1)  # -1: the group of unlisted nodes
