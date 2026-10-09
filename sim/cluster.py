"""A deterministic cluster of RaftNodes on a fake clock and a SimNetwork, for tests.

Each step ticks every live node, then delivers the messages due by then. All randomness (network
and election timeouts) comes from one random.Random(seed), so a seed always gives the same run.
"""

import random
from collections.abc import Callable, Iterable

from raft.kv import KVStore
from raft.node import RaftConfig, RaftNode, Role
from raft.storage import MemoryStorage
from sim.network import SimNetwork


class SimCluster:
    def __init__(
        self, n: int = 5, seed: int = 0, config: RaftConfig = RaftConfig(), **network_kwargs
    ) -> None:
        self.rng = random.Random(seed)
        self.config = config
        self.ids = [f"n{i}" for i in range(1, n + 1)]
        self.network = SimNetwork(self.rng, **network_kwargs)
        self.storages = {node_id: MemoryStorage() for node_id in self.ids}  # the "disks"
        self.kvs = {node_id: KVStore() for node_id in self.ids}
        self.nodes: dict[str, RaftNode | None] = {
            node_id: self._new_node(node_id) for node_id in self.ids  # None while crashed
        }
        self.now = 0
        self.checkers: list[Callable[["SimCluster"], None]] = []  # invariants, run after every step
        self.events: list[tuple[int, str, str]] = []  # (tick, node_id, event), for debugging

    # --- running ---

    def step(self) -> None:
        """Advance one tick: tick every live node, then deliver what is due to live nodes."""
        self.now += 1
        for node in self._live_nodes():
            node.tick()
            self._collect(node)
        for msg in self.network.deliver_due(self.now):
            node = self.nodes[msg.dst]
            if node is not None:  # a message to a crashed node is lost
                node.receive(msg)
                self._collect(node)
        for check in self.checkers:
            check(self)

    def run(self, ticks: int) -> None:
        for _ in range(ticks):
            self.step()

    def run_until(self, cond: Callable[[], bool], max_ticks: int = 2000) -> bool:
        """Step until `cond()` holds, for at most `max_ticks` steps. Returns whether it held."""
        for _ in range(max_ticks):
            if cond():
                return True
            self.step()
        return cond()

    # --- clients ---

    def write(self, key: str, value: str, max_ticks: int = 1000) -> bool:
        """Write key=value through the current leader, as a client would. True once it took effect.

        Proposes on the leader, then runs until that node has applied the entry's index. True
        only if what it applied there is our entry (same term), like the demo server's 200.
        False if there is no leader, if a later leader overwrote the entry (503), or if it wasn't
        applied within max_ticks (504). A write that returned False may still be applied later.
        """
        leader_id = self.leader()
        if leader_id is None:
            return False
        node = self.nodes[leader_id]
        assert node is not None  # leader() only returns live nodes...
        proposed = node.propose({"op": "set", "key": key, "value": value})
        assert proposed is not None  # ...that are leaders
        self._collect(node)  # send its AppendEntries, as after every call into a node
        index, term = proposed

        def applied_there() -> bool:
            current = self.nodes[leader_id]  # it may have crashed, or restarted as a new object
            return current is not None and current.last_applied >= index

        if not self.run_until(applied_there, max_ticks):
            return False
        # A committed entry never changes, so the term stored at `index` is the term applied.
        # A different term means a later leader replaced our entry before it was committed.
        return self.storages[leader_id].term_at(index) == term

    # --- observing ---

    def leader(self) -> str | None:
        """The live leader with the highest term, or None.

        A leader cut off by a partition can linger in a lower term, so there may be more than one.
        """
        leaders = [node for node in self._live_nodes() if node.role == Role.LEADER]
        if not leaders:
            return None
        return max(leaders, key=lambda node: node.term).node_id

    # --- faults ---

    def crash(self, node_id: str) -> None:
        """Stop a node. Its volatile state is gone; its storage is kept for restart()."""
        if self.nodes[node_id] is None:
            raise ValueError(f"{node_id} is already crashed")
        self.nodes[node_id] = None

    def restart(self, node_id: str) -> None:
        """Bring a crashed node back from its storage, with fresh volatile state and KV store."""
        if self.nodes[node_id] is not None:
            raise ValueError(f"{node_id} is not crashed")
        self.kvs[node_id] = KVStore()  # rebuilt by re-applying committed entries
        self.nodes[node_id] = self._new_node(node_id)

    def partition(self, *groups: Iterable[str]) -> None:
        """Split the network into groups (see SimNetwork.partition), replacing any earlier split."""
        self.network.partition([set(group) for group in groups])

    def heal(self) -> None:
        self.network.heal()

    # --- internals ---

    def _new_node(self, node_id: str) -> RaftNode:
        peers = [other for other in self.ids if other != node_id]
        # Each node gets its own Random, seeded from the cluster's, so its election timeouts
        # are independent of the network's draws but still fixed by the cluster seed.
        rng = random.Random(self.rng.getrandbits(64))
        return RaftNode(node_id, peers, self.storages[node_id], rng, self.config)

    def _live_nodes(self) -> list[RaftNode]:
        return [node for node in self.nodes.values() if node is not None]

    def _collect(self, node: RaftNode) -> None:
        """Collect a node's outputs, as DESIGN.md requires after every call into it."""
        for msg in node.take_messages():
            self.network.send(msg, self.now)
        for _index, entry in node.take_committed():
            self.kvs[node.node_id].apply(entry.command)
        for event in node.take_events():
            self.events.append((self.now, node.node_id, event))
