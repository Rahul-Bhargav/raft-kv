"""Replication across a whole simulated cluster. Seeds are fixed, so every run is the same."""

import random

from raft.messages import Entry
from raft.node import Role
from sim.cluster import SimCluster
from tests.safety_checks import checked_cluster

ELECTION_DEADLINE = 150  # ticks; test_election.py measured the worst first election at 84
# A follower applies an entry when the next AppendEntries brings the new commit index: at most
# one heartbeat interval (10) plus the largest delay later.
FOLLOWER_APPLY_DEADLINE = 20


def elect(cluster: SimCluster, max_ticks: int = ELECTION_DEADLINE) -> str:
    assert cluster.run_until(lambda: cluster.leader() is not None, max_ticks), (
        f"no leader within {max_ticks} ticks"
    )
    return cluster.leader()


def log(cluster: SimCluster, node_id: str) -> list[Entry]:
    return cluster.storages[node_id].entries_from(1)


def converged(cluster: SimCluster) -> bool:
    """Every node is live and has applied the same entries, with the same result."""
    nodes = list(cluster.nodes.values())
    if any(node is None for node in nodes):
        return False
    first = cluster.kvs[cluster.ids[0]].data
    return len({node.last_applied for node in nodes}) == 1 and all(
        kv.data == first for kv in cluster.kvs.values()
    )


def test_a_write_applies_on_every_node():
    for seed in range(20):
        cluster = checked_cluster(seed=seed)
        leader_id = elect(cluster)
        assert cluster.write("x", "1"), f"seed {seed}"
        assert cluster.kvs[leader_id].data == {"x": "1"}  # applied there when write() returns
        cluster.run(FOLLOWER_APPLY_DEADLINE)
        for node_id in cluster.ids:
            assert cluster.kvs[node_id].data == {"x": "1"}, f"seed {seed}, {node_id}"
            assert log(cluster, node_id) == log(cluster, leader_id), f"seed {seed}, {node_id}"


def test_writes_apply_in_order():
    cluster = checked_cluster(seed=0)
    elect(cluster)
    for key, value in [("x", "1"), ("y", "2"), ("x", "3")]:
        assert cluster.write(key, value)
    cluster.run(FOLLOWER_APPLY_DEADLINE)
    for node_id in cluster.ids:
        assert cluster.kvs[node_id].data == {"x": "3", "y": "2"}, node_id


def test_write_without_a_leader_returns_false_at_once():
    cluster = checked_cluster(seed=0)  # tick 0: nobody has stood for election yet
    assert not cluster.write("x", "1")
    assert cluster.now == 0


def test_a_lagging_follower_catches_up_after_restart():
    cluster = checked_cluster(seed=1)
    leader_id = elect(cluster)
    follower = next(node_id for node_id in cluster.ids if node_id != leader_id)
    cluster.crash(follower)
    for i in range(10):  # four of five nodes are still up: a majority
        assert cluster.write(f"k{i}", f"v{i}")
    assert len(log(cluster, follower)) < len(log(cluster, leader_id))  # it missed them all

    cluster.restart(follower)
    assert cluster.kvs[follower].data == {}  # a fresh KV store, rebuilt from committed entries
    expected = {f"k{i}": f"v{i}" for i in range(10)}
    assert cluster.run_until(lambda: cluster.kvs[follower].data == expected, 100)
    assert log(cluster, follower) == log(cluster, leader_id)
    assert cluster.leader() == leader_id  # catching up didn't disturb the leader


def test_conflicting_uncommitted_entries_are_overwritten():
    cluster = checked_cluster(seed=2)
    old = elect(cluster)
    assert cluster.write("a", "1")
    cluster.run(FOLLOWER_APPLY_DEADLINE)

    # Cut the leader off. It still accepts a write, but alone it can never commit it.
    cluster.partition({old})
    assert not cluster.write("lost", "x", max_ticks=100)
    lost_index = len(log(cluster, old))
    assert log(cluster, old)[lost_index - 1].command["key"] == "lost"

    # The other four elect a new leader, which commits its own entries at that same index.
    assert cluster.run_until(lambda: cluster.leader() != old, 200)
    new = cluster.leader()
    assert cluster.write("b", "2")
    assert len(log(cluster, new)) > lost_index

    # Once healed, the old leader learns the higher term (R2), and its entry is replaced (R5).
    cluster.heal()
    cluster.run(FOLLOWER_APPLY_DEADLINE)
    assert cluster.nodes[old].role == Role.FOLLOWER
    assert log(cluster, old) == log(cluster, new)
    assert log(cluster, old)[lost_index - 1].command.get("key") != "lost"
    for node_id in cluster.ids:
        assert cluster.kvs[node_id].data == {"a": "1", "b": "2"}, node_id


def test_write_fails_when_its_entry_is_overwritten():
    # Like a 503 from the demo server: something was applied at our index, but not our entry.
    cluster = checked_cluster(seed=2)
    old = elect(cluster)
    # Commit an entry first, so every node's log is as long as the old leader's. Then the new
    # leader's noop lands on the same index as the lost write. (Without this, the new leader's
    # log can be shorter, nothing reaches that index until a later write, and write() times out.)
    assert cluster.write("a", "1")
    cluster.run(FOLLOWER_APPLY_DEADLINE)
    cluster.partition({old})
    # Heal as soon as the other four have a new leader, while write() is still waiting.
    cluster.checkers.append(lambda c: c.heal() if c.leader() != old else None)
    start = cluster.now
    assert not cluster.write("lost", "x", max_ticks=1000)
    assert cluster.now - start < 1000  # it didn't time out: the old leader applied another entry
    assert cluster.kvs[old].data == {"a": "1"}


def test_all_kv_stores_converge():
    # Writes on a lossy network (drops, duplicates, reordering). Partway through, the leader
    # crashes in the middle of a write and restarts 100 ticks later. Every acknowledged write
    # must end up on every node, nothing that wasn't written may appear, and every node must end
    # up with the same KV store. (With these seeds even the interrupted write succeeds: its entry
    # had already reached a majority, so the next leader commits it.)
    for seed in range(20):
        cluster = checked_cluster(seed=seed, max_delay=5, drop_rate=0.1, dup_rate=0.2)
        elect(cluster)
        crash_at = cluster.now + random.Random(seed).randrange(20, 120)
        crashed: list[str] = []

        def crash_and_restart_the_leader(c: SimCluster) -> None:
            # A checker runs after every step, so the crash can land inside a write().
            if c.now == crash_at:
                leader_id = c.leader()
                assert leader_id is not None, f"seed {seed}: no leader to crash"
                crashed.append(leader_id)
                c.crash(leader_id)
            elif c.now == crash_at + 100:
                c.restart(crashed[0])

        cluster.checkers.append(crash_and_restart_the_leader)
        proposed, acknowledged = {}, {}
        for i in range(40):
            elect(cluster, max_ticks=500)  # wait out the election the crash causes
            key, value = f"k{i}", f"v{i}"
            proposed[key] = value
            if cluster.write(key, value, max_ticks=200):
                acknowledged[key] = value
        assert cluster.now > crash_at + 100, f"seed {seed}: the writes ended before the faults"
        assert len(acknowledged) >= 30, f"seed {seed}: too few writes got through to test much"

        assert cluster.run_until(lambda: converged(cluster), 1000), f"seed {seed}"
        for node_id in cluster.ids:
            data = cluster.kvs[node_id].data
            assert acknowledged.items() <= data.items(), f"seed {seed}, {node_id}"
            assert data.items() <= proposed.items(), f"seed {seed}, {node_id}"
