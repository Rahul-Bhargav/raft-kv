"""Tests for the safety checker itself (tests/safety_checks.py): each property must fire on a
hand-built bad state that correct Raft code can't reach, and a real run must be fully recorded."""

import pytest

from raft.messages import Entry
from raft.node import Role
from sim.cluster import SimCluster
from tests.safety_checks import SafetyChecker, SafetyViolation, checked_cluster


def put(key: str, value: str, term: int) -> Entry:
    return Entry(term, {"op": "set", "key": key, "value": value})


def give_log(cluster: SimCluster, node_id: str, *entries: Entry) -> None:
    cluster.storages[node_id].truncate_from(1)
    cluster.storages[node_id].append(entries)


def make_leader(cluster: SimCluster, node_id: str, term: int) -> None:
    cluster.storages[node_id].set_term_and_vote(term, node_id)
    cluster.nodes[node_id].role = Role.LEADER


def violation(message: str):
    """Expect a SafetyViolation whose message contains `message`: property, tick, nodes, index."""
    return pytest.raises(SafetyViolation, match=message)


def test_election_safety_remembers_every_term_s_leader():
    # n1 leads term 1 and crashes. Later an event shows n2 leading term 1 too.
    cluster, check = SimCluster(seed=0), SafetyChecker()
    make_leader(cluster, "n1", term=1)
    check(cluster)
    cluster.crash("n1")
    cluster.now = 5
    cluster.events.append((5, "n2", "became leader (term 1)"))
    with violation("Election Safety violated at tick 5: n2 and n1 both led term 1"):
        check(cluster)


def test_log_matching_fires_when_logs_share_an_index_and_term_but_differ():
    cluster = SimCluster(seed=0)
    give_log(cluster, "n1", put("a", "1", term=1))
    give_log(cluster, "n2", put("a", "2", term=1))
    with violation("Log Matching violated at tick 0: n2 has index 1 from term 1"):
        SafetyChecker()(cluster)


def test_leader_completeness_fires_when_a_later_leader_lacks_a_committed_entry():
    cluster = SimCluster(seed=0)
    give_log(cluster, "n1", put("a", "1", term=1))
    cluster.storages["n1"].set_term_and_vote(1, None)
    cluster.nodes["n1"].commit_index = 1
    make_leader(cluster, "n2", term=2)  # with an empty log
    with violation("Leader Completeness violated at tick 0: n2 leads term 2 but lacks index 1"):
        SafetyChecker()(cluster)


def test_state_machine_safety_remembers_what_a_node_applied_before_restarting():
    # n1 applies index 1 from term 1, crashes, and after its restart applies a different index 1.
    cluster, check = SimCluster(seed=0), SafetyChecker()
    give_log(cluster, "n1", put("a", "1", term=1))
    cluster.nodes["n1"].commit_index = cluster.nodes["n1"].last_applied = 1
    check(cluster)
    cluster.crash("n1")
    give_log(cluster, "n1", put("a", "2", term=2))
    cluster.restart("n1")
    cluster.nodes["n1"].commit_index = cluster.nodes["n1"].last_applied = 1
    with violation("State Machine Safety violated at tick 0: n1 applied index 1 from term 2") as e:
        check(cluster)
    assert "but n1 (before restarting) applied index 1 from term 1" in str(e.value)


def test_no_lost_acknowledged_writes_fires_when_a_node_s_kv_store_lacks_the_write():
    # n1 leads term 1 and applies its own write: acknowledged. n2 applies it, but its KV lacks it.
    cluster, check = SimCluster(seed=0), SafetyChecker()
    write = put("k", "v", term=1)
    make_leader(cluster, "n1", term=1)
    give_log(cluster, "n1", write)
    cluster.kvs["n1"].apply(write.command)
    cluster.nodes["n1"].commit_index = cluster.nodes["n1"].last_applied = 1
    check(cluster)
    give_log(cluster, "n2", write)
    cluster.nodes["n2"].commit_index = cluster.nodes["n2"].last_applied = 1
    with violation("No lost acknowledged writes violated at tick 0: n2 has applied up to index 1"):
        check(cluster)


def test_checker_follows_a_real_run():
    # A leader crash, writes before and after it, and a restart: the checker saw all of it.
    cluster = checked_cluster(seed=0)
    check = cluster.checkers[0]
    assert cluster.run_until(lambda: cluster.leader() is not None, 300)
    acknowledged = [f"k{i}" for i in range(5) if cluster.write(f"k{i}", f"v{i}")]
    old_leader = cluster.leader()
    cluster.crash(old_leader)
    assert cluster.run_until(lambda: cluster.leader() is not None, 300)
    acknowledged += [f"k{i}" for i in range(5, 10) if cluster.write(f"k{i}", f"v{i}")]
    cluster.restart(old_leader)
    cluster.run(50)
    led = {int(e.split()[-1][:-1]) for _, _, e in cluster.events if e.startswith("became leader")}
    assert len(led) >= 2 and set(check.leaders) == led
    assert len(acknowledged) == 10
    assert sorted(e.command["key"] for e, _ in check.acknowledged.values()) == sorted(acknowledged)
    last_applied = max(node.last_applied for node in cluster.nodes.values())
    assert sorted(check.applied) == list(range(1, last_applied + 1))
    assert len(check.committed) == last_applied
