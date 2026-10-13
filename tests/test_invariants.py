"""Tests for the invariant checker itself (tests/invariants.py).

Each violation test hand-builds a broken cluster state, one that correct Raft code can't reach, by
setting logs, roles and indexes directly, and calls the checker once. The check must fire, and its
message must name the invariant, the tick, the nodes and the index/term. A last test runs a real
cluster, so a checker that silently checks nothing can't pass.
"""

import pytest

from raft.messages import Entry
from raft.node import Role
from sim.cluster import SimCluster
from tests.invariants import Invariants, InvariantViolation, checked_cluster


def put(key: str, value: str, term: int) -> Entry:
    return Entry(term, {"op": "set", "key": key, "value": value})


def give_log(cluster: SimCluster, node_id: str, *entries: Entry) -> None:
    storage = cluster.storages[node_id]
    storage.truncate_from(1)
    storage.append(entries)


def make_leader(cluster: SimCluster, node_id: str, term: int) -> None:
    node = cluster.nodes[node_id]
    node.storage.set_term_and_vote(term, node_id)
    node.role = Role.LEADER


def apply_up_to(cluster: SimCluster, node_id: str, index: int, update_kv: bool = True) -> None:
    """Mark entries 1..index committed and applied on a node, as take_committed() would."""
    node = cluster.nodes[node_id]
    if update_kv:
        for i in range(node.last_applied + 1, index + 1):
            cluster.kvs[node_id].apply(node.storage.entry_at(i).command)
    node.commit_index = node.last_applied = index


def test_election_safety_remembers_every_term_s_leader():
    # n1 leads term 1 and crashes. Later an event shows n2 leading term 1 too, even if only for
    # part of a step. The record outlives n1, and events count as well as roles.
    cluster, check = SimCluster(seed=0), Invariants()
    make_leader(cluster, "n1", term=1)
    check(cluster)
    cluster.crash("n1")
    cluster.now = 5
    cluster.events.append((5, "n2", "became leader (term 1)"))
    with pytest.raises(InvariantViolation) as error:
        check(cluster)
    assert str(error.value) == (
        "Election Safety violated at tick 5: n2 and n1 both led term 1 (n1 since tick 0)"
    )


@pytest.mark.parametrize(
    "n1_log, n2_log, expected",
    [
        # Same index and term, different commands.
        (
            [put("a", "1", term=1)],
            [put("a", "2", term=1)],
            "n2 has index 1 from term 1 = {'op': 'set', 'key': 'a', 'value': '2'}, after a term-0 "
            "entry, but n1 had index 1 from term 1 = {'op': 'set', 'key': 'a', 'value': '1'}",
        ),
        # Index 2 matches exactly, but the logs differ before it.
        (
            [put("a", "1", term=1), put("b", "1", term=2)],
            [put("a", "2", term=2), put("b", "1", term=2)],
            "n2 has index 2 from term 2 = {'op': 'set', 'key': 'b', 'value': '1'}, after a "
            "term-2 entry, but n1 had index 2 from term 2 = {'op': 'set', 'key': 'b', 'value': "
            "'1'}, after a term-1 entry",
        ),
    ],
)
def test_log_matching_fires_when_logs_share_an_index_and_term_but_differ(n1_log, n2_log, expected):
    cluster = SimCluster(seed=0)
    give_log(cluster, "n1", *n1_log)
    give_log(cluster, "n2", *n2_log)
    with pytest.raises(InvariantViolation, match="^Log Matching violated at tick 0: ") as error:
        Invariants()(cluster)
    assert expected in str(error.value)


def test_leader_completeness_fires_when_a_later_leader_lacks_a_committed_entry():
    cluster = SimCluster(seed=0)
    give_log(cluster, "n1", put("a", "1", term=1))
    cluster.storages["n1"].set_term_and_vote(1, None)
    cluster.nodes["n1"].commit_index = 1
    make_leader(cluster, "n2", term=2)  # with an empty log
    with pytest.raises(InvariantViolation) as error:
        Invariants()(cluster)
    assert str(error.value) == (
        "Leader Completeness violated at tick 0: n2 leads term 2 but lacks index 1 from term 1 = "
        "{'op': 'set', 'key': 'a', 'value': '1'}, committed by term 1 (n1 showed it committed at "
        "tick 0): its log ends at index 0"
    )


def test_leader_completeness_does_not_bind_a_stale_leader():
    # n2 leads term 1 and is cut off; the entry was committed in term 2. A term-1 leader can't be
    # expected to have it: only leaders of later terms must.
    cluster = SimCluster(seed=0)
    give_log(cluster, "n1", put("a", "1", term=2))
    cluster.storages["n1"].set_term_and_vote(2, None)
    cluster.nodes["n1"].commit_index = 1
    make_leader(cluster, "n2", term=1)
    Invariants()(cluster)  # no violation


def test_state_machine_safety_remembers_what_a_node_applied_before_restarting():
    # The case T13 found with R6 removed, in miniature: n1 applies index 1 from term 1, crashes, and
    # after its restart applies a different index 1, from term 2. After the restart, nothing in the
    # cluster's state shows that n1 ever held the term-1 entry.
    cluster, check = SimCluster(seed=0), Invariants()
    give_log(cluster, "n1", put("a", "1", term=1))
    apply_up_to(cluster, "n1", 1)
    check(cluster)
    cluster.crash("n1")
    give_log(cluster, "n1", put("a", "2", term=2))
    cluster.restart("n1")
    apply_up_to(cluster, "n1", 1)
    cluster.now = 9
    with pytest.raises(InvariantViolation) as error:
        check(cluster)
    assert str(error.value) == (
        "State Machine Safety violated at tick 9: n1 applied index 1 from term 2 = "
        "{'op': 'set', 'key': 'a', 'value': '2'}, but n1 (before restarting) applied index 1 from "
        "term 1 = {'op': 'set', 'key': 'a', 'value': '1'} at tick 0"
    )


def test_a_node_that_deletes_committed_entries_is_reported_not_a_crash():
    # With R5 broken, a node can apply an entry and delete it within one step. Then the checker
    # can't read the entry it applied, and must say so rather than raise an IndexError.
    cluster = SimCluster(seed=0)
    give_log(cluster, "n1", put("a", "1", term=1))
    cluster.nodes["n1"].commit_index = cluster.nodes["n1"].last_applied = 2
    with pytest.raises(InvariantViolation) as error:
        Invariants()(cluster)
    assert str(error.value) == (
        "State Machine Safety violated at tick 0: n1 has committed up to index 2, but its log ends "
        "at index 1: it deleted entries it had committed"
    )


def test_a_node_that_later_deletes_a_committed_entry_is_reported():
    cluster, check = SimCluster(seed=0), Invariants()
    give_log(cluster, "n1", put("a", "1", term=1), put("b", "1", term=1))
    apply_up_to(cluster, "n1", 2)
    check(cluster)
    cluster.storages["n1"].truncate_from(2)  # its commit index stays at 2
    with pytest.raises(InvariantViolation) as error:
        check(cluster)
    assert str(error.value) == (
        "State Machine Safety violated at tick 0: n1 deleted or replaced index 2 (from term 1), "
        "but it had committed up to index 2"
    )


def test_no_lost_acknowledged_writes_fires_when_a_node_s_kv_store_lacks_the_write():
    # n1 leads term 1 and applies its own write: that acknowledges it. n2 then applies the same
    # entry, but its KV store doesn't show it (as if the driver skipped an entry).
    cluster, check = SimCluster(seed=0), Invariants()
    write = put("k", "v", term=1)
    make_leader(cluster, "n1", term=1)
    give_log(cluster, "n1", write)
    apply_up_to(cluster, "n1", 1)
    check(cluster)
    assert check.acknowledged == {1: (write, 0)}
    give_log(cluster, "n2", write)
    apply_up_to(cluster, "n2", 1, update_kv=False)
    cluster.now = 4
    with pytest.raises(InvariantViolation) as error:
        check(cluster)
    assert str(error.value) == (
        "No lost acknowledged writes violated at tick 4: n2 has applied up to index 1, past the "
        "write k='v' acknowledged at tick 0 (index 1, term 1), but its KV store has k=None where "
        "the entries it applied give k='v'"
    )


def test_no_lost_acknowledged_writes_allows_a_later_write_to_the_same_key():
    cluster = SimCluster(seed=0)
    make_leader(cluster, "n1", term=1)
    give_log(cluster, "n1", put("k", "v", term=1), put("k", "w", term=1))
    apply_up_to(cluster, "n1", 2)  # its KV store now has k=w
    Invariants()(cluster)  # no violation


def test_checker_follows_a_real_run():
    # A leader crash, writes before and after it, and a restart. The checker must have seen every
    # leader, every write that write() acknowledged and nothing else, and every applied index.
    cluster = checked_cluster(seed=0)
    check = cluster.checkers[0]
    assert isinstance(check, Invariants)
    acknowledged = []

    def write_some(first: int) -> None:
        assert cluster.run_until(lambda: cluster.leader() is not None, 300)
        for i in range(first, first + 5):
            if cluster.write(f"k{i}", f"v{i}"):
                acknowledged.append(f"k{i}")

    write_some(0)
    old_leader = cluster.leader()
    cluster.crash(old_leader)
    write_some(5)
    cluster.restart(old_leader)
    cluster.run(50)

    led = {int(e.split()[-1][:-1]) for _, _, e in cluster.events if e.startswith("became leader")}
    assert len(led) >= 2 and set(check.leaders) == led
    assert len(acknowledged) == 10
    assert sorted(e.command["key"] for e, _ in check.acknowledged.values()) == sorted(acknowledged)
    last_applied = max(node.last_applied for node in cluster.nodes.values())
    assert sorted(check.applied) == list(range(1, last_applied + 1))
    assert len(check.committed) == last_applied
