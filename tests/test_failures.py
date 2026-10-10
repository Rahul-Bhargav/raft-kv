"""Failure scenarios across a simulated cluster: crashes, partitions and lost majorities.

They mirror the demo scenarios in DESIGN.md. Each test's docstring tells its story: what fails, what
Raft must do about it, and what we check. Seeds are fixed, so every run is the same.

Scripted tests use SCRIPTED: election timeouts far longer than the test, so no node stands for
election on its own. The test picks who stands, and when, with SimCluster.time_out().
"""

import pytest

from raft.node import RaftConfig, Role
from sim.cluster import SimCluster

# Bounds, in ticks, with about 2x headroom over the worst case measured over these seeds.
ELECTION_BOUND = 150  # test_election.py measured the worst first election at 84
# The three nodes left without the old leader need all three votes, so split votes are common.
NEW_LEADER_BOUND = 250  # measured worst: 125
RECOVERY_BOUND = 110  # a leader again once a third node restarts; measured worst: 53
# A follower applies an entry when the next AppendEntries brings the new commit index: at most
# one heartbeat interval (10) plus the largest delay later. After a heal the old leader steps down
# and catches up within that same window (measured worst: 9).
FOLLOWER_APPLY_BOUND = 20
NO_MAJORITY_TICKS = 1000  # about 20 election timeouts: plenty of chances to (wrongly) elect someone

SCRIPTED = RaftConfig(election_timeout_min=1000, election_timeout_max=1001)
# A RequestVote and its reply take at most 3 + 3 ticks with the default network delays.
CAMPAIGN_TICKS = 10


def elect(cluster: SimCluster, max_ticks: int = ELECTION_BOUND) -> str:
    assert cluster.run_until(lambda: cluster.leader() is not None, max_ticks), (
        f"no leader within {max_ticks} ticks"
    )
    return cluster.leader()


def campaign(cluster: SimCluster, node_id: str) -> bool:
    """Make `node_id` stand for election now. True if it wins within CAMPAIGN_TICKS."""
    cluster.time_out(node_id)
    return cluster.run_until(lambda: cluster.nodes[node_id].role == Role.LEADER, CAMPAIGN_TICKS)


def leaders_of_term(cluster: SimCluster, term: int) -> list[str]:
    """Every node that ever became leader in `term`, from the cluster's event record."""
    return [n for _, n, event in cluster.events if event == f"became leader (term {term})"]


def keys_in_logs(cluster: SimCluster) -> dict[str, list[str]]:
    """The keys each node's log writes, in log order (noops left out)."""
    return {
        node_id: [e.command["key"] for e in storage.entries_from(1) if "key" in e.command]
        for node_id, storage in cluster.storages.items()
    }


# --- R1: a vote survives a crash ---


@pytest.mark.parametrize("seed", range(10))  # the seed changes only the message delays
def test_restart_does_not_double_vote(seed: int) -> None:
    """R1: a node that crashes right after voting still remembers the vote when it comes back.

    In term 1, n3 votes for n1 and n1 wins. Then n3 crashes, and it comes back to a changed
    network: it can reach n2 and n5, but not n1. n2 never heard of n1's election, so it stands
    in term 1 too. It has its own vote and n5's, and one more would make a majority (3 of 5).
    If n3 had forgotten its vote, it would give it to n2, and term 1 would have two leaders, each
    free to commit different entries at the same index. R1 saves the vote before the reply that
    announces it is queued, so the restarted n3 refuses.
    We check: n3 comes back still voted for n1, n2 doesn't win, and n1 is term 1's only leader.
    """
    cluster = SimCluster(seed=seed, config=SCRIPTED)
    cluster.partition({"n1", "n3", "n4"}, {"n2", "n5"})  # n2 can't hear about n1's election
    assert campaign(cluster, "n1")  # term 1, with votes from n3 and n4
    assert cluster.storages["n3"].voted_for == "n1"

    # n1's first AppendEntries is still on its way, so n3 restarts with an empty log, like n2's.
    # The new split also keeps it that way. So R3's log check can't be what stops n2 below.
    cluster.crash("n3")
    cluster.partition({"n1", "n4"}, {"n2", "n3", "n5"})
    cluster.restart("n3")
    n3 = cluster.nodes["n3"]
    assert (n3.term, n3.voted_for, n3.storage.last_index()) == (1, "n1", 0)

    assert not campaign(cluster, "n2")  # also term 1
    assert cluster.nodes["n2"].term == 1
    assert cluster.storages["n5"].voted_for == "n2"  # n2 had 2 votes; n3's would have made 3
    assert cluster.nodes["n3"].voted_for == "n1"
    assert leaders_of_term(cluster, 1) == ["n1"]


# --- demo scenario 3: partition {leader + 1} | {the other 3}, write to both sides, heal ---


def split_off_the_leader_and_one_follower(cluster: SimCluster) -> tuple[str, str, str]:
    """Commit a=1 everywhere, then cut the leader and one follower off from the other three.

    A client still talking to the old leader writes lost=x there, and the other three elect a
    new leader, which takes b=2. Returns (old leader, the follower cut off with it, new leader).
    """
    old = elect(cluster)
    assert cluster.write("a", "1")
    cluster.run(FOLLOWER_APPLY_BOUND)
    buddy = next(node_id for node_id in cluster.ids if node_id != old)
    cluster.partition({old, buddy})  # the three nodes not named form the other side
    # The old leader is the only leader for now, so write() proposes there. It waits the whole
    # NEW_LEADER_BOUND and fails, while the other side elects a new leader in the meantime.
    assert not cluster.write("lost", "x", max_ticks=NEW_LEADER_BOUND)
    assert cluster.run_until(lambda: cluster.leader() not in {old, buddy}, NEW_LEADER_BOUND)
    new = cluster.leader()  # the leader with the highest term
    assert cluster.write("b", "2")  # three of five is a majority
    return old, buddy, new


def test_minority_leader_cannot_commit():
    """A leader cut off with only one follower keeps accepting writes, but can never commit them.

    The old leader doesn't know it's cut off (pre-vote and check-quorum are out of scope), so it
    stays leader and takes a client's write. It reaches its one follower: two copies of five, short
    of the three needed to commit. Meanwhile the other three elect a new leader in a higher term
    and commit a write of their own.
    We check: lost=x is in exactly two logs, the old side's commit index still points at a=1, the
    old side has applied only a=1, and the majority side has applied b=2 as well.
    """
    for seed in range(20):
        cluster = SimCluster(seed=seed)
        old, buddy, new = split_off_the_leader_and_one_follower(cluster)
        old_node, buddy_node = cluster.nodes[old], cluster.nodes[buddy]
        assert old_node.role == Role.LEADER, f"seed {seed}"  # it still thinks it leads...
        assert old_node.term < cluster.nodes[new].term, f"seed {seed}"  # ...an older term
        holders = sorted(n for n, keys in keys_in_logs(cluster).items() if "lost" in keys)
        assert holders == sorted([old, buddy]), f"seed {seed}"
        # Committing stopped at a=1, the last entry from before the split, on both old-side nodes.
        assert buddy_node.commit_index == old_node.commit_index, f"seed {seed}"
        committed = cluster.storages[old].entry_at(old_node.commit_index)
        assert committed.command == {"op": "set", "key": "a", "value": "1"}, f"seed {seed}"
        cluster.run(FOLLOWER_APPLY_BOUND)  # so the majority-side followers have applied b=2 too
        for node_id in cluster.ids:
            expected = {"a": "1"} if node_id in {old, buddy} else {"a": "1", "b": "2"}
            assert cluster.kvs[node_id].data == expected, f"seed {seed}, {node_id}"


def test_stale_leader_steps_down():
    """R2: when the partition heals, the old leader sees the higher term and steps down.

    Continues the minority-leader story. Once healed, the old leader's heartbeats reach the
    majority side and the new leader's heartbeats reach the old one. Either way it sees a higher
    term, in a request or a reply, and R2 makes it a follower in that term. The new leader then
    makes both old-side logs match its own: its entries conflict with lost=x at the same index,
    and lost=x was never committed, so it is deleted and replaced (R5).
    We check: the old leader is now a follower in the new term, lost=x is gone from every log, every
    log matches the new leader's, and every node has applied a=1 and b=2 and nothing else.
    """
    for seed in range(20):
        cluster = SimCluster(seed=seed)
        old, buddy, new = split_off_the_leader_and_one_follower(cluster)
        cluster.heal()
        cluster.run(FOLLOWER_APPLY_BOUND)
        old_node = cluster.nodes[old]
        assert old_node.role == Role.FOLLOWER, f"seed {seed}"
        assert (old_node.term, old_node.leader_id) == (cluster.nodes[new].term, new), seed
        assert cluster.leader() == new, f"seed {seed}"
        new_log = cluster.storages[new].entries_from(1)
        for node_id in cluster.ids:
            assert cluster.storages[node_id].entries_from(1) == new_log, f"seed {seed}, {node_id}"
            assert cluster.kvs[node_id].data == {"a": "1", "b": "2"}, f"seed {seed}, {node_id}"
        assert all("lost" not in keys for keys in keys_in_logs(cluster).values()), f"seed {seed}"


# --- demo scenario 4: crash 3 of 5, then restart one ---


def crash_the_leader_and_two_followers(cluster: SimCluster) -> tuple[list[str], list[str]]:
    """Commit a=1 everywhere, then crash 3 of 5, leader included. Returns (crashed, survivors)."""
    leader_id = elect(cluster)
    assert cluster.write("a", "1")
    cluster.run(FOLLOWER_APPLY_BOUND)
    followers = [node_id for node_id in cluster.ids if node_id != leader_id]
    crashed, survivors = [leader_id] + followers[:2], followers[2:]
    for node_id in crashed:
        cluster.crash(node_id)
    return crashed, survivors


def test_no_majority_means_no_leader_and_no_commits():
    """With 3 of 5 nodes down, the two survivors can never elect a leader or commit anything.

    A candidate needs 3 votes, and two nodes have only 2 between them. The survivors keep timing
    out and standing for election in ever higher terms, and every election fails. With no leader,
    nobody appends or commits. (We crash the leader too: a leader left in a minority would keep
    calling itself leader, since check-quorum is out of scope, but couldn't commit either. That
    case is test_minority_leader_cannot_commit.)
    We check, for NO_MAJORITY_TICKS: no leader at any tick, the survivors' terms keep rising (so
    they really did try), their logs and commit indexes don't change, and a write fails.
    """
    for seed in range(20):
        cluster = SimCluster(seed=seed)
        _, survivors = crash_the_leader_and_two_followers(cluster)
        before = {n: cluster.nodes[n] for n in survivors}  # no restarts here: same objects
        terms = {n: node.term for n, node in before.items()}
        commit_indexes = {n: node.commit_index for n, node in before.items()}
        logs = {n: cluster.storages[n].entries_from(1) for n in survivors}

        def no_leader(c: SimCluster) -> None:
            assert c.leader() is None, f"seed {seed}: {c.leader()} leads at tick {c.now}"

        cluster.checkers.append(no_leader)
        cluster.run(NO_MAJORITY_TICKS)
        for n, node in before.items():
            # Measured: at least 22 more terms on every seed. Half that shows they kept trying.
            assert node.term >= terms[n] + 10, f"seed {seed}, {n}: too few elections to test much"
            assert node.commit_index == commit_indexes[n], f"seed {seed}, {n}"
            assert cluster.storages[n].entries_from(1) == logs[n], f"seed {seed}, {n}"
            assert cluster.kvs[n].data == {"a": "1"}, f"seed {seed}, {n}"
        assert not cluster.write("b", "2"), f"seed {seed}"


def test_restarting_one_node_restores_progress():
    """Bring one of the three crashed nodes back, and the cluster has a majority again.

    Continues the no-majority story. With 3 of 5 live, a candidate can collect 3 votes, so a
    leader is elected and commits again. The restarted node has kept its log on disk, adopts the
    survivors' higher term from their first message (R2), and rebuilds its KV store by
    re-applying committed entries.
    We check: a leader within RECOVERY_BOUND, a new write succeeds, and all three live nodes end
    up with both the old write and the new one.
    """
    for seed in range(20):
        cluster = SimCluster(seed=seed)
        crashed, survivors = crash_the_leader_and_two_followers(cluster)
        cluster.run(300)  # a while with no majority, so the survivors' terms run ahead
        cluster.restart(crashed[0])  # the old leader; it comes back as a follower
        assert cluster.run_until(lambda: cluster.leader() is not None, RECOVERY_BOUND), seed
        assert cluster.write("b", "2"), f"seed {seed}"
        cluster.run(FOLLOWER_APPLY_BOUND)
        for node_id in [crashed[0], *survivors]:
            assert cluster.kvs[node_id].data == {"a": "1", "b": "2"}, f"seed {seed}, {node_id}"
