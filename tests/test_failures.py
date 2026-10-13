"""Failure scenarios across a simulated cluster: crashes, partitions and lost majorities.

They mirror the demo scenarios in DESIGN.md. Each test's docstring tells its story: what fails, what
Raft must do about it, and what we check. Seeds are fixed, so every run is the same.

Scripted tests use SCRIPTED: election timeouts far longer than the test, so no node stands for
election on its own. The test picks who stands, and when, with SimCluster.time_out().
"""

from dataclasses import replace

import pytest

from raft.node import RaftConfig, RaftNode, Role
from sim.cluster import SimCluster
from tests.safety_checks import checked_cluster

# Deadlines, in ticks: how long a test waits for something to happen before it fails.
# Each is about twice the slowest case measured over these seeds.
ELECTION_DEADLINE = 150  # test_election.py measured the worst first election at 84
# The three nodes left without the old leader need all three votes, so split votes are common.
NEW_LEADER_DEADLINE = 250  # measured worst: 125
RECOVERY_DEADLINE = 110  # a leader again once a third node restarts; measured worst: 53
# A follower applies an entry when the next AppendEntries brings the new commit index: at most
# one heartbeat interval (10) plus the largest delay later. After a heal the old leader steps down
# and catches up within that same window (measured worst: 9).
FOLLOWER_APPLY_DEADLINE = 20
NO_MAJORITY_TICKS = 1000  # about 20 election timeouts: plenty of chances to (wrongly) elect someone

SCRIPTED = RaftConfig(election_timeout_min=1000, election_timeout_max=1001)
# A RequestVote and its reply take at most 3 + 3 ticks with the default network delays.
CAMPAIGN_DEADLINE = 10


def elect(cluster: SimCluster, max_ticks: int = ELECTION_DEADLINE) -> str:
    assert cluster.run_until(lambda: cluster.leader() is not None, max_ticks), (
        f"no leader within {max_ticks} ticks"
    )
    return cluster.leader()


def campaign(cluster: SimCluster, node_id: str) -> bool:
    """Make `node_id` stand for election now. True if it wins within CAMPAIGN_DEADLINE."""
    cluster.time_out(node_id)
    return cluster.run_until(lambda: cluster.nodes[node_id].role == Role.LEADER, CAMPAIGN_DEADLINE)


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
    cluster = checked_cluster(seed=seed, config=SCRIPTED)
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
    cluster.run(FOLLOWER_APPLY_DEADLINE)
    buddy = next(node_id for node_id in cluster.ids if node_id != old)
    cluster.partition({old, buddy})  # the three nodes not named form the other side
    # The old leader is the only leader for now, so write() proposes there. It waits the whole
    # NEW_LEADER_DEADLINE and fails, while the other side elects a new leader in the meantime.
    assert not cluster.write("lost", "x", max_ticks=NEW_LEADER_DEADLINE)
    assert cluster.run_until(lambda: cluster.leader() not in {old, buddy}, NEW_LEADER_DEADLINE)
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
        cluster = checked_cluster(seed=seed)
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
        cluster.run(FOLLOWER_APPLY_DEADLINE)  # so the majority-side followers have applied b=2 too
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
        cluster = checked_cluster(seed=seed)
        old, buddy, new = split_off_the_leader_and_one_follower(cluster)
        cluster.heal()
        cluster.run(FOLLOWER_APPLY_DEADLINE)
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
    cluster.run(FOLLOWER_APPLY_DEADLINE)
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
        cluster = checked_cluster(seed=seed)
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
    We check: a leader within RECOVERY_DEADLINE, a new write succeeds, and all three live nodes end
    up with both the old write and the new one.
    """
    for seed in range(20):
        cluster = checked_cluster(seed=seed)
        crashed, survivors = crash_the_leader_and_two_followers(cluster)
        cluster.run(300)  # a while with no majority, so the survivors' terms run ahead
        cluster.restart(crashed[0])  # the old leader; it comes back as a follower
        assert cluster.run_until(lambda: cluster.leader() is not None, RECOVERY_DEADLINE), seed
        assert cluster.write("b", "2"), f"seed {seed}"
        cluster.run(FOLLOWER_APPLY_DEADLINE)
        for node_id in [crashed[0], *survivors]:
            assert cluster.kvs[node_id].data == {"a": "1", "b": "2"}, f"seed {seed}, {node_id}"


# --- R6: Figure 8 of the Raft paper ---
#
# Each row is one node's log, and each number is the term of the entry at that index. Stages
# (a)-(e) match the paper's, with n1-n5 standing for S1-S5 and the same term numbers. Our entries
# are each new leader's R7 noop, standing in for the paper's client entries. L = leader,
# x = crashed. Entry 1 is committed on every node before (a).
#
#             (a)      (b)       (c)        (d)        (e), instead of (d)
#   index:    1 2      1 2       1 2 3      1 2 3      1 2 3
#   n1        1 2 L    1 2 x     1 2 4 L    1 3 5      1 2 4 L
#   n2        1 2      1 2       1 2 4      1 3 5      1 2 4
#   n3        1        1         1 2        1 3 5      1 2 4
#   n4        1        1         1          1 3 5      1
#   n5        1        1 3 L,x   1 3 x      1 3 5 L    1 3 x
#
# (a) n1 leads term 2. Its entry 2 reaches only n2.
# (b) n1 crashes. n5 wins term 3 (votes: n3, n4, itself). Its entry 2 reaches nobody; it crashes.
# (c) n1 restarts and wins term 4. It copies its term-2 entry to n3: now on 3 of 5, a majority.
#     It must NOT count as committed (R6), because of what can happen next:
# (d) n1 crashes. n5 wins term 5: its log ends in term 3, later than n3's and n4's (R3). Its entry 2
#     replaces the term-2 entry everywhere, n1's too once it restarts. A node that had applied the
#     term-2 entry would have applied two different entries at index 2.
# (e) Or: n1's term-4 entry reaches n3 before any crash. That entry is from n1's own term and on
#     a majority, so it commits, and entry 2 with it. n5 can't win any more: n2 and n3 now end
#     their logs in term 4, later than n5's term 3, so they refuse it (R3).
#
# Without a batch cap, n1's message to n3 in (c) would carry entries 2 and 3 together, and the
# moment in (c) where a majority has the old entry but not n1's new one would never happen.
FIGURE_8 = replace(SCRIPTED, max_entries_per_append=1)
# One or two entries reaching a follower, one entry per heartbeat: measured worst 16 ticks.
STAGE_DEADLINE = 35
# After (d), n5 overwrites entries 2-3 on three followers by backing up: measured worst 33.
CATCH_UP_DEADLINE = 70


def live(cluster: SimCluster) -> list[RaftNode]:
    return [node for node in cluster.nodes.values() if node is not None]


def log_terms(cluster: SimCluster) -> dict[str, list[int]]:
    """Each node's log as the term of each entry: one row of the Figure 8 picture per node."""
    return {n: [entry.term for entry in s.entries_from(1)] for n, s in cluster.storages.items()}


def figure_8_up_to_c(cluster: SimCluster) -> None:
    """Script stages (a)-(c) of Figure 8, checking each stage's picture as it goes."""
    # Before (a): n2 leads term 1, and entry 1 commits on every node.
    assert campaign(cluster, "n2")
    assert cluster.run_until(lambda: all(n.commit_index == 1 for n in live(cluster)), STAGE_DEADLINE)

    # (a) n1 wins term 2. Its noop (entry 2) is on its way to everyone, so split the network
    # right away: only n2 gets it.
    assert campaign(cluster, "n1")
    cluster.partition({"n1", "n2"}, {"n3", "n4", "n5"})
    assert cluster.run_until(lambda: cluster.nodes["n1"].match_index["n2"] == 2, STAGE_DEADLINE)
    assert log_terms(cluster) == {"n1": [1, 2], "n2": [1, 2], "n3": [1], "n4": [1], "n5": [1]}

    # (b) n1 crashes. n5 wins term 3 with n3's and n4's votes, then is cut off before its noop
    # (its entry 2) reaches anyone, and crashes.
    cluster.crash("n1")
    assert campaign(cluster, "n5")
    cluster.partition({"n1", "n2"}, {"n3", "n4"})  # n5, not named, is alone
    cluster.crash("n5")
    assert log_terms(cluster)["n5"] == [1, 3]

    # (c) n1 restarts, next to n2 and n3. Its first try, in term 3, fails: n3 already voted for
    # n5 in term 3 (R3: one vote per term). It wins term 4. Its noop is entry 3. n3 lacks entry
    # 2, so n1 backs up and sends it entry 2 alone (the cap is 1), and n3 confirms it.
    cluster.restart("n1")
    cluster.partition({"n1", "n2", "n3"}, {"n4"})
    assert not campaign(cluster, "n1")
    assert campaign(cluster, "n1")
    assert cluster.nodes["n1"].term == 4
    assert cluster.run_until(lambda: cluster.nodes["n1"].match_index["n3"] == 2, STAGE_DEADLINE)
    assert log_terms(cluster) == {
        "n1": [1, 2, 4],
        "n2": [1, 2, 4],
        "n3": [1, 2],
        "n4": [1],
        "n5": [1, 3],
    }
    # The term-2 entry is on n1, n2 and n3, a majority, and still nobody counts it committed.
    # (n1's commit index is 0: it restarted, and has committed nothing in term 4 yet.)
    assert all(node.commit_index <= 1 for node in live(cluster)), "R6: entry 2 was committed"


@pytest.mark.parametrize("seed", range(10))  # the seed changes only the message delays
def test_figure8(seed: int) -> None:
    """R6: an earlier term's entry on a majority is not committed, because it can still be lost.

    Stages (a)-(c) and (d) of the diagram above. By (c), n1 leads term 4, and its entry from term 2
    is on a majority. Counting copies would call it committed, and n1 would apply it. Then in (d)
    n1 crashes, and n5 wins term 5 and replaces that entry on every node with its own from term 3.
    R6 prevents the damage: a leader commits by counting copies only entries of its own term.
    We check: at no tick does any node count the term-2 entry as committed, and in the end every
    node has n5's entry at index 2 and has committed it.
    """
    cluster = checked_cluster(seed=seed, config=FIGURE_8)

    def never_commits_the_term_2_entry(c: SimCluster) -> None:
        for node in live(c):
            assert not (node.commit_index >= 2 and node.storage.term_at(2) == 2), (
                f"tick {c.now}: {node.node_id} counts entry 2, from term 2, as committed (R6)"
            )

    cluster.checkers.append(never_commits_the_term_2_entry)
    figure_8_up_to_c(cluster)

    # (d) n1 crashes, and n5 comes back. Its first try, in term 4, fails: n2 and n3 already voted
    # for n1 in term 4. In term 5, n3 and n4 vote for it: its log ends in term 3, later than
    # theirs (R3). n2 refuses: its log ends in term 4.
    cluster.crash("n1")
    cluster.restart("n5")
    cluster.heal()
    assert not campaign(cluster, "n5")
    assert campaign(cluster, "n5")
    assert cluster.nodes["n5"].term == 5

    # n5's entries replace the term-2 entry and everything after it (R5), and they commit.
    def has_n5s_log(node_id: str) -> bool:
        node = cluster.nodes[node_id]
        return log_terms(cluster)[node_id] == [1, 3, 5] and node.commit_index == 3

    others = ["n2", "n3", "n4"]
    assert cluster.run_until(lambda: all(has_n5s_log(n) for n in others), CATCH_UP_DEADLINE)
    cluster.restart("n1")  # n1's copy of the term-2 entry goes too
    assert cluster.run_until(lambda: has_n5s_log("n1"), CATCH_UP_DEADLINE)
    entry_2 = cluster.storages["n5"].entry_at(2)
    assert all(cluster.storages[n].entry_at(2) == entry_2 for n in cluster.ids)


@pytest.mark.parametrize("seed", range(10))
def test_figure8_current_term_entry_commits_the_old_one(seed: int) -> None:
    """R6's other half: a current-term entry on a majority commits the earlier entries with it.

    Stages (a)-(c) and (e) of the diagram above. From (c), n1's next heartbeat brings its term-4
    entry to n3. It's now on a majority, so n1 commits index 3, and index 2 with it. That's safe:
    n2 and n3 now end their logs in term 4, so n5, whose log ends in term 3, can never win again,
    and any future leader has the term-4 entry and everything before it.
    We check: n1's commit index jumps from 0 straight to 3 (index 2 is never committed on its
    own), n5 loses every election it tries, and after n2 wins, every node keeps the term-2 entry.
    """
    cluster = checked_cluster(seed=seed, config=FIGURE_8)
    figure_8_up_to_c(cluster)

    n1 = cluster.nodes["n1"]
    commit_indexes = {n1.commit_index}

    def record_n1s_commit_index(c: SimCluster) -> None:
        commit_indexes.add(n1.commit_index)

    cluster.checkers.append(record_n1s_commit_index)
    assert cluster.run_until(lambda: n1.commit_index == 3, STAGE_DEADLINE)
    cluster.checkers.remove(record_n1s_commit_index)
    assert commit_indexes == {0, 3}  # entries 2 and 3 were committed in the same step
    assert log_terms(cluster)["n3"] == [1, 2, 4]

    # n1 crashes, and n5 comes back. It loses in term 4 (n2 and n3 voted for n1 in term 4), and
    # in term 5 (their logs end in term 4, later than its term 3). Only n4 votes for it.
    cluster.crash("n1")
    cluster.restart("n5")
    cluster.heal()
    assert not campaign(cluster, "n5")
    assert not campaign(cluster, "n5")
    assert campaign(cluster, "n2")  # term 6

    cluster.restart("n1")
    expected = [1, 2, 4, 6]  # n5's term-3 entry is the one replaced this time

    def caught_up() -> bool:
        return all(t == expected for t in log_terms(cluster).values()) and all(
            node.commit_index == 4 for node in live(cluster)
        )

    assert cluster.run_until(caught_up, CATCH_UP_DEADLINE)
