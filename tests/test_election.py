"""Leader election across a whole simulated cluster. Seeds are fixed, so every run is the same."""

from raft.messages import Entry
from raft.node import RaftConfig, Role
from sim.cluster import SimCluster
from tests.safety_checks import checked_cluster

# Deadlines, in ticks: how long a test waits before it fails. About twice the slowest case measured.
FIRST_ELECTION_DEADLINE = 150  # measured worst: 84
RE_ELECTION_DEADLINE = 200  # measured worst: 106 after the crash


def leaders(cluster: SimCluster) -> list[str]:
    return [n.node_id for n in cluster.nodes.values() if n is not None and n.role == Role.LEADER]


def elect(cluster: SimCluster, max_ticks: int = FIRST_ELECTION_DEADLINE) -> str:
    assert cluster.run_until(lambda: cluster.leader() is not None, max_ticks), (
        f"no leader within {max_ticks} ticks"
    )
    return cluster.leader()


def assert_everyone_follows(cluster: SimCluster, leader_id: str) -> None:
    """Exactly one leader, and every live node is in its term and knows who it is."""
    assert leaders(cluster) == [leader_id]
    term = cluster.nodes[leader_id].term
    for node in cluster.nodes.values():
        if node is not None:
            assert (node.term, node.leader_id) == (term, leader_id), node.node_id


def test_one_leader_is_elected_and_everyone_follows_it():
    for seed in range(100):
        cluster = checked_cluster(seed=seed)
        leader_id = elect(cluster)
        cluster.run(30)  # a few heartbeats, so the losing candidates hear from the winner
        assert cluster.leader() == leader_id, f"seed {seed}"
        assert_everyone_follows(cluster, leader_id)


def test_split_votes_resolve():
    # Timeouts from only 5 values often split the vote. Every seed must still end with one leader.
    config = RaftConfig(election_timeout_min=30, election_timeout_max=35)
    contested = 0
    for seed in range(200):
        cluster = checked_cluster(seed=seed, config=config)
        leader_id = elect(cluster, max_ticks=500)  # measured worst: 226
        cluster.run(30)
        assert_everyone_follows(cluster, leader_id)
        if cluster.nodes[leader_id].term > 1:
            contested += 1  # the term-1 election produced no leader
    assert contested >= 60, "too few split votes: the test is not exercising them"  # measured: 112


def test_a_new_leader_is_elected_after_the_leader_crashes():
    for seed in range(50):
        cluster = checked_cluster(seed=seed)
        old_leader = elect(cluster)
        old_term = cluster.nodes[old_leader].term
        cluster.crash(old_leader)
        assert cluster.run_until(lambda: cluster.leader() is not None, RE_ELECTION_DEADLINE), seed
        new_leader = cluster.leader()
        assert new_leader != old_leader
        assert cluster.nodes[new_leader].term > old_term

        # The old leader comes back as a follower and learns the new term (R2).
        cluster.restart(old_leader)
        cluster.run(30)
        assert cluster.leader() == new_leader, f"seed {seed}"
        assert_everyone_follows(cluster, new_leader)


def test_outdated_log_cannot_win():
    # R3: n1's and n2's logs end in term 1, n3-n5's in term 2. n1 and n2 get at most 2 votes each.
    logs = {"n1": [1, 1], "n2": [1, 1, 1, 1], "n3": [1, 2], "n4": [1, 2], "n5": [1, 2]}
    outdated_campaigns = 0
    for seed in range(200):
        cluster = checked_cluster(seed=seed)
        for node_id, terms in logs.items():
            storage = cluster.storages[node_id]
            storage.append([Entry(t, {"op": "noop"}) for t in terms])
            storage.set_term_and_vote(2, None)
        elect(cluster, max_ticks=200)  # measured worst: 85
        cluster.run(100)
        assert cluster.leader() in {"n3", "n4", "n5"}, f"seed {seed}"
        assert not any(
            node_id in {"n1", "n2"} and event.startswith("became leader")
            for _, node_id, event in cluster.events
        ), f"seed {seed}: an outdated node led, if only briefly"
        outdated_campaigns += any(
            node_id in {"n1", "n2"} and event.startswith("started election")
            for _, node_id, event in cluster.events
        )
    assert outdated_campaigns >= 50, "n1/n2 rarely stood for election: the test is too weak"


def test_follower_keeps_leader_under_load():
    # R4: under back-to-back writes and duplicated, reordered messages, a live leader keeps leading.
    for seed in range(10):
        # Nothing is dropped: AppendEntries reach a follower at most 10 + 7 ticks apart, under 30.
        cluster = checked_cluster(seed=seed, min_delay=1, max_delay=8, dup_rate=0.5)
        leader_id = elect(cluster)
        term = cluster.nodes[leader_id].term
        events_before = len(cluster.events)

        def same_leader(c: SimCluster) -> None:
            assert (c.leader(), c.nodes[leader_id].term) == (leader_id, term), f"seed {seed}"

        cluster.checkers.append(same_leader)
        end = cluster.now + 3000
        writes = 0
        while cluster.now < end and writes < 3000:  # the count stops a write() that never ticks
            assert cluster.write(f"k{writes}", str(writes)), f"seed {seed}, write {writes}"
            writes += 1
        assert writes >= 200, f"seed {seed}: only {writes} writes, too little load"
        later_elections = [
            e for e in cluster.events[events_before:] if e[2].startswith("started election")
        ]
        assert later_elections == [], f"seed {seed}"
        assert_everyone_follows(cluster, leader_id)
        cluster.run(30)  # the last commit index reaches the followers with the next heartbeat
        expected = {f"k{i}": str(i) for i in range(writes)}
        for node_id in cluster.ids:
            assert cluster.kvs[node_id].data == expected, f"seed {seed}, {node_id}"


def test_same_seed_gives_the_same_run():
    def events(seed: int) -> list[tuple[int, str, str]]:
        cluster = checked_cluster(seed=seed, max_delay=5, drop_rate=0.2, dup_rate=0.2)
        cluster.run(300)
        cluster.crash("n1")
        cluster.run(100)
        cluster.restart("n1")
        cluster.run(300)
        return cluster.events

    assert events(11) == events(11)
    assert events(11) != events(12)
