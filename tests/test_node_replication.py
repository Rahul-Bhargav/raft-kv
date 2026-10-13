"""Unit tests for log replication. Each drives RaftNodes by hand with tick()/receive()/propose().

The node under test is "n1". In the follower tests, "n2" plays the leader, in hand-written messages.
"""

import random
from collections.abc import Sequence
from dataclasses import replace

from raft.messages import AppendEntries, AppendEntriesReply, Entry, RequestVoteReply
from raft.node import RaftConfig, RaftNode, Role
from raft.storage import MemoryStorage

IDS = ["n1", "n2", "n3", "n4", "n5"]
PEERS = IDS[1:]
FIXED = RaftConfig(election_timeout_min=30, election_timeout_max=31)  # every timeout is 30 ticks


def entries(*terms: int, start: int = 1) -> list[Entry]:
    """One entry per given term, for indexes from `start`. Each command names its index and term."""
    return [
        Entry(t, {"op": "set", "key": f"k{start + i}", "value": f"t{t}"})
        for i, t in enumerate(terms)
    ]


def storage_with(*log_terms: int) -> MemoryStorage:
    """A log with one entry per given term. The current term is the last entry's."""
    s = MemoryStorage()
    s.append(entries(*log_terms))
    s.set_term_and_vote(log_terms[-1], None)
    return s


def make_node(
    node_id: str = "n1", storage: MemoryStorage | None = None, config: RaftConfig = FIXED
) -> RaftNode:
    peers = [other for other in IDS if other != node_id]
    return RaftNode(node_id, peers, storage or MemoryStorage(), random.Random(0), config)


def tick(node: RaftNode, n: int) -> None:
    for _ in range(n):
        node.tick()


def append_entries(
    term: int,
    prev_index: int,
    prev_term: int,
    new_entries: Sequence[Entry] = (),
    leader_commit: int = 0,
) -> AppendEntries:
    """An AppendEntries from n2, the leader of `term`, to n1."""
    return AppendEntries("n2", "n1", term, prev_index, prev_term, tuple(new_entries), leader_commit)


def leader(storage: MemoryStorage | None = None, config: RaftConfig = FIXED) -> RaftNode:
    """n1 right after winning the election that follows its stored term, messages taken."""
    node = make_node(storage=storage, config=config)
    tick(node, 30)
    node.receive(RequestVoteReply("n2", "n1", node.term, granted=True))
    node.receive(RequestVoteReply("n3", "n1", node.term, granted=True))
    assert node.role == Role.LEADER
    node.take_messages()
    return node


def confirmed(peer: str, node: RaftNode, match_index: int) -> AppendEntriesReply:
    """A success reply from `peer` to leader `node`: its log matches up to match_index."""
    return AppendEntriesReply(peer, "n1", node.term, True, match_index, last_log_index=match_index)


def refused(peer: str, node: RaftNode, last_log_index: int) -> AppendEntriesReply:
    """A failure reply from `peer` to leader `node`, whose log ends at last_log_index."""
    return AppendEntriesReply(peer, "n1", node.term, False, 0, last_log_index)


def exchange(nodes: dict[str, RaftNode], limit: int = 1000) -> None:
    """A perfect network: deliver the nodes' queued messages, oldest first, until none are left."""
    queue = [msg for node in nodes.values() for msg in node.take_messages()]
    for _ in range(limit):
        if not queue:
            return
        msg = queue.pop(0)
        if msg.dst in nodes:
            nodes[msg.dst].receive(msg)
            queue.extend(nodes[msg.dst].take_messages())
    raise AssertionError(f"still exchanging after {limit} messages")


# --- the leader: propose, and backing up after a failure ---


def test_propose_on_a_leader_saves_the_entry_and_sends_it_to_every_peer():
    node = leader(storage_with(1, 1))  # leader of term 2; its noop is entry 3
    assert node.propose({"op": "set", "key": "x", "value": "1"}) == (4, 2)
    unconfirmed = (Entry(2, {"op": "noop"}), Entry(2, {"op": "set", "key": "x", "value": "1"}))
    # R1: the entry is in storage by the time the driver can send the messages carrying it.
    assert node.storage.entries_from(3) == list(unconfirmed)
    assert node.take_messages() == [
        AppendEntries(
            "n1", peer, 2, prev_log_index=2, prev_log_term=1, entries=unconfirmed, leader_commit=0
        )
        for peer in PEERS
    ]


def test_propose_on_a_node_that_is_not_the_leader_returns_none():
    node = make_node()  # a follower
    assert node.propose({"op": "noop"}) is None
    assert (node.storage.last_index(), node.take_messages()) == (0, [])


def test_failure_backs_up_and_retries_at_once():
    node = leader(storage_with(1, 1, 1, 1, 1))  # term 2; next_index is 6 for every peer
    node.receive(refused("n2", node, last_log_index=9))  # n2's log is longer, but differs at 5
    node.receive(refused("n3", node, last_log_index=2))  # n3's log is shorter
    assert (node.next_index["n2"], node.next_index["n3"]) == (5, 3)  # one back; just past its end
    to_n2, to_n3 = node.take_messages()
    assert (to_n2.dst, to_n2.prev_log_index, to_n2.prev_log_term) == ("n2", 4, 1)
    assert to_n2.entries == tuple(node.storage.entries_from(5))  # everything from index 5 on
    assert (to_n3.dst, to_n3.prev_log_index, to_n3.prev_log_term) == ("n3", 2, 1)


# --- the follower: the consistency check, and R5 (truncate only on a real conflict) ---


def test_follower_appends_entries_that_follow_a_matching_entry():
    node = make_node(storage=storage_with(1))
    new = entries(2, 2, start=2)
    node.receive(append_entries(2, prev_index=1, prev_term=1, new_entries=new))
    assert node.storage.entries_from(1) == entries(1) + new
    assert node.take_messages() == [
        AppendEntriesReply("n1", "n2", 2, success=True, match_index=3, last_log_index=3)
    ]


def test_follower_refuses_entries_that_do_not_follow_its_log_and_changes_nothing():
    # First n1 has no entry 5 at all. Then its entry 3 is from term 2, not the leader's term 3.
    node = make_node(storage=storage_with(1, 1, 2))
    node.receive(append_entries(3, prev_index=5, prev_term=3, new_entries=entries(3, start=6)))
    node.receive(append_entries(3, prev_index=3, prev_term=3, new_entries=entries(3, start=4)))
    assert node.storage.entries_from(1) == entries(1, 1, 2)  # no truncation: the leader backs up
    refusal = AppendEntriesReply("n1", "n2", 3, success=False, match_index=0, last_log_index=3)
    assert node.take_messages() == [refusal, refusal]


def test_reordered_append_does_not_truncate():
    # R5: the newest AppendEntries (e1-e3) arrives first. Two older ones must not delete e2 or e3.
    node = make_node()
    e1, e2, e3 = entries(1, 1, 1)
    node.receive(append_entries(1, prev_index=0, prev_term=0, new_entries=[e1, e2, e3]))
    node.receive(append_entries(1, prev_index=0, prev_term=0, new_entries=[e1]))
    node.receive(append_entries(1, prev_index=1, prev_term=1, new_entries=[e2]))
    assert node.storage.entries_from(1) == [e1, e2, e3]
    replies = node.take_messages()
    assert all(r.success and r.last_log_index == 3 for r in replies)
    assert [r.match_index for r in replies] == [3, 1, 2]  # each confirms only what it covered


def test_conflicting_entry_and_everything_after_it_are_replaced():
    # R5: n1's two term-2 entries were never committed. The term-3 leader's entry 3 differs.
    node = make_node(storage=storage_with(1, 1, 2, 2))
    node.receive(append_entries(3, prev_index=2, prev_term=1, new_entries=entries(3, start=3)))
    assert node.storage.entries_from(1) == entries(1, 1) + entries(3, start=3)
    assert node.take_messages() == [
        AppendEntriesReply("n1", "n2", 3, success=True, match_index=3, last_log_index=3)
    ]


# --- commit: the leader (R6), its noop (R7), the follower, and take_committed ---


def test_leader_commits_an_entry_once_a_majority_stores_it():
    node = leader()  # leader of term 1; its noop is entry 1
    assert node.propose({"op": "set", "key": "x", "value": "1"}) == (2, 1)
    node.receive(confirmed("n2", node, 2))
    assert node.commit_index == 0  # stored on n1 and n2: 2 of 5
    node.receive(confirmed("n3", node, 2))
    assert node.commit_index == 2  # n1, n2 and n3: 3 of 5, counting the leader itself


def test_leader_does_not_count_copies_of_an_earlier_term_entry():
    # R6, with replies written by hand: entries 1-2 (terms 1, 2) reach a majority before the noop.
    e1, e2 = entries(1, 2)
    node = leader(storage_with(1, 2))  # wins term 3; noop at index 3
    node.receive(confirmed("n2", node, 2))
    node.receive(confirmed("n3", node, 2))
    assert node.commit_index == 0  # entries 1-2 are on n1, n2 and n3, and still not committed
    node.receive(confirmed("n2", node, 3))
    node.receive(confirmed("n3", node, 3))
    assert node.commit_index == 3  # the noop commits, and entries 1-2 with it
    assert node.take_committed() == [(1, e1), (2, e2), (3, Entry(3, {"op": "noop"}))]


def test_new_leader_commits_previous_entries():
    # R7: entries 1-2 (term 1) are on n1-n3, uncommitted. n1 wins term 2, and its noop commits them.
    e1, e2 = entries(1, 1)
    nodes = {node_id: make_node(node_id, storage_with(1, 1)) for node_id in ["n1", "n2", "n3"]}
    tick(nodes["n1"], 30)  # n1 times out and stands for term 2
    exchange(nodes)  # votes, then the noop and the replies to it
    n1 = nodes["n1"]
    assert (n1.role, n1.term) == (Role.LEADER, 2)
    expected = [(1, e1), (2, e2), (3, Entry(2, {"op": "noop"}))]
    assert n1.take_committed() == expected
    tick(n1, 10)  # the next heartbeat tells the followers
    exchange(nodes)
    assert nodes["n2"].take_committed() == nodes["n3"].take_committed() == expected


def test_batch_cap_still_replicates_a_long_log_to_lagging_followers():
    # n1 has 4 term-1 entries and wins term 2, so its noop is entry 5. n2 and n3 have nothing.
    n1 = leader(storage_with(1, 1, 1, 1), replace(FIXED, max_entries_per_append=2))
    nodes = {"n1": n1, "n2": make_node("n2"), "n3": make_node("n3")}
    progress = []
    for _ in range(3):  # at most 2 entries per message, and one message per heartbeat
        tick(n1, 10)
        exchange(nodes)
        progress.append((nodes["n2"].storage.last_index(), n1.commit_index))
    # R6: entries 1-4 are on a majority after two heartbeats, but only the noop commits them.
    assert progress == [(2, 0), (4, 0), (5, 5)]
    assert nodes["n3"].storage.entries_from(1) == n1.storage.entries_from(1)


def test_follower_commits_no_further_than_the_message_checked():
    # n1's entries 2-3 are an old leader's. This message only checked index 1, so only 1 commits.
    node = make_node(storage=storage_with(1, 1, 1))
    node.receive(append_entries(2, prev_index=1, prev_term=1, leader_commit=3))
    assert node.commit_index == 1
    assert node.take_committed() == [(1, entries(1)[0])]


def test_follower_commit_index_never_goes_back():
    node = make_node()
    node.receive(append_entries(1, 0, 0, new_entries=entries(1, 1, 1), leader_commit=3))
    node.receive(append_entries(1, 0, 0, new_entries=entries(1), leader_commit=1))  # delayed
    assert node.commit_index == 3


def test_take_committed_hands_out_each_entry_once_in_order():
    node = make_node()
    log = entries(1, 1, 1, 1)
    node.receive(append_entries(1, prev_index=0, prev_term=0, new_entries=log, leader_commit=2))
    assert node.take_committed() == [(1, log[0]), (2, log[1])]
    assert node.take_committed() == []
    node.receive(append_entries(1, prev_index=4, prev_term=1, leader_commit=4))  # a heartbeat
    assert node.take_committed() == [(3, log[2]), (4, log[3])]
    assert node.last_applied == 4
