"""Unit tests for log replication. Each drives RaftNodes by hand with tick()/receive()/propose().

The node under test is "n1". In the follower tests, n1 gets hand-written AppendEntries from "n2",
which plays the leader.
"""

import random
from collections.abc import Sequence

import pytest

from raft.messages import AppendEntries, AppendEntriesReply, Entry, RequestVoteReply
from raft.node import RaftConfig, RaftNode, Role
from raft.storage import MemoryStorage

IDS = ["n1", "n2", "n3", "n4", "n5"]
PEERS = IDS[1:]
FIXED = RaftConfig(election_timeout_min=30, election_timeout_max=31)  # every timeout is 30 ticks


def entries(*terms: int, start: int = 1) -> list[Entry]:
    """One entry per given term, for consecutive indexes from `start`.

    Each command names its index and term, so the same (index, term) always gives an equal
    entry, and entries from different leaders can be told apart.
    """
    return [
        Entry(t, {"op": "set", "key": f"k{start + i}", "value": f"t{t}"})
        for i, t in enumerate(terms)
    ]


def storage_with(*log_terms: int, term: int | None = None) -> MemoryStorage:
    """A log with one entry per given term. The current term defaults to the last log term."""
    s = MemoryStorage()
    s.append(entries(*log_terms))
    s.set_term_and_vote(term if term is not None else max(log_terms, default=0), None)
    return s


class RecordingStorage(MemoryStorage):
    """A MemoryStorage that records every change to the log, to check what was rewritten."""

    def __init__(self, *log_terms: int) -> None:
        super().__init__()
        super().append(entries(*log_terms))
        self.set_term_and_vote(max(log_terms, default=0), None)
        self.changes: list[str] = []

    def append(self, new: Sequence[Entry]) -> None:
        self.changes.append(f"append {len(new)}")
        super().append(new)

    def truncate_from(self, index: int) -> None:
        self.changes.append(f"truncate_from {index}")
        super().truncate_from(index)


def make_node(node_id: str = "n1", storage: MemoryStorage | None = None) -> RaftNode:
    peers = [other for other in IDS if other != node_id]
    return RaftNode(node_id, peers, storage or MemoryStorage(), random.Random(0), FIXED)


def tick(node: RaftNode, n: int) -> None:
    for _ in range(n):
        node.tick()


def log_terms(node: RaftNode) -> list[int]:
    return [entry.term for entry in node.storage.entries_from(1)]


def append_entries(
    term: int,
    prev_index: int,
    prev_term: int,
    new_entries: Sequence[Entry] = (),
    leader_commit: int = 0,
) -> AppendEntries:
    """An AppendEntries from n2, the leader of `term`, to n1."""
    return AppendEntries(
        "n2", "n1", term, prev_index, prev_term, tuple(new_entries), leader_commit
    )


def leader(storage: MemoryStorage | None = None) -> RaftNode:
    """n1 right after winning the election that follows its stored term, outputs taken."""
    node = make_node(storage=storage)
    tick(node, 30)
    node.receive(RequestVoteReply("n2", "n1", node.term, granted=True))
    node.receive(RequestVoteReply("n3", "n1", node.term, granted=True))
    assert node.role == Role.LEADER
    node.take_messages()
    node.take_events()
    return node


def confirmed(peer: str, node: RaftNode, match_index: int) -> AppendEntriesReply:
    """A success reply from `peer` to leader `node`: its log matches up to match_index."""
    return AppendEntriesReply(peer, "n1", node.term, True, match_index, last_log_index=match_index)


def refused(peer: str, node: RaftNode, last_log_index: int) -> AppendEntriesReply:
    """A failure reply from `peer` to leader `node`, whose log ends at last_log_index."""
    return AppendEntriesReply(peer, "n1", node.term, False, 0, last_log_index)


def synced_leader(storage: MemoryStorage | None = None) -> RaftNode:
    """A new leader whose peers have all confirmed its whole log, outputs taken."""
    node = leader(storage)
    for peer in PEERS:
        node.receive(confirmed(peer, node, node.storage.last_index()))
    node.take_messages()
    node.take_committed()
    return node


def exchange(nodes: dict[str, RaftNode], limit: int = 1000) -> None:
    """Deliver queued messages between `nodes`, oldest first, until there are none left.

    A perfect network: nothing is lost, delayed or reordered. Messages to other nodes are dropped.
    """
    queue = [msg for node in nodes.values() for msg in node.take_messages()]
    for _ in range(limit):
        if not queue:
            return
        msg = queue.pop(0)
        if msg.dst in nodes:
            nodes[msg.dst].receive(msg)
            queue.extend(nodes[msg.dst].take_messages())
    raise AssertionError(f"still exchanging after {limit} messages")


# --- propose ---


def test_propose_on_a_leader_saves_the_entry_and_sends_it_to_every_peer():
    node = synced_leader(storage_with(1, 1))  # leader of term 2
    last = node.storage.last_index()
    command = {"op": "set", "key": "x", "value": "1"}
    assert node.propose(command) == (last + 1, 2)
    new = Entry(2, {"op": "set", "key": "x", "value": "1"})
    # R1: the entry is in storage by the time the driver can send the messages carrying it.
    assert node.storage.entries_from(last + 1) == [new]
    assert node.take_messages() == [
        AppendEntries(
            "n1",
            peer,
            2,
            prev_log_index=last,
            prev_log_term=node.storage.term_at(last),
            entries=(new,),
            leader_commit=node.commit_index,
        )
        for peer in PEERS
    ]
    command["value"] = "changed"  # the caller reusing its dict must not change the log
    assert node.storage.entry_at(last + 1) == new


def test_propose_on_a_follower_or_candidate_returns_none():
    follower = make_node()
    candidate = make_node()
    tick(candidate, 30)
    candidate.take_messages()
    for node in (follower, candidate):
        assert node.propose({"op": "noop"}) is None
        assert node.storage.last_index() == 0
        assert node.take_messages() == []


def test_unconfirmed_entries_go_out_again_with_every_heartbeat():
    # Every AppendEntries carries all entries from next_index on, so an entry whose first
    # AppendEntries was lost is sent again with the next heartbeat.
    node = synced_leader()  # leader of term 1
    last = node.storage.last_index()
    node.propose({"op": "set", "key": "a", "value": "1"})
    node.propose({"op": "set", "key": "b", "value": "2"})
    node.take_messages()
    node.receive(confirmed("n2", node, last + 2))  # only n2 confirms both
    tick(node, 10)  # one heartbeat interval
    sent = {msg.dst: msg for msg in node.take_messages()}
    assert (sent["n2"].prev_log_index, sent["n2"].entries) == (last + 2, ())
    for peer in ["n3", "n4", "n5"]:
        assert sent[peer].prev_log_index == last
        assert sent[peer].entries == (
            Entry(1, {"op": "set", "key": "a", "value": "1"}),
            Entry(1, {"op": "set", "key": "b", "value": "2"}),
        )


# --- the leader handling replies ---


def test_success_moves_match_index_and_next_index_forward():
    node = synced_leader()
    last = node.storage.last_index()
    for key in "abc":
        node.propose({"op": "set", "key": key, "value": "v"})
    node.receive(confirmed("n2", node, last + 2))
    assert (node.match_index["n2"], node.next_index["n2"]) == (last + 2, last + 3)
    assert (node.match_index["n3"], node.next_index["n3"]) == (last, last + 1)


def test_stale_success_is_ignored():
    node = synced_leader()
    last = node.storage.last_index()
    for key in "abc":
        node.propose({"op": "set", "key": key, "value": "v"})
    node.receive(confirmed("n2", node, last + 3))
    node.receive(confirmed("n2", node, last + 1))  # a late reply to an earlier AppendEntries
    assert (node.match_index["n2"], node.next_index["n2"]) == (last + 3, last + 4)


def test_failure_backs_up_one_entry_and_retries_at_once():
    node = leader(storage_with(1, 1, 1, 1, 1))  # term 2; next_index is 6 for every peer
    node.receive(refused("n2", node, last_log_index=9))  # n2's log is longer, but differs at 5
    assert node.next_index["n2"] == 5
    [retry] = node.take_messages()
    assert (retry.dst, retry.prev_log_index, retry.prev_log_term) == ("n2", 4, 1)
    assert retry.entries == tuple(node.storage.entries_from(5))  # everything from index 5 on


def test_failure_jumps_back_to_just_past_a_shorter_follower_log():
    node = leader(storage_with(1, 1, 1, 1, 1))
    node.receive(refused("n2", node, last_log_index=2))
    assert node.next_index["n2"] == 3
    [retry] = node.take_messages()
    assert (retry.dst, retry.prev_log_index, retry.prev_log_term) == ("n2", 2, 1)


def test_next_index_never_drops_below_one():
    node = leader(storage_with(1))  # next_index is 2
    for _ in range(3):  # a refusal and two duplicates of it
        node.receive(refused("n2", node, last_log_index=0))
    assert node.next_index["n2"] == 1
    assert {(m.prev_log_index, m.prev_log_term) for m in node.take_messages()} == {(0, 0)}


def test_failure_after_a_success_is_stale_and_ignored():
    node = leader(storage_with(1, 1, 1, 1, 1))
    node.receive(refused("n2", node, last_log_index=2))
    node.receive(confirmed("n2", node, 5))  # the retry worked
    node.take_messages()
    node.receive(refused("n2", node, last_log_index=2))  # a duplicate of the refusal, arriving late
    assert (node.match_index["n2"], node.next_index["n2"]) == (5, 6)
    assert node.take_messages() == []  # and no retry


def test_reply_reaching_a_restarted_former_leader_is_ignored():
    storage = MemoryStorage()
    leader(storage)  # leads term 1, then crashes
    restarted = make_node(storage=storage)  # a follower in term 1, with no next/match_index
    restarted.receive(confirmed("n2", restarted, 0))  # a reply to its last heartbeat
    assert restarted.role == Role.FOLLOWER
    assert restarted.take_messages() == []


# --- the follower: consistency check ---


def test_follower_appends_entries_that_follow_a_matching_entry():
    node = make_node(storage=storage_with(1))
    new = entries(2, 2, start=2)
    node.receive(append_entries(2, prev_index=1, prev_term=1, new_entries=new))
    assert node.storage.entries_from(1) == entries(1) + new
    assert node.take_messages() == [
        AppendEntriesReply("n1", "n2", 2, success=True, match_index=3, last_log_index=3)
    ]


def test_first_entries_follow_index_zero_which_always_matches():
    node = make_node()
    node.receive(append_entries(1, prev_index=0, prev_term=0, new_entries=entries(1, 1)))
    assert node.storage.entries_from(1) == entries(1, 1)
    assert node.take_messages() == [
        AppendEntriesReply("n1", "n2", 1, success=True, match_index=2, last_log_index=2)
    ]


def test_follower_refuses_when_it_lacks_the_previous_entry():
    # prev_log_index is past the end of n1's log. The bounds rule: check that before term_at.
    node = make_node(storage=storage_with(1))
    node.receive(append_entries(2, prev_index=3, prev_term=2, new_entries=entries(2, start=4)))
    assert log_terms(node) == [1]
    assert node.take_messages() == [
        AppendEntriesReply("n1", "n2", 2, success=False, match_index=0, last_log_index=1)
    ]


def test_follower_refuses_when_its_previous_entry_has_another_term_and_changes_nothing():
    node = make_node(storage=storage_with(1, 1, 2))
    node.receive(append_entries(3, prev_index=3, prev_term=3, new_entries=entries(3, start=4)))
    assert log_terms(node) == [1, 1, 2]  # no truncation yet: the leader backs up and fixes it
    assert node.take_messages() == [
        AppendEntriesReply("n1", "n2", 3, success=False, match_index=0, last_log_index=3)
    ]


def test_refused_append_entries_still_resets_the_election_timer():
    # R4: it comes from the current leader, which is alive even though our logs differ.
    node = make_node()
    tick(node, 20)
    node.receive(append_entries(1, prev_index=5, prev_term=1))
    assert node.leader_id == "n2"
    tick(node, 29)
    assert node.role == Role.FOLLOWER
    node.tick()  # 30 ticks after the AppendEntries
    assert node.role == Role.CANDIDATE


# --- the follower: R5, truncate only on a real conflict ---


def test_reordered_append_does_not_truncate():
    # R5. AppendEntries can arrive in any order. Here the newest one, carrying e1-e3, arrives
    # first. Two older ones that carry less arrive after it, and must not delete e2 or e3.
    node = make_node()
    e1, e2, e3 = entries(1, 1, 1)
    node.receive(append_entries(1, prev_index=0, prev_term=0, new_entries=[e1, e2, e3]))
    node.receive(append_entries(1, prev_index=0, prev_term=0, new_entries=[e1]))
    node.receive(append_entries(1, prev_index=1, prev_term=1, new_entries=[e2]))
    assert node.storage.entries_from(1) == [e1, e2, e3]
    replies = node.take_messages()
    assert all(r.success and r.last_log_index == 3 for r in replies)
    assert [r.match_index for r in replies] == [3, 1, 2]  # each confirms only what it covered


def test_entries_the_follower_already_has_are_not_rewritten():
    # Deleting and re-adding the same entries would end with the same log, but a crash in
    # between would lose entries already confirmed to the leader. So: no writes at all.
    storage = RecordingStorage(1, 1, 1)
    node = make_node(storage=storage)
    node.receive(append_entries(1, prev_index=0, prev_term=0, new_entries=entries(1, 1, 1)))
    node.receive(append_entries(1, prev_index=1, prev_term=1, new_entries=entries(1, start=2)))
    assert storage.changes == []
    assert log_terms(node) == [1, 1, 1]


def test_conflicting_entry_and_everything_after_it_are_replaced():
    # R5. n1 has two term-2 entries from a leader that never got them committed. The term-3
    # leader's entry at index 3 is different, so n1's entries 3 and 4 go.
    storage = RecordingStorage(1, 1, 2, 2)
    node = make_node(storage=storage)
    node.receive(append_entries(3, prev_index=2, prev_term=1, new_entries=entries(3, start=3)))
    assert node.storage.entries_from(1) == entries(1, 1) + entries(3, start=3)
    assert storage.changes == ["truncate_from 3", "append 1"]
    assert node.take_messages() == [
        AppendEntriesReply("n1", "n2", 3, success=True, match_index=3, last_log_index=3)
    ]


def test_conflict_inside_the_message_keeps_the_part_that_matches():
    storage = RecordingStorage(1, 1, 2)
    node = make_node(storage=storage)
    new = entries(1, 3, 3, start=2)  # index 2 matches n1's; index 3 conflicts
    node.receive(append_entries(3, prev_index=1, prev_term=1, new_entries=new))
    assert node.storage.entries_from(1) == entries(1) + new
    assert storage.changes == ["truncate_from 3", "append 2"]


def test_success_confirms_only_what_the_message_covered():
    # n1's entries 2 and 3 are from an old term-1 leader. The term-2 leader only asks about
    # index 1. Nothing in the message conflicts with entries 2 and 3, so they stay, but n1 can't
    # confirm them: the leader's own entries 2 and 3 may be different ones.
    node = make_node(storage=storage_with(1, 1, 1))
    node.receive(append_entries(2, prev_index=1, prev_term=1))
    assert log_terms(node) == [1, 1, 1]
    assert node.take_messages() == [
        AppendEntriesReply("n1", "n2", 2, success=True, match_index=1, last_log_index=3)
    ]


# --- leader and follower together ---

FIGURE_7_LEADER = [1, 1, 1, 4, 4, 5, 5, 6, 6, 6]


@pytest.mark.parametrize(
    "follower_log",
    [
        [1, 1, 1, 4, 4, 5, 5, 6, 6],  # (a) missing one entry
        [1, 1, 1, 4],  # (b) missing many
        [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6],  # (c) one extra, uncommitted entry
        [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7],  # (d) extra entries from term 7
        [1, 1, 1, 4, 4, 4, 4],  # (e) extra term-4 entries, and missing the rest
        [1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3],  # (f) entries from terms 2 and 3 only it has
    ],
    ids=list("abcdef"),
)
def test_leader_makes_any_follower_log_a_copy_of_its_own(follower_log):
    # The follower logs from Figure 7 of the Raft paper. The term-8 leader proposes one entry.
    # Backing up and R5 must turn each follower's log into an exact copy of the leader's.
    leader_node = leader(storage_with(*FIGURE_7_LEADER, term=7))  # wins term 8
    follower = make_node("n2", storage_with(*follower_log))
    leader_node.propose({"op": "set", "key": "new", "value": "t8"})
    exchange({"n1": leader_node, "n2": follower})
    leader_log = leader_node.storage.entries_from(1)
    assert leader_log[:10] == entries(*FIGURE_7_LEADER)  # a leader never changes its own log
    assert follower.storage.entries_from(1) == leader_log
    assert leader_node.match_index["n2"] == len(leader_log)
