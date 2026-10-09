"""Unit tests for leader election. Each drives one RaftNode ("n1") by hand with tick()/receive()."""

import random

import pytest

from raft.messages import (
    AppendEntries,
    AppendEntriesReply,
    Entry,
    Message,
    RequestVote,
    RequestVoteReply,
)
from raft.node import RaftConfig, RaftNode, Role
from raft.storage import MemoryStorage

PEERS = ["n2", "n3", "n4", "n5"]
FIXED = RaftConfig(election_timeout_min=30, election_timeout_max=31)  # every timeout is 30 ticks


def storage_with(*log_terms: int, term: int | None = None) -> MemoryStorage:
    """A log with one entry per given term. The current term defaults to the last log term."""
    s = MemoryStorage()
    s.append([Entry(t, {"op": "noop"}) for t in log_terms])
    s.set_term_and_vote(term if term is not None else max(log_terms, default=0), None)
    return s


def make_node(
    storage: MemoryStorage | None = None, config: RaftConfig = FIXED, seed: int = 0
) -> RaftNode:
    return RaftNode("n1", PEERS, storage or MemoryStorage(), random.Random(seed), config)


def tick(node: RaftNode, n: int) -> None:
    for _ in range(n):
        node.tick()


def ticks_until_election(node: RaftNode, limit: int = 1000) -> int:
    """Tick until the node starts an election (its term goes up); return how many ticks it took."""
    start_term = node.term
    for ticks in range(1, limit + 1):
        node.tick()
        if node.term > start_term:
            return ticks
    raise AssertionError(f"no election within {limit} ticks")


def candidate(storage: MemoryStorage | None = None, elections: int = 1) -> RaftNode:
    """n1 after `elections` election timeouts, with its outputs so far already taken."""
    node = make_node(storage)
    tick(node, 30 * elections)
    node.take_messages()
    node.take_events()
    return node


def message_from_n2(kind: type, term: int) -> Message:
    """A message of type `kind` from n2 to n1. Any log it describes is empty."""
    if kind is RequestVote:
        return RequestVote("n2", "n1", term, last_log_index=0, last_log_term=0)
    if kind is RequestVoteReply:
        return RequestVoteReply("n2", "n1", term, granted=False)
    if kind is AppendEntries:
        return AppendEntries(
            "n2", "n1", term, prev_log_index=0, prev_log_term=0, entries=(), leader_commit=0
        )
    return AppendEntriesReply("n2", "n1", term, success=False, match_index=0, last_log_index=0)


ALL_KINDS = [RequestVote, RequestVoteReply, AppendEntries, AppendEntriesReply]


# --- a new node ---


def test_new_node_is_a_quiet_follower():
    node = make_node()
    assert (node.role, node.term, node.voted_for, node.leader_id) == (Role.FOLLOWER, 0, None, None)
    assert (node.commit_index, node.last_applied) == (0, 0)
    assert node.take_messages() == node.take_events() == node.take_committed() == []


def test_take_methods_hand_out_each_item_once():
    node = make_node()
    tick(node, 30)
    assert len(node.take_messages()) == len(PEERS)
    assert node.take_messages() == []
    assert len(node.take_events()) == 1
    assert node.take_events() == []


# --- election timer ---


def test_follower_starts_an_election_when_its_timer_runs_out():
    node = make_node()
    tick(node, 29)
    assert node.role == Role.FOLLOWER
    assert node.take_messages() == []
    node.tick()
    assert node.role == Role.CANDIDATE


def test_election_timeouts_are_random_within_bounds():
    config = RaftConfig(election_timeout_min=30, election_timeout_max=60)
    timeouts = [ticks_until_election(make_node(config=config, seed=s)) for s in range(200)]
    assert min(timeouts) >= 30
    assert max(timeouts) <= 59  # the upper bound is exclusive
    assert len(set(timeouts)) > 20  # spread out, not one value


def test_each_countdown_draws_a_fresh_timeout():
    node = make_node(config=RaftConfig(election_timeout_min=30, election_timeout_max=60), seed=1)
    gaps = [ticks_until_election(node) for _ in range(20)]  # a candidate keeps timing out
    assert all(30 <= gap < 60 for gap in gaps)
    assert len(set(gaps)) > 5


def test_same_seed_gives_the_same_timeouts():
    config = RaftConfig(election_timeout_min=30, election_timeout_max=60)
    a, b = make_node(config=config, seed=7), make_node(config=config, seed=7)
    gaps_a = [ticks_until_election(a) for _ in range(10)]
    assert gaps_a == [ticks_until_election(b) for _ in range(10)]


# --- starting an election ---


def test_starting_an_election_votes_for_itself_and_asks_every_peer():
    node = make_node()
    tick(node, 30)
    assert (node.role, node.term, node.voted_for, node.leader_id) == (Role.CANDIDATE, 1, "n1", None)
    assert node.take_messages() == [
        RequestVote("n1", peer, 1, last_log_index=0, last_log_term=0) for peer in PEERS
    ]
    assert node.take_events() == ["started election (term 1)"]


def test_request_vote_describes_the_candidates_log():
    node = make_node(storage_with(1, 1, 3))
    tick(node, 30)
    sent = {(m.term, m.last_log_index, m.last_log_term) for m in node.take_messages()}
    assert sent == {(4, 3, 3)}


def test_new_term_and_self_vote_are_saved_before_request_votes_go_out():
    # R1. The driver sends whatever take_messages() returns, so by then the new term and the
    # self-vote must already be in storage: a node restarted from it remembers both.
    storage = MemoryStorage()
    node = make_node(storage)
    tick(node, 30)
    assert node.take_messages()  # RequestVotes are ready to send...
    assert (storage.term, storage.voted_for) == (1, "n1")  # ...and the vote is already saved
    restarted = make_node(storage)
    assert (restarted.role, restarted.term, restarted.voted_for) == (Role.FOLLOWER, 1, "n1")


def test_candidate_whose_election_times_out_starts_another():
    node = candidate()
    tick(node, 29)
    assert node.take_messages() == []
    node.tick()
    assert (node.role, node.term, node.voted_for) == (Role.CANDIDATE, 2, "n1")
    assert node.take_messages() == [
        RequestVote("n1", peer, 2, last_log_index=0, last_log_term=0) for peer in PEERS
    ]
    assert node.take_events() == ["started election (term 2)"]


# --- R2: higher terms ---


@pytest.mark.parametrize("kind", ALL_KINDS, ids=lambda kind: kind.__name__)
def test_higher_term_in_any_message_makes_a_candidate_step_down(kind):
    # n1 has an entry from term 1 and every message's log is empty, so a RequestVote here comes
    # from a candidate whose log is behind: this checks the step-down only, not a vote (R3).
    node = candidate(storage_with(1))  # candidate in term 2
    node.receive(message_from_n2(kind, term=5))
    assert (node.role, node.term, node.voted_for) == (Role.FOLLOWER, 5, None)
    assert node.take_events() == ["became follower (term 5)"]


def test_follower_adopts_a_higher_term_and_forgets_its_vote():
    storage = MemoryStorage()
    storage.set_term_and_vote(3, "n4")
    node = make_node(storage)
    node.receive(RequestVoteReply("n2", "n1", 7, granted=False))
    assert (node.role, node.term, node.voted_for) == (Role.FOLLOWER, 7, None)
    assert (storage.term, storage.voted_for) == (7, None)  # saved, so a restart can't undo it
    assert node.take_messages() == []
    assert node.take_events() == []  # still a follower: no role change to report


def test_adopting_a_higher_term_does_not_reset_the_election_timer():
    # R4 doesn't list "saw a higher term", so the countdown carries on as if nothing arrived.
    node = make_node()
    tick(node, 20)
    node.receive(RequestVoteReply("n2", "n1", 4, granted=False))
    tick(node, 9)
    assert node.role == Role.FOLLOWER
    node.tick()  # 30 ticks after the countdown started
    assert (node.role, node.term) == (Role.CANDIDATE, 5)


# --- R2: lower terms ---


def test_lower_term_request_vote_is_refused_with_our_term():
    node = make_node(storage_with(1, 2, term=3))
    # The candidate's log is longer than ours, but its term is stale, and that is checked first.
    node.receive(RequestVote("n2", "n1", 2, last_log_index=9, last_log_term=2))
    assert node.take_messages() == [RequestVoteReply("n1", "n2", 3, granted=False)]
    assert (node.role, node.term, node.voted_for) == (Role.FOLLOWER, 3, None)


def test_lower_term_append_entries_is_rejected_with_our_term():
    node = make_node(storage_with(1, 2, term=3))
    node.receive(
        AppendEntries("n2", "n1", 2, prev_log_index=2, prev_log_term=2, entries=(), leader_commit=0)
    )
    assert node.take_messages() == [
        AppendEntriesReply("n1", "n2", 3, success=False, match_index=0, last_log_index=2)
    ]
    assert (node.role, node.term, node.leader_id) == (Role.FOLLOWER, 3, None)


@pytest.mark.parametrize(
    "reply",
    [
        RequestVoteReply("n2", "n1", 1, granted=True),
        AppendEntriesReply("n2", "n1", 1, success=True, match_index=0, last_log_index=0),
    ],
    ids=["RequestVoteReply", "AppendEntriesReply"],
)
def test_lower_term_replies_are_dropped(reply):
    node = candidate(elections=2)  # candidate in term 2; the reply answers something from term 1
    node.receive(reply)
    assert (node.role, node.term, node.voted_for) == (Role.CANDIDATE, 2, "n1")
    assert node.take_messages() == []
    assert node.take_events() == []
