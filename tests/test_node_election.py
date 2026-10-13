"""Unit tests for leader election. Each drives one RaftNode ("n1") by hand with tick()/receive()."""

import random

import pytest

from raft.messages import AppendEntries, AppendEntriesReply, Entry, RequestVote, RequestVoteReply
from raft.node import RaftConfig, RaftNode, Role
from raft.storage import MemoryStorage

PEERS = ["n2", "n3", "n4", "n5"]
FIXED = RaftConfig(election_timeout_min=30, election_timeout_max=31)  # every timeout is 30 ticks


def storage_with(*log_terms: int) -> MemoryStorage:
    """A log with one entry per given term. The current term is the last entry's."""
    s = MemoryStorage()
    s.append([Entry(t, {"op": "noop"}) for t in log_terms])
    s.set_term_and_vote(log_terms[-1], None)
    return s


def make_node(storage: MemoryStorage | None = None) -> RaftNode:
    return RaftNode("n1", PEERS, storage or MemoryStorage(), random.Random(0), FIXED)


def tick(node: RaftNode, n: int) -> None:
    for _ in range(n):
        node.tick()


def candidate(storage: MemoryStorage | None = None, elections: int = 1) -> RaftNode:
    """n1 after `elections` election timeouts, with its outputs so far already taken."""
    node = make_node(storage)
    tick(node, 30 * elections)
    node.take_messages()
    node.take_events()
    return node


def vote_request(
    src: str, term: int = 1, last_log_index: int = 0, last_log_term: int = 0
) -> RequestVote:
    return RequestVote(src, "n1", term, last_log_index, last_log_term)


def grant(voter: str, term: int = 1) -> RequestVoteReply:
    return RequestVoteReply(voter, "n1", term, granted=True)


def heartbeat(src: str, term: int = 1) -> AppendEntries:
    return AppendEntries(
        src, "n1", term, prev_log_index=0, prev_log_term=0, entries=(), leader_commit=0
    )


# --- starting an election ---


def test_follower_starts_an_election_when_its_timer_runs_out():
    storage = storage_with(1, 1, 3)  # term 3; the log ends at index 3, term 3
    node = make_node(storage)
    tick(node, 29)
    assert (node.role, node.take_messages()) == (Role.FOLLOWER, [])
    node.tick()
    assert (node.role, node.leader_id) == (Role.CANDIDATE, None)
    # R1: the new term and the vote for itself are saved before any RequestVote can be sent.
    assert (storage.term, storage.voted_for) == (4, "n1")
    assert node.take_messages() == [
        RequestVote("n1", peer, 4, last_log_index=3, last_log_term=3) for peer in PEERS
    ]
    assert node.take_events() == ["started election (term 4)"]


def test_candidate_whose_election_times_out_starts_another():
    node = candidate()  # term 1
    tick(node, 29)  # R4: starting the election restarted the timer
    assert node.term == 1
    node.tick()
    assert (node.role, node.term, node.voted_for) == (Role.CANDIDATE, 2, "n1")


# --- R2: higher and lower terms ---


@pytest.mark.parametrize(
    "msg",
    [
        RequestVote("n2", "n1", 5, last_log_index=0, last_log_term=0),  # its log is behind ours
        AppendEntriesReply("n2", "n1", 5, success=False, match_index=0, last_log_index=0),
    ],
    ids=["request", "reply"],
)
def test_higher_term_in_a_request_or_a_reply_makes_a_candidate_step_down(msg):
    node = candidate(storage_with(1))  # candidate in term 2
    node.receive(msg)
    assert (node.role, node.term, node.voted_for) == (Role.FOLLOWER, 5, None)  # no vote yet in 5
    assert node.take_events() == ["became follower (term 5)"]


def test_candidate_steps_down_when_the_winner_of_its_term_appears():
    node = candidate()  # term 1, voted for itself
    node.receive(heartbeat("n2", term=1))
    assert (node.role, node.term, node.leader_id) == (Role.FOLLOWER, 1, "n2")
    assert node.voted_for == "n1"  # same term, so its vote stands


def test_stale_append_entries_is_rejected_and_does_not_reset_the_election_timer():
    node = make_node(storage_with(1, 3))  # term 3
    tick(node, 20)
    node.receive(heartbeat("n2", term=2))  # from a leader that is behind: our term tells it so
    assert node.take_messages() == [
        AppendEntriesReply("n1", "n2", 3, success=False, match_index=0, last_log_index=2)
    ]
    assert node.leader_id is None
    tick(node, 10)  # R4: 30 ticks since the countdown started
    assert node.role == Role.CANDIDATE


# --- R3: granting votes, and counting them ---


def test_grants_one_vote_per_term_and_saves_it():
    storage = MemoryStorage()
    node = make_node(storage)
    node.receive(vote_request("n2"))
    assert (storage.term, storage.voted_for) == (1, "n2")  # R1: saved before the reply goes out
    node.receive(vote_request("n3"))  # a second candidate in the same term
    assert node.take_messages() == [
        RequestVoteReply("n1", "n2", 1, granted=True),
        RequestVoteReply("n1", "n3", 1, granted=False),
    ]
    assert node.voted_for == "n2"


@pytest.mark.parametrize(
    "last_log_term, last_log_index, granted",
    [
        (3, 1, True),  # later last term: wins even though shorter
        (2, 2, False),  # same last term, shorter
        (1, 9, False),  # earlier last term: loses even though longer
    ],
)
def test_vote_requires_a_log_at_least_as_up_to_date(last_log_term, last_log_index, granted):
    node = make_node(storage_with(1, 1, 2))  # our log ends at index 3, term 2
    node.receive(vote_request("n2", 3, last_log_index, last_log_term))
    assert node.take_messages() == [RequestVoteReply("n1", "n2", 3, granted)]
    assert node.voted_for == ("n2" if granted else None)


def test_candidate_becomes_leader_with_a_majority():
    node = candidate()  # 5 nodes: it needs 3 votes, its own included
    node.receive(grant("n2"))
    node.receive(grant("n2"))  # a duplicate counts once
    assert node.role == Role.CANDIDATE
    node.receive(grant("n3"))
    assert (node.role, node.term, node.leader_id) == (Role.LEADER, 1, "n1")
    assert node.take_events() == ["became leader (term 1)"]


def test_votes_from_an_earlier_election_do_not_count():
    node = candidate(elections=2)  # term 2
    node.receive(grant("n2", term=1))
    node.receive(grant("n3", term=1))
    assert node.role == Role.CANDIDATE


# --- R4: what resets the election timer ---


def test_granting_a_vote_resets_the_election_timer():
    node = make_node()  # fixed 30-tick timeout
    tick(node, 20)
    node.receive(vote_request("n2"))
    tick(node, 29)
    assert node.role == Role.FOLLOWER
    node.tick()
    assert node.role == Role.CANDIDATE


def test_refusing_a_vote_does_not_reset_the_election_timer():
    # The case R4 exists for: a candidate that can never win (log behind) must not hold us back.
    node = make_node(storage_with(1))
    tick(node, 20)
    node.receive(vote_request("n2", term=5, last_log_index=0, last_log_term=0))
    assert node.take_messages() == [RequestVoteReply("n1", "n2", 5, granted=False)]
    tick(node, 10)
    assert (node.role, node.term) == (Role.CANDIDATE, 6)


def test_heartbeats_from_the_current_leader_keep_a_follower_from_standing():
    node = make_node()
    for _ in range(10):
        tick(node, 29)
        node.receive(heartbeat("n2"))
    assert (node.role, node.term, node.leader_id) == (Role.FOLLOWER, 1, "n2")
    tick(node, 29)
    assert node.role == Role.FOLLOWER
    node.tick()  # 30 ticks after the last heartbeat
    assert node.role == Role.CANDIDATE
