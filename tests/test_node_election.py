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


def leader(storage: MemoryStorage | None = None) -> RaftNode:
    """n1 right after winning its first election (votes from n2 and n3), outputs already taken."""
    node = candidate(storage)
    node.receive(grant("n2", node.term))
    node.receive(grant("n3", node.term))
    assert node.role == Role.LEADER
    node.take_messages()
    node.take_events()
    return node


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


# --- R3: granting votes ---


def test_grants_its_vote_to_the_first_candidate_and_saves_it():
    storage = MemoryStorage()
    node = make_node(storage)
    node.receive(vote_request("n2"))
    assert node.take_messages() == [RequestVoteReply("n1", "n2", 1, granted=True)]
    assert (storage.term, storage.voted_for) == (1, "n2")  # R1: saved before the reply goes out


def test_refuses_a_second_candidate_in_the_same_term():
    node = make_node()
    node.receive(vote_request("n2"))
    node.receive(vote_request("n3"))
    assert node.take_messages()[1] == RequestVoteReply("n1", "n3", 1, granted=False)
    assert node.voted_for == "n2"


def test_grants_again_to_the_same_candidate():
    # The network can duplicate a request: the copy must get the same answer, not a refusal.
    node = make_node()
    node.receive(vote_request("n2"))
    node.receive(vote_request("n2"))
    assert node.take_messages() == [RequestVoteReply("n1", "n2", 1, granted=True)] * 2


def test_restarted_node_keeps_its_vote():
    storage = MemoryStorage()
    make_node(storage).receive(vote_request("n2"))
    restarted = make_node(storage)
    restarted.receive(vote_request("n3"))
    assert restarted.take_messages() == [RequestVoteReply("n1", "n3", 1, granted=False)]


def test_votes_again_in_a_later_term():
    node = make_node()
    node.receive(vote_request("n2", term=1))
    node.receive(vote_request("n3", term=2))
    assert node.take_messages()[1] == RequestVoteReply("n1", "n3", 2, granted=True)
    assert node.voted_for == "n3"


def test_candidate_refuses_rivals_in_its_own_term():
    node = candidate()  # term 1, voted for itself
    node.receive(vote_request("n2", term=1))
    assert node.take_messages() == [RequestVoteReply("n1", "n2", 1, granted=False)]
    assert (node.role, node.voted_for) == (Role.CANDIDATE, "n1")


@pytest.mark.parametrize(
    "last_log_term, last_log_index, granted",
    [
        (2, 3, True),  # the same log end
        (2, 4, True),  # same last term, longer
        (3, 1, True),  # later last term: wins even though shorter
        (2, 2, False),  # same last term, shorter
        (1, 9, False),  # earlier last term: loses even though longer
        (0, 0, False),  # empty log
    ],
)
def test_vote_requires_a_log_at_least_as_up_to_date(last_log_term, last_log_index, granted):
    node = make_node(storage_with(1, 1, 2))  # our log ends at index 3, term 2
    node.receive(vote_request("n2", 3, last_log_index, last_log_term))
    assert node.take_messages() == [RequestVoteReply("n1", "n2", 3, granted)]
    assert node.voted_for == ("n2" if granted else None)


def test_refusing_an_outdated_candidate_leaves_the_vote_free():
    node = make_node(storage_with(1, 1, 2))
    node.receive(vote_request("n2", term=3, last_log_index=1, last_log_term=1))  # behind
    node.receive(vote_request("n3", term=3, last_log_index=3, last_log_term=2))  # up to date
    assert node.take_messages() == [
        RequestVoteReply("n1", "n2", 3, granted=False),
        RequestVoteReply("n1", "n3", 3, granted=True),
    ]


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
    # The case R4 exists for: a candidate that can never win (its log is behind) must not hold
    # our timer back, or the nodes that could win would never stand.
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


def test_stale_append_entries_does_not_reset_the_election_timer():
    node = make_node(storage_with(term=3))
    tick(node, 20)
    node.receive(heartbeat("n2", term=2))
    tick(node, 10)
    assert node.role == Role.CANDIDATE


# --- counting votes ---


def test_candidate_becomes_leader_with_a_majority():
    node = candidate()  # 5 nodes: it needs 3 votes, its own included
    node.receive(grant("n2"))
    assert node.role == Role.CANDIDATE
    node.receive(grant("n3"))
    assert (node.role, node.term, node.leader_id) == (Role.LEADER, 1, "n1")
    assert node.take_events() == ["became leader (term 1)"]


def test_a_duplicated_vote_counts_once():
    node = candidate()
    node.receive(grant("n2"))
    node.receive(grant("n2"))
    assert node.role == Role.CANDIDATE


def test_refused_votes_do_not_count():
    node = candidate()
    node.receive(RequestVoteReply("n2", "n1", 1, granted=False))
    node.receive(RequestVoteReply("n3", "n1", 1, granted=False))
    node.receive(grant("n4"))
    assert node.role == Role.CANDIDATE


def test_votes_from_an_earlier_election_do_not_count():
    node = candidate(elections=2)  # term 2
    node.receive(grant("n2", term=1))
    node.receive(grant("n3", term=1))
    assert node.role == Role.CANDIDATE
    node.receive(grant("n2", term=2))
    node.receive(grant("n3", term=2))
    assert node.role == Role.LEADER


def test_votes_do_not_carry_over_to_the_next_election():
    node = candidate()
    node.receive(grant("n2"))  # 2 of the 3 needed in term 1
    tick(node, 30)  # the election splits: term 2
    node.receive(grant("n3", term=2))
    assert node.role == Role.CANDIDATE  # n1 and n3 only: n2's vote was for term 1


@pytest.mark.parametrize("cluster_size, votes_needed", [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3)])
def test_majority_counts_the_candidate_itself(cluster_size, votes_needed):
    peers = [f"n{i}" for i in range(2, cluster_size + 1)]
    node = RaftNode("n1", peers, MemoryStorage(), random.Random(0), FIXED)
    tick(node, 30)
    votes = 1  # its own
    for peer in peers:
        if node.role == Role.LEADER:
            break
        node.receive(grant(peer))
        votes += 1
    assert node.role == Role.LEADER
    assert votes == votes_needed


def test_single_node_cluster_elects_itself_at_the_first_timeout():
    node = RaftNode("n1", [], MemoryStorage(), random.Random(0), FIXED)
    tick(node, 30)
    assert (node.role, node.term) == (Role.LEADER, 1)
    assert node.take_messages() == []
    assert node.take_events() == ["started election (term 1)", "became leader (term 1)"]


# --- leading ---


def test_new_leader_sends_every_peer_a_heartbeat_at_once():
    node = candidate(storage_with(1, 1))  # candidate in term 2; log ends at index 2, term 1
    node.receive(grant("n2", term=2))
    node.receive(grant("n3", term=2))
    assert node.take_messages() == [
        AppendEntries(
            "n1", peer, 2, prev_log_index=2, prev_log_term=1, entries=(), leader_commit=0
        )
        for peer in PEERS
    ]


def test_new_leader_sets_next_index_past_its_log_and_match_index_to_zero():
    node = leader(storage_with(1, 1))
    assert node.next_index == {peer: 3 for peer in PEERS}
    assert node.match_index == {peer: 0 for peer in PEERS}


def test_leader_sends_heartbeats_every_interval():
    node = leader()  # heartbeat_interval is 10
    for _ in range(3):
        tick(node, 9)
        assert node.take_messages() == []
        node.tick()
        sent = node.take_messages()
        assert all(isinstance(m, AppendEntries) and m.entries == () for m in sent)
        assert [m.dst for m in sent] == PEERS


def test_leader_never_times_out():
    node = leader()
    tick(node, 1000)
    assert (node.role, node.term) == (Role.LEADER, 1)
    assert all(isinstance(m, AppendEntries) for m in node.take_messages())


def test_leader_refuses_votes_in_its_own_term():
    node = leader()
    node.receive(vote_request("n2", term=1))
    assert node.take_messages() == [RequestVoteReply("n1", "n2", 1, granted=False)]
    assert node.role == Role.LEADER


def test_deposed_leader_stops_heartbeats_and_resumes_its_countdown():
    node = leader()  # won right after its timer reset, so a full 30 ticks remain
    node.receive(AppendEntriesReply("n2", "n1", 3, success=False, match_index=0, last_log_index=0))
    assert (node.role, node.term, node.leader_id) == (Role.FOLLOWER, 3, None)
    assert node.take_events() == ["became follower (term 3)"]
    tick(node, 29)
    assert node.take_messages() == []  # no more heartbeats
    node.tick()
    assert (node.role, node.term) == (Role.CANDIDATE, 4)


# --- following ---


def test_follower_accepts_a_heartbeat_and_records_the_leader():
    node = make_node()
    node.receive(heartbeat("n2", term=1))
    assert (node.role, node.term, node.leader_id) == (Role.FOLLOWER, 1, "n2")
    assert node.voted_for is None  # following a leader is not voting for it


def test_candidate_steps_down_when_the_winner_of_its_term_appears():
    node = candidate()  # term 1, voted for itself
    node.receive(heartbeat("n2", term=1))
    assert (node.role, node.term, node.leader_id) == (Role.FOLLOWER, 1, "n2")
    assert node.take_events() == ["became follower (term 1)"]
    # Same term, so its vote for itself stands (R3): no second vote, and late grants are ignored.
    assert node.voted_for == "n1"
    node.receive(vote_request("n3", term=1))
    node.receive(grant("n3"))
    node.receive(grant("n4"))
    assert node.take_messages() == [RequestVoteReply("n1", "n3", 1, granted=False)]
    assert node.role == Role.FOLLOWER


def test_higher_term_append_entries_makes_the_sender_our_leader():
    node = candidate()
    node.receive(heartbeat("n2", term=4))
    assert (node.role, node.term, node.voted_for, node.leader_id) == (Role.FOLLOWER, 4, None, "n2")


def test_new_term_forgets_the_old_leader():
    node = make_node()
    node.receive(heartbeat("n2", term=1))
    node.receive(vote_request("n3", term=2))
    assert node.leader_id is None
