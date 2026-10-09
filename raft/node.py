"""One Raft node: the consensus algorithm itself. Pure logic, no I/O.

Time arrives as tick() and messages as receive(msg). Nothing is sent directly: messages and events
are queued, and the driver collects them with take_*() after every call. Because the driver sends
only after the call returns, anything saved to storage during the call is durable before any
message that depends on it leaves the node (rule R1). Rule names R1–R7 refer to DESIGN.md.
"""

import random
from dataclasses import dataclass
from enum import Enum

from raft.messages import (
    AppendEntries,
    AppendEntriesReply,
    Entry,
    Message,
    RequestVote,
    RequestVoteReply,
)
from raft.storage import Storage


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass(frozen=True)
class RaftConfig:
    """Timing, in ticks. Frozen, because the default instance is shared by every node."""

    heartbeat_interval: int = 10
    election_timeout_min: int = 30  # each timeout is drawn at random from [min, max)
    election_timeout_max: int = 60


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: list[str],
        storage: Storage,
        rng: random.Random,
        config: RaftConfig = RaftConfig(),
    ) -> None:
        self.node_id = node_id
        self.peers = list(peers)  # every other node, not including node_id
        self.storage = storage  # term, vote and log: the state that survives a restart
        self.config = config
        self._rng = rng

        # Volatile state, rebuilt from scratch on restart.
        self.role = Role.FOLLOWER
        self.leader_id: str | None = None
        self.commit_index = 0
        self.last_applied = 0

        # Election timer: counts ticks up to a random timeout, restarting only as R4 allows.
        self._election_elapsed = 0
        self._election_timeout = 0
        self._reset_election_timer()  # start the first countdown

        # Candidate state: who granted us a vote in this term. A set, so a duplicate counts once.
        self._votes: set[str] = set()

        # Leader state, set up on winning an election. next_index: the next log index to send
        # each peer. match_index: the highest index known to be replicated on each peer.
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self._heartbeat_elapsed = 0

        # Outputs, collected by the driver with take_*().
        self._outbox: list[Message] = []
        self._events: list[str] = []

    # --- persistent state, read through from storage ---

    @property
    def term(self) -> int:
        return self.storage.term

    @property
    def voted_for(self) -> str | None:
        return self.storage.voted_for

    # --- inputs ---

    def tick(self) -> None:
        """Advance time by one tick.

        A leader sends heartbeats every heartbeat_interval ticks. A follower or candidate whose
        election timer runs out starts an election.
        """
        if self.role == Role.LEADER:
            # A leader never times out, so its election timer is paused. R4 has no reset for
            # stepping down, so a deposed leader resumes the countdown it was elected with.
            self._heartbeat_elapsed += 1
            if self._heartbeat_elapsed >= self.config.heartbeat_interval:
                self._send_heartbeats()
            return
        self._election_elapsed += 1
        if self._election_elapsed >= self._election_timeout:
            self._start_election()

    def receive(self, msg: Message) -> None:
        """Handle one incoming message: the term check first (R2), then the message itself."""
        if msg.term > self.term:
            # R2: a higher term in any message, request or reply, means our term is over.
            self._adopt_term(msg.term)
        elif msg.term < self.term:
            # R2: the sender is behind. Ignore the message, but answer a request with our term
            # so the sender learns it is stale and steps down.
            self._reject_stale(msg)
            return
        # From here on msg.term == self.term.
        if isinstance(msg, RequestVote):
            self._on_request_vote(msg)
        elif isinstance(msg, RequestVoteReply):
            self._on_request_vote_reply(msg)
        elif isinstance(msg, AppendEntries):
            self._on_append_entries(msg)
        # An AppendEntriesReply needs nothing yet: the leader uses them for replication (T09).

    # --- outputs: each take_*() returns what is queued and clears it ---

    def take_messages(self) -> list[Message]:
        messages, self._outbox = self._outbox, []
        return messages

    def take_committed(self) -> list[tuple[int, Entry]]:
        """(index, entry) pairs that became committed, in log order. Nothing commits until T10."""
        return []

    def take_events(self) -> list[str]:
        events, self._events = self._events, []
        return events

    # --- elections ---

    def _start_election(self) -> None:
        """Become a candidate in the next term, vote for ourselves and ask every peer for a vote."""
        term = self.term + 1
        # R1: the new term and our own vote are saved before any RequestVote is queued, so
        # after a crash and restart this node can't vote for a different candidate in this term.
        self.storage.set_term_and_vote(term, self.node_id)
        self.role = Role.CANDIDATE
        self.leader_id = None
        self._votes = set()
        self._reset_election_timer()  # R4: starting an election (a new countdown in case it splits)
        self._event(f"started election (term {term})")
        last_index = self.storage.last_index()
        last_term = self.storage.term_at(last_index)  # in bounds: 0 <= last_index
        for peer in self.peers:
            self._send(RequestVote(self.node_id, peer, term, last_index, last_term))
        self._record_vote(self.node_id)  # wins at once if this is a one-node cluster

    def _on_request_vote(self, msg: RequestVote) -> None:
        """Vote for the candidate if R3 allows it, and reply either way."""
        # R3: at most one vote per term. Granting the same candidate again is fine: it's a
        # duplicate of a request we already granted. A candidate or leader has voted for itself.
        free_to_vote = self.voted_for in (None, msg.src)
        granted = free_to_vote and self._log_is_at_least_as_up_to_date(
            msg.last_log_term, msg.last_log_index
        )
        if granted:
            # R1: save the vote before queuing the reply that announces it, so a restart can't
            # give this term's vote to someone else.
            self.storage.set_term_and_vote(self.term, msg.src)
            self._reset_election_timer()  # R4: granting a vote
        self._send(RequestVoteReply(self.node_id, msg.src, self.term, granted))

    def _log_is_at_least_as_up_to_date(self, last_term: int, last_index: int) -> bool:
        """R3: is a log ending at (last_term, last_index) at least as up to date as ours?

        The later last term wins. With equal last terms, the longer log wins. This keeps a
        candidate missing committed entries from collecting a majority.
        """
        our_last_index = self.storage.last_index()
        our_last_term = self.storage.term_at(our_last_index)
        # Tuples compare element by element: last term first, then last index.
        return (last_term, last_index) >= (our_last_term, our_last_index)

    def _on_request_vote_reply(self, msg: RequestVoteReply) -> None:
        # Same term, so this answers our current election. Once we have won or stepped down
        # in this term, late replies don't matter.
        if self.role == Role.CANDIDATE and msg.granted:
            self._record_vote(msg.src)

    def _record_vote(self, voter: str) -> None:
        self._votes.add(voter)
        if len(self._votes) >= self._majority():
            self._become_leader()

    def _majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    # --- leadership ---

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.node_id
        # Start optimistic: assume every peer's log matches ours, and back off on failure (T09).
        # Nothing is known to be replicated on any peer yet.
        self.next_index = {peer: self.storage.last_index() + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        self._event(f"became leader (term {self.term})")
        self._send_heartbeats()  # announce the win now, before any follower times out

    def _send_heartbeats(self) -> None:
        self._heartbeat_elapsed = 0
        for peer in self.peers:
            self._send_append_entries(peer)

    def _send_append_entries(self, peer: str) -> None:
        """Send `peer` an AppendEntries. For now always a heartbeat; T09 adds the entries."""
        prev_index = self.next_index[peer] - 1
        self._send(
            AppendEntries(
                self.node_id,
                peer,
                self.term,
                prev_log_index=prev_index,
                prev_log_term=self.storage.term_at(prev_index),  # 1 <= next_index <= last + 1
                entries=(),
                leader_commit=self.commit_index,
            )
        )

    def _on_append_entries(self, msg: AppendEntries) -> None:
        """Same term: the sender won this term's election, so follow it."""
        # R3 allows one winner per term, so another leader in our term would be a safety bug.
        assert self.role != Role.LEADER, f"{self.node_id}: two leaders in term {self.term}"
        self._become_follower()  # a candidate that hears from the winner stops campaigning
        self.leader_id = msg.src
        self._reset_election_timer()  # R4: a valid AppendEntries from the current leader
        # T09: consistency check, append msg.entries, and reply.

    # --- terms and roles ---

    def _adopt_term(self, term: int) -> None:
        """R2: move to a higher term as a follower, with no vote cast in it yet."""
        self.storage.set_term_and_vote(term, None)
        self.leader_id = None  # whoever leads the new term hasn't been heard from yet
        self._become_follower()
        # No timer reset: R4 doesn't list "saw a higher term", and resetting here would let a
        # candidate that can never win (outdated log) hold everyone's timers back forever.

    def _become_follower(self) -> None:
        if self.role != Role.FOLLOWER:
            self.role = Role.FOLLOWER
            self._event(f"became follower (term {self.term})")

    def _reject_stale(self, msg: Message) -> None:
        """Answer a lower-term request with our term. A lower-term reply is dropped."""
        if isinstance(msg, RequestVote):
            self._send(RequestVoteReply(self.node_id, msg.src, self.term, granted=False))
        elif isinstance(msg, AppendEntries):
            self._send(
                AppendEntriesReply(
                    self.node_id,
                    msg.src,
                    self.term,
                    success=False,
                    match_index=0,
                    last_log_index=self.storage.last_index(),
                )
            )
        # A reply with a lower term answers something we sent in an older term: nothing to do.

    def _reset_election_timer(self) -> None:
        """Restart the countdown with a fresh random timeout, so nodes rarely time out together.

        R4: call this only on a valid AppendEntries from the current leader, on granting a vote,
        or on starting an election.
        """
        self._election_elapsed = 0
        self._election_timeout = self._rng.randrange(
            self.config.election_timeout_min, self.config.election_timeout_max
        )

    # --- output helpers ---

    def _send(self, msg: Message) -> None:
        self._outbox.append(msg)

    def _event(self, text: str) -> None:
        self._events.append(text)
