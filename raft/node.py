"""One Raft node: the consensus algorithm itself. Pure logic, no I/O.

Time arrives as tick() and messages as receive(msg). Messages are never sent from inside the node:
they're queued, and the driver collects them with take_*() and sends them after the call returns.
By then, everything the call saved (term, vote, log entries) is already stored. So a crash can never
leave another node holding a promise this node has forgotten, such as "I voted for you" or "I have
entry 7". That's rule R1. Rule names R1-R7 refer to DESIGN.md.
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
    """Timing (in ticks) and batch size. Frozen: the default instance is shared by every node."""

    heartbeat_interval: int = 10
    election_timeout_min: int = 30  # each timeout is drawn at random from [min, max)
    election_timeout_max: int = 60
    # The most entries one AppendEntries may carry. None means no limit: everything from
    # next_index on. _send_append_entries explains why a limit matters for R6.
    max_entries_per_append: int | None = None

    def __post_init__(self) -> None:
        # A limit of 0 would send only empty heartbeats, and nothing would ever replicate.
        if self.max_entries_per_append is not None and self.max_entries_per_append < 1:
            raise ValueError("max_entries_per_append must be None or at least 1")


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
                self._broadcast_append_entries()
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
        elif isinstance(msg, AppendEntriesReply):
            self._on_append_entries_reply(msg)

    def propose(self, command: dict) -> tuple[int, int] | None:
        """Leader only: add `command` to the log and start replicating it to every peer.

        Returns the new entry's (index, term), or None if this node isn't the leader. The caller
        knows its write took effect only once take_committed() hands out this index with this
        term. Until then it may still be lost: a later leader can overwrite it with another entry.
        """
        if self.role != Role.LEADER:
            return None
        # Copy the command, so a caller changing its dict later can't change the log.
        # R1: the entry is saved before any AppendEntries carrying it is queued.
        self.storage.append([Entry(self.term, dict(command))])
        # In a one-node cluster our own copy is already a majority, and no reply will ever
        # come to trigger this. With peers, nothing new can be committed yet.
        self._advance_commit_index()
        self._broadcast_append_entries()  # send it now rather than at the next heartbeat
        return self.storage.last_index(), self.term

    # --- outputs: each take_*() returns what is queued and clears it ---

    def take_messages(self) -> list[Message]:
        messages, self._outbox = self._outbox, []
        return messages

    def take_committed(self) -> list[tuple[int, Entry]]:
        """(index, entry) for each entry committed since the last call, in log order.

        last_applied marks how far entries have been handed out, so each goes out exactly once.
        After a restart it starts again from 0, and the driver rebuilds its state machine.
        """
        committed = [
            (index, self.storage.entry_at(index))
            for index in range(self.last_applied + 1, self.commit_index + 1)
        ]
        self.last_applied = self.commit_index
        return committed

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
        # Start optimistic: assume every peer's log matches ours, and back off on failure.
        # Nothing is known to be replicated on any peer yet. Set before the noop below is
        # appended, so the first AppendEntries to each peer carries it.
        self.next_index = {peer: self.storage.last_index() + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        self._event(f"became leader (term {self.term})")
        # R7: append a noop in our own term at once. R6 won't let us commit entries left by
        # earlier leaders by counting copies, only by committing one of ours after them. Without
        # the noop they'd wait for the next client write. propose() also sends it to every peer
        # right away, which announces the win before any follower times out.
        self.propose({"op": "noop"})

    def _broadcast_append_entries(self) -> None:
        """Send every peer an AppendEntries. It's also the heartbeat, so restart that countdown."""
        self._heartbeat_elapsed = 0
        for peer in self.peers:
            self._send_append_entries(peer)

    def _send_append_entries(self, peer: str) -> None:
        """Send `peer` the entries from next_index[peer] on: a plain heartbeat if it has them all.

        next_index moves only when the peer confirms entries, so every heartbeat repeats whatever
        is still unconfirmed. A lost AppendEntries is simply retried at the next one.

        With max_entries_per_append set, one message carries at most that many. The rest follow
        as the peer confirms them: a success moves next_index, and the next heartbeat or proposal
        sends from there. Why allow a limit at all? Without one, every message runs to the end of
        our log, so any reply that confirms an earlier term's entry also confirms our R7 noop, and
        R6's term check never changes what gets committed. Real implementations do limit batch
        size. Then a follower can confirm an earlier term's entry before it has our noop, which is
        the case R6 exists for (Figure 8 in the Raft paper).
        """
        prev_index = self.next_index[peer] - 1
        entries = self.storage.entries_from(prev_index + 1)
        if self.config.max_entries_per_append is not None:
            entries = entries[: self.config.max_entries_per_append]
        self._send(
            AppendEntries(
                self.node_id,
                peer,
                self.term,
                prev_log_index=prev_index,
                prev_log_term=self.storage.term_at(prev_index),  # 1 <= next_index <= last + 1
                entries=tuple(entries),
                leader_commit=self.commit_index,
            )
        )

    def _on_append_entries_reply(self, msg: AppendEntriesReply) -> None:
        """Leader: record how much of our log a follower has, or back up and resend."""
        if self.role != Role.LEADER:
            # Only this term's leader sends AppendEntries, so we led this term. The reply can
            # still arrive after we crashed and restarted, as a follower in the same term.
            return
        peer = msg.src
        if msg.match_index < self.match_index[peer]:
            # Stale: a delayed or duplicated answer to an older AppendEntries; we know more.
            # A failure has match_index 0, so this also drops every failure once the peer has
            # confirmed an entry. From then on we send from match_index + 1, and the peer keeps
            # the entries it confirmed for the rest of our term (in our term only we send it
            # entries, and they never conflict with each other), so its check can't fail again.
            return
        if msg.success:
            self.match_index[peer] = msg.match_index
            self.next_index[peer] = msg.match_index + 1
            self._advance_commit_index()
        else:
            # The peer lacks our entry at next_index - 1. Back up one entry, or straight to
            # just past the peer's last entry if its log is shorter than that. Never below 1.
            self.next_index[peer] = max(
                1, min(self.next_index[peer] - 1, msg.last_log_index + 1)
            )
            self._send_append_entries(peer)  # retry now rather than at the next heartbeat

    def _advance_commit_index(self) -> None:
        """Leader: commit up to the highest entry of our term that a majority stores (R6)."""
        for n in range(self.storage.last_index(), self.commit_index, -1):  # highest first
            if self.storage.term_at(n) != self.term:
                # R6: never commit an earlier term's entry by counting copies. Even on a majority
                # it can still be overwritten (Figure 8 in the Raft paper). Terms only go down
                # from here towards the start of the log, so no lower entry qualifies either.
                return
            copies = 1 + sum(1 for match in self.match_index.values() if match >= n)  # 1: ours
            if copies >= self._majority():
                # This commits every entry before n too, earlier terms' included: any later
                # leader will have entry n, and so everything before it.
                self.commit_index = n
                return

    def _on_append_entries(self, msg: AppendEntries) -> None:
        """Same term: the sender won this term's election. Follow it and take its entries."""
        # R3 allows one winner per term, so another leader in our term would be a safety bug.
        assert self.role != Role.LEADER, f"{self.node_id}: two leaders in term {self.term}"
        self._become_follower()  # a candidate that hears from the winner stops campaigning
        self.leader_id = msg.src
        # R4: a valid AppendEntries from the current leader, even if the log check below fails.
        # The leader is alive either way; it only has to back up and resend.
        self._reset_election_timer()

        if not self._has_entry(msg.prev_log_index, msg.prev_log_term):
            # Consistency check failed: we don't have the entry the new ones must follow.
            # Taking them anyway could leave a gap, or put them after an entry the leader
            # doesn't have. Change nothing; our last index tells the leader where to retry.
            self._reply_append_entries(msg.src, success=False, match_index=0)
            return
        self._append_new_entries(msg.prev_log_index, msg.entries)
        # The last index this message covered. Our log now matches the leader's up to here. It
        # may go further, but entries past this weren't checked against the leader's log, so
        # they may be an old leader's entries that the current leader doesn't have.
        last_new_entry = msg.prev_log_index + len(msg.entries)
        # Commit what the leader has committed, but no further than last_new_entry: leader_commit
        # is about the leader's entries, not about our unchecked ones. max(), because a delayed
        # message can carry an older leader_commit.
        self.commit_index = max(self.commit_index, min(msg.leader_commit, last_new_entry))
        # R1: the entries are saved before the reply that confirms them is queued. We confirm
        # only what this message covered.
        self._reply_append_entries(msg.src, success=True, match_index=last_new_entry)

    def _has_entry(self, index: int, term: int) -> bool:
        """Does our log have an entry at `index` from `term`? Index 0 (no entry) always matches."""
        # Check the length first: term_at raises past the end of the log.
        return index <= self.storage.last_index() and self.storage.term_at(index) == term

    def _append_new_entries(self, prev_index: int, entries: tuple[Entry, ...]) -> None:
        """Put the leader's entries after prev_index, keeping the ones we already have (R5)."""
        for offset, entry in enumerate(entries):
            index = prev_index + 1 + offset
            if index > self.storage.last_index():
                self.storage.append(entries[offset:])  # the rest are all new to us
                return
            if self.storage.term_at(index) != entry.term:
                # R5: a real conflict. Our entry here came from a leader of another term, so it
                # and every entry after it are missing from the current leader's log. A leader
                # has every committed entry (R3 and R6 together make sure of that), so these were
                # never committed: delete them and take the leader's instead.
                assert index > self.commit_index, f"{self.node_id}: deleting committed {index}"
                self.storage.truncate_from(index)
                self.storage.append(entries[offset:])
                return
            # Same index and term: it's the entry we already have, because a leader creates
            # only one entry per index in its term. Keep it.
        # Everything was already in our log. A delayed or duplicated AppendEntries ends up here,
        # and must not delete what comes after it: a newer message may have added those entries.

    def _reply_append_entries(self, leader: str, success: bool, match_index: int) -> None:
        """Answer the leader. Every reply has our last index: on failure, it's where to retry."""
        self._send(
            AppendEntriesReply(
                self.node_id,
                leader,
                self.term,
                success=success,
                match_index=match_index,
                last_log_index=self.storage.last_index(),
            )
        )

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
