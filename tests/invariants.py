"""Raft's safety invariants, checked after every simulator step.

Every SimCluster test builds its cluster with checked_cluster(), which puts an Invariants checker
first in cluster.checkers. So the five invariants below run after every step of every cluster test,
scripted or fuzzed. A broken invariant raises InvariantViolation (an AssertionError) whose message
names the invariant, the tick, the nodes and the index/term involved.

The checker only reads the cluster: it never sends anything, changes state or draws random numbers,
so a run is the same with or without it. It keeps a history, not just a picture of the current
state: what a node applied before it crashed stays on record. The T13 work found a run where only
that history shows the bug (see State Machine Safety below).

1. Election Safety: at most one leader per term, over the whole run.
2. Log Matching: if two logs have an entry with the same index and term, the logs are identical up
   to that index.
3. Leader Completeness: an entry committed in some term is in the log of every leader of every later
   term.
4. State Machine Safety: no two nodes apply different entries at the same index, and neither does
   one node before and after a restart.
5. No lost acknowledged writes: an acknowledged write shows in the KV store of every node that has
   applied that far, unless the node has since applied a later entry that sets the same key.

An *acknowledged write* is a client `set` entry at index i, from term t, that the node which
proposed it has applied at index i. Only term t's leader creates term-t entries, so that node is
term t's leader. This is exactly when SimCluster.write returns True: it proposes on the leader,
waits until that node has applied index i, then checks that the entry there has term t. If write()
gave up waiting and the leader applied the entry later, the checker still counts the write. That
only makes the check stricter.
"""

import re

from raft.messages import Entry
from raft.node import RaftNode, Role
from sim.cluster import SimCluster

_BECAME_LEADER = re.compile(r"became leader \(term (\d+)\)")  # the event RaftNode emits


class InvariantViolation(AssertionError):
    """A Raft safety invariant failed. The message says which one, when, where and on what."""


def checked_cluster(*args, **kwargs) -> SimCluster:
    """A SimCluster that checks every invariant after every step. Tests use this, not SimCluster.

    The checker goes first in cluster.checkers, so it sees each step before a test's own checker
    crashes or partitions anything.
    """
    cluster = SimCluster(*args, **kwargs)
    cluster.checkers.append(Invariants())
    return cluster


def _fail(invariant: str, tick: int, details: str) -> None:
    raise InvariantViolation(f"{invariant} violated at tick {tick}: {details}")


class Invariants:
    """A SimCluster checker for the five invariants. Call it after every step.

    Each check is incremental: it looks only at what changed since the last step (new log entries,
    newly committed or applied indexes, new leaders), so the cost stays small on long runs.
    """

    def __init__(self) -> None:
        # 1. term -> (its leader, the tick it was first seen leading).
        self.leaders: dict[int, tuple[str, int]] = {}
        self._events_read = 0  # how far into cluster.events we have looked

        # 2. Every entry ever seen in any log, by (index, term): the entry, the term of the entry
        # before it, and which node had it when.
        self._entries: dict[tuple[int, int], tuple[Entry, int, str, int]] = {}
        self._logs: dict[str, list[Entry]] = {}  # each node's log at the last check
        self._rewritten: set[str] = set()  # nodes whose existing entries changed in this step

        # 3. Committed entries in index order, index i at position i - 1 (a commit always covers
        # everything before it): the entry, a term by which it was committed, and who showed it
        # committed when. That term is the term of the first node seen with it committed: as a rule
        # the leader that committed it. It is never earlier than the real one, so the check can't
        # fire falsely.
        self.committed: list[tuple[Entry, int, str, int]] = []
        self._leaderships: set[tuple[str, int]] = set()  # (node, term) leaderships checked so far

        # 4. index -> the first entry any node applied there, which node, and when.
        self.applied: dict[int, tuple[Entry, str, int]] = {}

        # 5. index -> (acknowledged entry, tick it was acknowledged).
        self.acknowledged: dict[int, tuple[Entry, int]] = {}

        # Per node: the RaftNode object last seen (a restart makes a new one, starting again from
        # commit index 0), how far that incarnation has committed and applied, and the KV store
        # its applied entries should give.
        self._incarnation: dict[str, RaftNode] = {}
        self._committed_upto: dict[str, int] = {}
        self._applied_upto: dict[str, int] = {}
        self._expected_kv: dict[str, dict[str, str]] = {}

    def __call__(self, cluster: SimCluster) -> None:
        self._notice_restarts(cluster)
        self._check_election_safety(cluster)
        self._check_log_matching(cluster)
        self._check_leader_completeness(cluster)
        newly_applied = self._check_state_machine_safety(cluster)
        self._check_no_lost_acknowledged_writes(cluster, newly_applied)

    def _notice_restarts(self, cluster: SimCluster) -> None:
        for node in _live(cluster):
            if self._incarnation.get(node.node_id) is not node:
                self._incarnation[node.node_id] = node
                self._committed_upto[node.node_id] = 0
                self._applied_upto[node.node_id] = 0
                self._expected_kv[node.node_id] = {}

    # --- 1. Election Safety ---

    def _check_election_safety(self, cluster: SimCluster) -> None:
        """Record who leads each term. The events catch a leader that lasted less than one step;
        the roles don't depend on the events' wording."""
        for tick, node_id, event in cluster.events[self._events_read :]:
            if match := _BECAME_LEADER.fullmatch(event):
                self._record_leader(int(match[1]), node_id, tick)
        self._events_read = len(cluster.events)
        for node in _live(cluster):
            if node.role == Role.LEADER:
                self._record_leader(node.term, node.node_id, cluster.now)

    def _record_leader(self, term: int, node_id: str, tick: int) -> None:
        leader, since = self.leaders.setdefault(term, (node_id, tick))
        if leader != node_id:
            _fail(
                "Election Safety",
                tick,
                f"{node_id} and {leader} both led term {term} ({leader} since tick {since})",
            )

    # --- 2. Log Matching ---

    def _check_log_matching(self, cluster: SimCluster) -> None:
        """Check every new or changed log entry against every entry ever seen at its index and term.

        Two logs that agree at (index, term) must be identical up to there. We check an equivalent
        form one entry at a time: the entry at a given (index, term) always has the same command
        and follows an entry of the same term. Then two logs that agree at (i, t) also agree at
        i - 1, so at i - 2, and so on down to index 1. We check it across the whole run, not just
        between logs that exist at the same moment: only term t's one leader (Election Safety)
        creates term-t entries, at most one per index, so an (index, term) pair names one entry
        forever.

        Crashed nodes' logs count too: their storage is the disk they will restart with.
        """
        self._rewritten = set()
        for node_id, storage in cluster.storages.items():
            log = storage.entries_from(1)
            old = self._logs.get(node_id, [])
            start = _first_difference(old, log)
            if start < len(old):
                self._rewritten.add(node_id)  # entries it already had were removed or replaced
            for position in range(start, len(log)):
                index, entry = position + 1, log[position]
                prev_term = log[position - 1].term if position > 0 else 0
                seen = self._entries.setdefault(
                    (index, entry.term), (entry, prev_term, node_id, cluster.now)
                )
                seen_entry, seen_prev_term, seen_node, seen_tick = seen
                if (seen_entry, seen_prev_term) != (entry, prev_term):
                    _fail(
                        "Log Matching",
                        cluster.now,
                        f"{node_id} has index {index} from term {entry.term} = {entry.command}, "
                        f"after a term-{prev_term} entry, but {seen_node} had index {index} from "
                        f"term {entry.term} = {seen_entry.command}, after a term-{seen_prev_term} "
                        f"entry, at tick {seen_tick}",
                    )
            self._logs[node_id] = log

    # --- 3. Leader Completeness ---

    def _check_leader_completeness(self, cluster: SimCluster) -> None:
        """Every leader must have every entry committed in an earlier term.

        A new leader is checked against everything committed so far. After that, a leader only
        appends to its log, so it is checked again only against entries newly committed (it may
        have been elected before a slow commit of an earlier term was seen), or in full if its log
        was rewritten.
        """
        newly_committed = self._record_commits(cluster)
        for node in _live(cluster):
            if node.role != Role.LEADER:
                continue
            leadership = (node.node_id, node.term)
            if leadership not in self._leaderships or node.node_id in self._rewritten:
                self._leaderships.add(leadership)
                indexes = range(1, len(self.committed) + 1)
            else:
                indexes = newly_committed
            for index in indexes:
                entry, commit_term, who, tick = self.committed[index - 1]
                if commit_term >= node.term:
                    continue  # it may be a stale leader, elected before this entry was committed
                last = node.storage.last_index()
                if index > last or node.storage.entry_at(index) != entry:
                    its_log = (
                        f"has a term-{node.storage.term_at(index)} entry there"
                        if index <= last
                        else f"ends at index {last}"
                    )
                    _fail(
                        "Leader Completeness",
                        cluster.now,
                        f"{node.node_id} leads term {node.term} but lacks index {index} from term "
                        f"{entry.term} = {entry.command}, committed by term {commit_term} ({who} "
                        f"showed it committed at tick {tick}): its log {its_log}",
                    )

    def _record_commits(self, cluster: SimCluster) -> list[int]:
        """Add newly committed indexes to self.committed and return them."""
        new = []
        for node in _live(cluster):
            for index in range(self._committed_upto[node.node_id] + 1, node.commit_index + 1):
                if index > len(self.committed):
                    entry = node.storage.entry_at(index)
                    self.committed.append((entry, node.term, node.node_id, cluster.now))
                    new.append(index)
            self._committed_upto[node.node_id] = node.commit_index
        return new

    # --- 4. State Machine Safety ---

    def _check_state_machine_safety(self, cluster: SimCluster) -> list[tuple[str, int, Entry]]:
        """Compare every newly applied entry with the first entry ever applied at that index.

        The record outlives crashes and restarts. A check of the current state alone would miss
        what T13 found with R6 removed: n1 applied index 2 from term 2, then crashed; the others
        later applied a different index 2 from term 3, and so did n1 after its restart. At every
        moment the live nodes agreed with each other.

        Reads each entry from the node's log after the step. That is the entry it applied: a node
        never changes an entry at or below its commit index (RaftNode asserts this).
        Returns (node, index, entry) for every newly applied index, in order.
        """
        newly_applied = []
        for node in _live(cluster):
            for index in range(self._applied_upto[node.node_id] + 1, node.last_applied + 1):
                entry = node.storage.entry_at(index)
                first, first_node, tick = self.applied.setdefault(
                    index, (entry, node.node_id, cluster.now)
                )
                if entry != first:
                    itself = " (before restarting)" if first_node == node.node_id else ""
                    _fail(
                        "State Machine Safety",
                        cluster.now,
                        f"{node.node_id} applied index {index} from term {entry.term} = "
                        f"{entry.command}, but {first_node}{itself} applied index {index} from "
                        f"term {first.term} = {first.command} at tick {tick}",
                    )
                newly_applied.append((node.node_id, index, entry))
            self._applied_upto[node.node_id] = node.last_applied
        return newly_applied

    # --- 5. No lost acknowledged writes ---

    def _check_no_lost_acknowledged_writes(
        self, cluster: SimCluster, newly_applied: list[tuple[str, int, Entry]]
    ) -> None:
        """Every node that has applied an acknowledged write's index must show it in its KV store.

        We replay what each node applied into a plain dict of our own (not KVStore, so this checks
        the driver independently). The node's KV store must hold the same value for the write's key:
        the write's value, or a later one if the node has since applied another write to that key.
        The entry itself is State Machine Safety's job; this checks what a client would read.

        Checked when a node applies the write's index, and when the write is acknowledged (for nodes
        that applied it earlier). In between, a KV store changes only by applying entries.
        """
        to_check: set[tuple[str, int]] = set()
        for node_id, index, entry in newly_applied:
            command = entry.command
            if command.get("op") == "set":
                self._expected_kv[node_id][command["key"]] = command["value"]
            elif command.get("op") == "delete":
                self._expected_kv[node_id].pop(command["key"], None)
            proposer, _ = self.leaders.get(entry.term, (None, None))
            proposed_here = command.get("op") == "set" and node_id == proposer
            if proposed_here and index not in self.acknowledged:
                self.acknowledged[index] = (entry, cluster.now)
                to_check.update(
                    (other, index)
                    for other, upto in self._applied_upto.items()
                    if cluster.nodes[other] is not None and upto >= index
                )
            if index in self.acknowledged:
                to_check.add((node_id, index))

        for node_id, index in sorted(to_check):
            entry, acknowledged_at = self.acknowledged[index]
            key = entry.command["key"]
            actual = cluster.kvs[node_id].data.get(key)
            expected = self._expected_kv[node_id].get(key)
            if actual != expected:
                _fail(
                    "No lost acknowledged writes",
                    cluster.now,
                    f"{node_id} has applied up to index {self._applied_upto[node_id]}, past the "
                    f"write {key}={entry.command['value']!r} acknowledged at tick "
                    f"{acknowledged_at} (index {index}, term {entry.term}), but its KV store has "
                    f"{key}={actual!r} where the entries it applied give {key}={expected!r}",
                )


def _live(cluster: SimCluster) -> list[RaftNode]:
    return [node for node in cluster.nodes.values() if node is not None]


def _first_difference(old: list[Entry], new: list[Entry]) -> int:
    """Where two logs first differ, or the shorter one's length if one extends the other."""
    if len(new) >= len(old) and new[: len(old)] == old:
        return len(old)  # the usual case: entries were only appended (a fast comparison in C)
    position = 0
    while position < min(len(old), len(new)) and old[position] == new[position]:
        position += 1
    return position
