# Raft Primer

Background notes on Raft for this project. The requirements are in [ASSIGNMENT.md](../ASSIGNMENT.md).
Why I chose Raft over the other algorithms is in [consensus-alternatives.md](consensus-alternatives.md).

## What problem Raft solves

You want several machines (usually 3 or 5) to act like one reliable machine. If one crashes, the
others carry on and no data is lost.

The standard way to do this is called a **replicated state machine**:
- Every node keeps a **log**, which is an ordered list of commands such as `set x=5` and `delete y`.
- Every node applies the commands in its log, in order, to its own copy of the state (for example a
  key-value store).
- If every node has the **same log in the same order**, every node ends up with the **same state**.

So the whole problem comes down to getting the nodes to agree on the log, even when machines crash
and messages are lost, delayed or reordered. Agreeing on that is called **consensus**, and Raft is
an algorithm for it.

Diego Ongaro and John Ousterhout published Raft in 2014 as an easier-to-understand alternative to
Paxos. etcd (the store behind Kubernetes), Consul, CockroachDB and Kafka all use it.

## The core idea: one leader, majority rules

1. The nodes **elect a leader**.
2. All writes go to the leader. It appends each command to its log and sends it to the followers.
3. Once a **majority** of nodes have stored an entry, it is **committed**. It is now permanent and
   safe to apply.

**Why a majority?** Any two majorities of the same cluster always share at least one node. That one
shared node is what stops two conflicting decisions from both succeeding. It also sets the fault
tolerance: a 5-node cluster keeps working with 2 nodes down, and a 3-node cluster with 1 down.
In general, 2f + 1 nodes tolerate f failures.

## Terms: Raft's logical clock

Time is divided into **terms**, numbered 1, 2, 3 and so on. Each term starts with an election, and
there is at most one leader per term.

Every message carries the sender's term. If a node sees a higher term than its own, it updates to
that term and immediately becomes a follower. That is how an old leader that was partitioned off or
paused finds out it has been replaced.

## Three roles

```
             election timeout             wins majority of votes
  Follower ─────────────────► Candidate ─────────────────────► Leader
     ▲                            │                               │
     └────────────────────────────┴───────────────────────────────┘
                 sees a higher term, or a valid leader
```

- **Follower:** passive. It responds to the leader and to candidates.
- **Candidate:** trying to become leader.
- **Leader:** handles client requests and sends heartbeats to the followers.

## Leader election

- Every follower runs an **election timeout**. If it hears nothing from a leader for that long, it
  assumes the leader is dead.
- It then becomes a candidate, increments the term, votes for itself, and sends `RequestVote` to
  everyone else.
- Each node votes **at most once per term**. It only votes for a candidate whose log is at least as
  up to date as its own. "Up to date" means the last entry has a higher term, or the same term and
  a higher or equal index.
- A candidate that gets votes from a majority becomes leader. It starts sending heartbeats right away
  so the others don't start elections of their own.
- Timeouts are **randomized** (for example 150–300 ms). That way one node usually times out first
  and wins, instead of several splitting the vote forever.

## Log replication

Say a client sends `set x=5` to the leader:

1. The leader appends `(term=3, set x=5)` to its log at index 7.
2. It sends `AppendEntries` to the followers. The message includes the index and term of the entry
   just before the new one (`prevLogIndex=6, prevLogTerm=3`).
3. Each follower checks whether it also has that previous entry. If it does, it appends the new entry
   and replies that it succeeded. If it doesn't, it rejects the message, and the leader retries with
   an earlier starting point until the two logs match.
4. Once a majority has index 7, the leader marks it committed and applies it. Followers learn the new
   commit point in the next message and apply it too.

That consistency check on the previous entry is what keeps all the logs identical. Heartbeats are
just `AppendEntries` messages with no new entries.

## Why committed data is never lost

Raft's main guarantee is that **once an entry is committed, every future leader will have it.** Two
rules make this work:

1. **The voting rule:** a node only votes for candidates whose log is at least as up to date as its
   own. A committed entry is on a majority of nodes, and a winner needs votes from a majority. So
   any winner got at least one vote from a node that holds the entry, and it couldn't have won that
   vote without having the entry itself.
2. **The commit rule:** a leader only commits entries from its **own term** by counting replicas.
   Entries from earlier terms become committed indirectly, as part of committing a current-term
   entry. The "Figure 8" scenario in the paper shows how ignoring this rule loses data, and it's a
   classic implementation bug.

## Safety properties to test

These are the properties the paper proves. They are what the invariant checker should verify after
every simulation step:

| Property | Meaning |
|---|---|
| Election Safety | At most one leader per term |
| Leader Append-Only | A leader never overwrites or deletes entries in its own log |
| Log Matching | If two logs have an entry with the same index and term, the logs are identical up to that index |
| Leader Completeness | A committed entry appears in the logs of all leaders of later terms |
| State Machine Safety | No two nodes apply different commands at the same index |

## What Raft doesn't handle

- **Nodes that lie or misbehave on purpose** (called Byzantine faults). Raft assumes nodes are honest
  and only crash or slow down.
- **Losing the majority.** If a majority is unreachable, the cluster stops accepting writes. It
  chooses consistency over availability.
- **Throughput.** All writes go through the leader, so the leader limits how fast the cluster can
  write.

## Why it suits this assignment

The whole algorithm uses just **two message types**, `RequestVote` and `AppendEntries`, plus a
handful of state variables per node:

| State | Persistent? | Notes |
|---|---|---|
| `current_term` | Yes | Must be saved before replying to any message |
| `voted_for` | Yes | Must be saved so a restarted node can't vote twice in a term |
| `log` | Yes | The replicated commands |
| `role` | No | Follower, candidate or leader |
| `commit_index` | No | Highest entry known to be committed |
| `last_applied` | No | Highest entry applied to the state machine |
| `next_index[]`, `match_index[]` | No (leader only) | Per follower: next entry to send, highest entry known to be replicated |

That makes it small enough to build in a day, while the edge cases are still interesting enough to
make a good panel discussion.

## Common implementation bugs

- **Old-term commits:** committing an entry from an earlier term once a majority has it (Figure 8).
- **Over-eager truncation:** when a follower gets an AppendEntries, always deleting everything after
  `prevLogIndex`. It should only truncate when an existing entry actually conflicts (same index,
  different term). Otherwise a delayed old message can delete entries that were already committed.
- **Election timer resets:** resetting the timer on any message. It should only reset on a valid
  AppendEntries from the current leader, on granting a vote, or on starting an election.
- **Persistence order:** replying to a request before saving the term, vote or log.
- **Missed step-downs:** a leader or candidate not stepping down when a *reply* carries a higher term.

## Resources

- [The Secret Lives of Data: Raft](https://thesecretlivesofdata.com/raft/): a 5-minute animated
  walkthrough. Best place to start.
- [raft.github.io](https://raft.github.io/): the paper, with an interactive simulation you can click
  to crash nodes. Figure 2 of the paper is the complete rulebook on one page.
- [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/): the common
  implementation bugs. Read it before writing the node logic.
