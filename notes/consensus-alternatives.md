# Consensus Algorithms: Alternatives to Raft

Why I chose Raft for this project, and what each alternative does. For how Raft itself works, see
[raft-primer.md](raft-primer.md).

## Summary

| Algorithm | What it does | Why not for this assignment |
|---|---|---|
| Basic Paxos | Agrees on a single value | Doesn't replicate a log on its own |
| Multi-Paxos | Paxos per log slot, with a stable leader | Same capability as Raft, but leaves key details unspecified |
| Viewstamped Replication | Leader-based log replication with "views" | Valid choice; fewer resources, messier leader change |
| Zab | ZooKeeper's leader-based broadcast | Built for ZooKeeper; more phases, less self-contained spec |
| Two-phase commit | All-or-nothing commit across participants | Not consensus; one crash can block everything |
| Primary-backup / chain replication | Copies writes from a primary to backups | Needs a separate consensus service to handle failures |
| EPaxos | Leaderless consensus | Far too complex for a day |
| PBFT and other BFT protocols | Tolerates malicious nodes | Solves a harder problem than asked |
| Gossip / CRDTs / Dynamo-style | Replicas converge eventually | Not consensus at all |

## Group 1: Same family as Raft

These all do what Raft does: a leader puts commands in order in a log that is copied to a majority.
Any of them would work. Each costs more in a one-day project and gains nothing.

### Basic (single-decree) Paxos

Leslie Lamport, written 1989, published 1998. The original consensus algorithm.

- **How it works:** proposers pick a unique, increasing proposal number `n`.
  - *Phase 1 (prepare/promise):* a proposer asks a majority of acceptors to promise to ignore any
    proposal numbered below `n`. Each acceptor that promises also reports any value it has already
    accepted.
  - *Phase 2 (accept):* if a majority promised, the proposer asks them to accept a value. It must
    use the highest-numbered value reported in phase 1, or its own value if none was reported. Once
    a majority accepts, the value is chosen.
- **Why not:** it agrees on *one* value, once. Replicating state needs a sequence of values, which
  means Multi-Paxos.

### Multi-Paxos

- **How it works:** run a separate Paxos instance for each slot in the log. A stable leader runs
  phase 1 once for all future slots, so each new command only needs phase 2. That's one round trip,
  the same as Raft.
- **Why not:**
  - **Under-specified.** The papers don't pin down leader election, how lagging replicas catch up,
    or how to fill gaps. You'd have to design those yourself, which is exactly what Raft already did.
  - **The log can have holes.** Slots are decided independently, so slot 8 can be chosen before
    slot 7. The state machine has to wait for gaps to fill, and a new leader has to fill them, often
    with no-op entries.
  - **Harder to explain** in a 15-minute walkthrough.
- **Worth knowing:** Raft is essentially a fully specified Multi-Paxos. Being able to say that at the
  panel shows you understand both.

### Viewstamped Replication (VR)

Oki and Liskov, 1988; revised by Liskov and Cowling, 2012. It predates the publication of Paxos.

- **How it works:** time is divided into *views*, similar to Raft's terms. The primary (leader) for
  each view is fixed in advance: view number mod cluster size. When the primary fails, a *view
  change* protocol moves to the next view. Replicas send their logs to the new primary, which adopts
  the most up-to-date one.
- **Why not:** honestly, it's a good choice and very close to Raft. But:
  - The new primary is chosen by rotation, not by how fresh its log is. It may be behind and has to
    learn missing entries during the view change. In Raft only an up-to-date node can win, so entries
    only ever flow from leader to follower. That's simpler to implement and reason about.
  - There are far fewer tutorials, guides and known test cases to check your work against.
- **Verdict:** defensible, but you'd spend panel time explaining the algorithm instead of your decisions.

### Zab (ZooKeeper Atomic Broadcast)

Junqueira, Reed and Serafini, 2011.

- **How it works:** leader-based, with *epochs* similar to terms. A new leader goes through
  *discovery* (learn the latest history from followers) and *synchronization* (bring followers up to
  date) before *broadcast* (normal replication).
- **Why not:** designed around ZooKeeper's needs, with more phases and a less self-contained
  specification than Raft. Same capability, more moving parts.

## Group 2: Smaller or different problems

### Two-phase commit (2PC)

- **How it works:** a coordinator asks every participant "can you commit?" If *all* say yes, it tells
  everyone to commit. Otherwise it tells everyone to abort.
- **What it's for:** atomic transactions across nodes that hold *different* data, for example a debit
  in one database and a credit in another. It's not about keeping copies identical.
- **Why not:**
  - It needs *every* participant, not a majority. One node being down blocks progress.
  - If the coordinator crashes after participants voted yes, they're stuck holding locks until it
    comes back.
  - Choosing it would suggest you've confused atomic commit with consensus, which is a red flag in
    this interview.
- **Three-phase commit** removes the blocking under some timing assumptions, but it is unsafe when
  the network partitions.

### Primary-backup and chain replication

- **How they work:**
  - *Primary-backup:* the primary executes writes and forwards them to backups.
  - *Chain replication* (van Renesse and Schneider, 2004): nodes form a chain. Writes enter at the
    head and pass down the chain. The tail acknowledges writes and serves reads. It gives high
    throughput and strong consistency.
- **Why not:** neither one decides who the primary is, or how to reconfigure after a failure. That
  job goes to a separate configuration service, which itself runs Paxos or Raft. It hands off the
  exact part the assignment wants to see.

## Group 3: Harder problems

### EPaxos (Egalitarian Paxos)

Moraru, Andersen and Kaminsky, 2013.

- **How it works:** there's no leader. Any replica can commit a command. Commands that don't conflict
  (for example, writes to different keys) commit in one round trip. Conflicting ones track
  dependencies on each other and need an extra round.
- **Why not:** much more complex, with dependency graphs, ordering cycles and an intricate recovery
  protocol. A correctness bug in its recovery procedure was published years after the original
  paper. Not a one-day project.

### PBFT and other Byzantine fault-tolerant protocols

Castro and Liskov, 1999. Later examples include HotStuff and Tendermint.

- **How they work:** they tolerate nodes that lie or act maliciously, not just nodes that crash. They
  need 3f + 1 nodes to survive f bad ones (Raft needs 2f + 1), extra voting rounds, and signed
  messages. In PBFT the number of messages grows with the square of the cluster size.
- **Why not:** they solve a harder problem than the one asked. Machines in a company's own cluster
  are trusted; the realistic failures are crashes and network faults.

### Flexible Paxos (worth mentioning, not choosing)

Howard, Malkhi and Spiegelman, 2016. It shows that the quorum used for elections and the quorum used
for replication don't each need to be a majority. They only need to overlap with each other. Not an
algorithm to build here, but a good discussion point if the panel asks about trading election cost
for write latency.

## Group 4: Not consensus at all

### Gossip, CRDTs and Dynamo-style quorums

- **How they work:** replicas accept writes independently and exchange updates in the background.
  - *CRDTs* use data types whose merges always converge, such as counters and sets.
  - *Dynamo-style systems* (Cassandra, Riak) read from and write to a subset of replicas and repair
    differences later, using version vectors to detect conflicts.
- **Why not:** they give *eventual* consistency. Replicas can disagree for a while, and there's no
  single agreed order of operations. The brief asks specifically about how consensus is achieved.
  Worth a line in the README as the contrast: what I'd use if availability mattered more than
  consistency.

### Proof of work (blockchain)

Designed for open networks where anyone can join and nobody is trusted. Agreement is only
probabilistic and confirmation takes minutes. Wrong setting entirely.

## Why Raft for this assignment

- **Fits the time box:** two message types and a handful of state variables.
- **Precisely specified:** Figure 2 of the paper is a checklist to verify code (and AI output) against.
- **Covers both parts of the brief:** consensus (leader election) and replication (log replication
  and commit).
- **The panel knows it:** time goes on my decisions, not on explaining the algorithm.
- **Clear extension paths:** the paper itself describes snapshots and membership changes, which are
  likely live-extension prompts.

## Draft paragraph for the README

> I chose Raft. Multi-Paxos and Viewstamped Replication solve the same problem, but Multi-Paxos
> leaves leader election and log catch-up unspecified, and VR's view change requires a new primary
> to learn missing entries from other replicas. Raft only lets an up-to-date node win an election,
> so entries only ever flow from leader to followers. That keeps the implementation small enough to
> test thoroughly in a day. I ruled out two-phase commit (atomic commit rather than consensus, and
> it blocks on a single failure), chain replication (it depends on an external consensus service),
> EPaxos (leaderless and much more complex), BFT protocols (they defend against malicious nodes,
> which isn't the threat model here), and eventually consistent approaches (not consensus).
