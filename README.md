# Raft KV

A hand-written implementation of the Raft consensus algorithm in Python. It replicates a small key-value store
across five nodes. It was built as a take-home assignment (the brief is in [ASSIGNMENT.md](ASSIGNMENT.md)).

- **The algorithm** is in `raft/`: about 700 lines, with no networking, threads, clocks or files inside it.
- **The evidence that it works** is a simulator (`sim/`) that runs the same code under random crashes, partitions and message
  loss, and checks Raft's safety properties after every step.
- **The demo** runs the same code again as five Docker containers talking over HTTP, with a live dashboard.

## Run it

You need [uv](https://docs.astral.sh/uv/) for the tests and Docker for the demo.

```bash
uv run pytest                    # 1,117 tests in about 14 seconds
```

```bash
docker compose up --build        # then open http://localhost:8080
```

`docker compose down -v` wipes all state. To run more fuzz seeds: `FUZZ_SEEDS=5000 uv run pytest tests/test_fuzz.py`.

## Design

**One core, two drivers.** `RaftNode` never does I/O. Time arrives as `tick()` and messages as `receive(msg)`. Anything the node
wants to send or apply is queued, and whatever is running it collects that with `take_messages()` and `take_committed()` after every call.

| Driver | Clock | Network | Storage | Used for |
|---|---|---|---|---|
| `sim/` | a counter | in memory, seeded: delays, drops, duplicates, partitions | in memory, kept across "crashes" | tests |
| `server/` | 100 ms timer | HTTP between containers | one JSON file per node, fsynced | the demo |

This is what lets a test replay any failure exactly from its seed, and it means the code you watch in Docker is the code the tests verified.
There are no threads anywhere: the server is one asyncio loop, and nothing awaits between calling the node and collecting its outputs.

**The rules that are easy to get wrong** each have a named test:

| Rule | Test |
|---|---|
| R1. Save term, vote and log before any message that depends on them is sent | `test_restart_does_not_double_vote` |
| R2. A higher term in any message, request or reply, makes a node step down | `test_stale_leader_steps_down` |
| R3. One vote per term, only for a candidate whose log is at least as up to date | `test_outdated_log_cannot_win` |
| R4. The election timer resets only on a valid AppendEntries, on granting a vote, or on starting an election | `test_follower_keeps_leader_under_load` |
| R5. A follower deletes entries only on a real conflict, never because a message is short or late | `test_reordered_append_does_not_truncate` |
| R6. A leader commits by counting copies only for entries from its own term | `test_figure8` |
| R7. A new leader appends a no-op entry, so entries from earlier terms can commit | `test_new_leader_commits_previous_entries` |

## How I know it works

Raft makes two kinds of promise, and they're tested differently.

**Nothing bad ever happens.** [tests/safety_checks.py](tests/safety_checks.py) checks five safety properties after every simulated tick of
every cluster test: at most one leader per term, logs that share an entry agree on everything before it, a committed entry is in every later
leader's log, no two nodes apply different entries at the same index, and no acknowledged write is lost. The first four are the properties the Raft paper
lists in its Figure 3.

**Something good eventually happens.** The scenario tests in [tests/test_failures.py](tests/test_failures.py) give the cluster a deadline in
ticks: a new leader after a crash, a minority-side leader that can't commit, progress again once a majority returns, and Figure 8 from the paper replayed step by step.

**The fuzz test** ([tests/test_fuzz.py](tests/test_fuzz.py)) runs 1,000 seeds by default. Each seed is a different schedule of crashes, restarts,
partitions, dropped and duplicated messages and client writes, with the safety properties checked every tick. A failure prints the seed and the command to replay it.

A test suite that never fails proves little, so the suite was checked against deliberate bugs:

| Bug put into `raft/node.py` | Caught by |
|---|---|
| R6's check removed (the Figure 8 bug) | All 20 Figure 8 cases, 7 of the default 1,000 fuzz seeds, 2 unit tests |
| R5 deleting entries unconditionally | About 63% of fuzz seeds, plus scenario and unit tests |
| A vote forgotten on restart | About 1.4% of fuzz seeds, plus scenario and unit tests |

I re-ran the first row myself. The other two rates come from the agent's run over 8,000 seeds.

## The demo

The dashboard shows each node's role, term, log (solid boxes are committed, dashed are not) and data, with an event list underneath.
It sends writes to whichever leader has the highest term. Nodes are killed and started from a terminal.

| Scenario | Do this | Watch for |
|---|---|---|
| Leader fails | `docker compose kill node3` (whichever is leader), later `docker compose start node3` | A new leader in a few seconds at a higher term. The old one rejoins as a follower and catches up |
| Follower fails | Kill a follower, write a few values, start it | Its log fills in from the leader |
| Partition | Click **Leader + 1 \| rest** and write a value straight away. Wait a few seconds, write again, then click **Heal** | The first write goes to the cut-off leader: its entry stays dashed and the write times out. The other side elects a leader at a higher term (two cards say LEADER; the term tells them apart) and the second write commits. After healing, the old leader steps down and its entry is replaced |
| No majority | Kill three nodes, then start one | No leader and no writes with two nodes. Progress returns with three |

## Trade-offs and limitations

I kept the scope to what one person can review and defend in a day. These are deliberate:

- **No snapshots, membership changes or pre-vote.** The log grows forever, the cluster is fixed at five nodes, and a node rejoining with a
  high term can force an unnecessary election.
- **Partitions in the demo are simulated inside each node** (a list of peers to ignore), not at the network level. The Raft code can't tell the difference.
- **A retried write can apply twice.** There's no duplicate detection. A write that times out may also still commit later.
- **Reads aren't linearizable.** The dashboard reads each node's local state, which is what makes disagreement visible.
- **Storage rewrites the whole log file on every change.** It's simple and correct, but slow for a long log.
- **R6 only matters with a batch cap.** A new leader's no-op plus sending the whole unconfirmed log means every confirmation already covers a
  current-term entry. `max_entries_per_append` exists so the Figure 8 test can show R6 doing its job, as it would in a system that limits message size.
- **Test deadlines are measured, not derived.** Each is about twice the slowest case seen, so changing the timeouts means re-measuring them.

## How I used AI

I used Claude Code throughout, and [AI_NOTES.md](AI_NOTES.md) is the full log. In short:

- **How the work was split.** I reviewed the design as an annotated page ([.lavish/design.html](.lavish/design.html)) and pushed back on it before any
  code existed. The work was then cut into small tasks in [PLAN.md](PLAN.md), each one commit, with review gates where everything stopped until I approved.
  Separate agents did the tasks from a short spec ([DESIGN.md](DESIGN.md)) instead of one long conversation.
- **How it was verified.** Every task ran the whole suite. Rules were broken on purpose to prove a test catches them, and the main claims were
  re-checked independently of the agent that made them.
- **Where the AI got it wrong, and how it was caught:**
  - It justified a design choice with an argument that didn't apply to our simulator. My question made it re-check and withdraw it.
  - Its serialization tests only round-tripped, so a mistake shared by both directions would pass. I caught this in review, and the wire format is now pinned by hand-written JSON.
  - Its first fuzz test was too weak: with R6 removed it failed 1 seed in 8,000. It reported this itself and added two more fault types.
  - It found that our Figure 8 test could never fail, because of how we batch entries. That led to the batch cap.
  - It kept choosing names I had to ask about ("durable", "invariants", "bounds"). I renamed them as I reviewed.
  - It over-built. Asked for thorough tests and explanations, it produced about 3,400 lines of tests for 900 lines of code. I cut the
    remaining scope, set line budgets, and trimmed the tests by a third.

## Layout

```
raft/        node.py (the algorithm), messages.py, storage.py, kv.py
sim/         network.py, cluster.py: the deterministic simulator
server/      node_server.py, file_storage.py: the HTTP runtime for the demo
dashboard/   app.py, static/index.html
tests/       unit, scenario, safety-check and fuzz tests
notes/       a Raft primer and why I chose Raft over the alternatives
```
