# Plan

The work is split into small tasks. Each task is **one commit with its tests**, sized so the user can review it
in a few minutes and any agent can pick it up cold. The spec is in `DESIGN.md`, and the requirements are in `ASSIGNMENT.md`.

## Status

`todo` → `in-progress` → `review` (committed, waiting for the user) → `done` (approved by the user)

| ID | Task | Depends on | Status |
|---|---|---|---|
| T01 | Project scaffold: uv, Python 3.12, pytest, empty packages | n/a | review |
| T02 | `raft/messages.py`: message dataclasses + JSON round trip | T01 | todo |
| T03 | `raft/storage.py`: `Storage` protocol + `MemoryStorage` | T02 | todo |
| T04 | `raft/kv.py`: `KVStore` (set / delete / noop) | T01 | todo |
| T05 | `sim/network.py`: `SimNetwork` (delay, drop, dup, partition) | T02 | todo |
| **G1** | **Gate: foundations reviewed** | T02–T05 | waiting |
| T06 | `RaftNode` skeleton: roles, election timer, start election, step down on higher term (R2) | G1 | todo |
| T07 | Voting and leadership: grant rules (R3, R4), count votes, heartbeats | T06 | todo |
| T08 | `sim/cluster.py`: `SimCluster` + `tests/test_election.py` | T07 | todo |
| **G2** | **Gate: leader election reviewed** | T06–T08 | waiting |
| T09 | Log replication: `propose`, consistency check, conflict truncation (R5), backtracking | G2 | todo |
| T10 | Commit: majority rule (R6), `leader_commit`, `take_committed`, noop on election (R7) | T09 | todo |
| T11 | `SimCluster.write` + KV apply + `tests/test_replication.py` | T10 | todo |
| **G3** | **Gate: replication reviewed** | T09–T11 | waiting |
| T12 | `tests/test_failures.py`: restart (R1), minority leader, heal (R2), no majority | G3 | todo |
| T13 | `test_figure8`: scripted Figure 8 scenario (R6) | T12 | todo |
| T14 | `tests/invariants.py`: the 5 invariants, run after every simulator step | T12 | todo |
| T15 | `tests/test_fuzz.py`: randomized fault schedules over many seeds | T14 | todo |
| **G4** | **Gate: correctness evidence complete** | T12–T15 | waiting |
| T16 | KV duplicate detection by `(client_id, seq)` | G4 | todo |
| T17 | `server/file_storage.py`: `FileStorage` | G4 | todo |
| T18 | `server/node_server.py`: `/raft`, `/state`, `/admin/partition`, tick loop, `step()` | T17 | todo |
| T19 | `POST /client/write` with waiting writes (200 / 421 / 503 / 504) | T18 | todo |
| T20 | `Dockerfile` + `docker-compose.yml` (**the user must install Docker first**) | T19 | todo |
| **G5** | **Gate: cluster runs in Docker** | T16–T20 | waiting |
| T21 | `dashboard/app.py`: cluster fan-out, write, partition/heal, kill/start | G5 | todo |
| T22 | `dashboard/static/index.html`: node cards, controls, event feed, auto-write | T21 | todo |
| T23 | `README.md`: design summary, trade-offs, how to run, AI usage (from `AI_NOTES.md`) | T22 | todo |
| **G6** | **Gate: ready to submit** | all | waiting |

## How to work a task (any agent)

1. **Read** `CLAUDE.md` (loaded automatically), `DESIGN.md` and this file. Skip `.lavish/design.html`: it's large, and `DESIGN.md` is the canonical version.
2. **Pick** the task the user named. Otherwise pick the first `todo` task whose dependencies are `review` or `done`.
   **Never start past a gate whose status is `waiting`.** Stop and tell the user the gate is ready for review.
3. **Stay in scope.** Touch only the files the task names, plus `PLAN.md` and `AI_NOTES.md`.
   If the design is wrong or unclear, make the smallest reasonable choice, write it in the task's notes below,
   and flag it in your report. Don't silently redesign. Interfaces must match `DESIGN.md`.
4. **Test.** Write the tests together with the code. `uv run pytest -q` (the **whole** suite) must pass before committing.
   Tests must be deterministic: seed every `random.Random`.
5. **Record.** In this file, set the task's status to `review` and add a one-line note under the task.
   If something worth telling the panel happened, add an entry to `AI_NOTES.md`: an AI mistake you caught, a judgment call, or a design gap.
6. **Commit once.** Stage files by name (no `git add -A`). Use the message format:
   ```
   T05: Add simulated network with delay, drops and partitions

   2–5 lines: what changed and why, and anything the reviewer should look at first.

   Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
   ```
   Never push unless the user asks.
7. **Report** in 10 lines or fewer: files changed, tests added, decisions made, and anything the user should look at.

The user marks tasks `done` and gates `approved` after review. Agents never do.

## Task details

### T01 · Project scaffold
`pyproject.toml` (requires Python ≥ 3.12, aiohttp, pytest dev group, `pythonpath = ["."]`), `.python-version`,
`.gitignore`, empty `raft/ sim/ server/ dashboard/` packages, `tests/test_smoke.py`, a stub `README.md`.
**Done when:** `uv run pytest -q` passes.
*Notes:* done by the orchestrator. `uv sync` + `uv run pytest -q` give 2 passed.

### T02 · Messages
`raft/messages.py`, `tests/test_messages.py`. Five dataclasses exactly as in DESIGN.md, plus `Message`, `to_dict` and `from_dict`.
**Done when:** every message type round-trips through `json.dumps`/`json.loads`, entries included, and an unknown `type` raises `ValueError`.

### T03 · Storage
`raft/storage.py`, `tests/test_storage.py`. `Storage` Protocol and `MemoryStorage` with the index-0 sentinel.
**Done when:** the tests cover `term_at(0) == 0`, append, `entries_from`, `truncate_from` (middle, end, past the end) and `set_term_and_vote`.

### T04 · KV store
`raft/kv.py`, `tests/test_kv.py`. `KVStore.apply` handles set, delete (a missing key is fine) and noop. An unknown op raises `ValueError`.
No duplicate detection yet (that's T16).

### T05 · Simulated network
`sim/network.py`, `tests/test_network.py`. As specified in DESIGN.md. Partitions are checked **at delivery time**.
**Done when:** the tests show delivery after the delay, reordering, drop_rate=1 dropping everything, duplication,
partition and heal, and the same seed producing the same delivery sequence.

### T06 · Node skeleton
`raft/node.py`, `tests/test_node_election.py`. `Role`, `RaftConfig` and `RaftNode` with outputs (`take_messages`,
`take_committed` returning `[]` for now, `take_events`). Timer: a follower or candidate times out after a random election
timeout, then becomes a candidate, increments the term, votes for itself (saved first), and sends `RequestVote` to all peers.
R2: a higher term in any message → follower. Lower-term messages are ignored.
**Done when:** unit tests drive a single node by calling `tick()`/`receive()` and asserting on `take_messages()`.

### T07 · Voting and leadership
`raft/node.py`, `tests/test_node_election.py`. Handle `RequestVote`: R3 (one vote per term, up-to-date check) and R4 (reset the timer
only when granting). Handle `RequestVoteReply` and count the majority. On becoming leader, initialize `next_index`/`match_index` and send
heartbeats every `heartbeat_interval`. A follower accepts a current-term heartbeat, records `leader_id` and resets its timer (R4).
Candidates step down on an AppendEntries from a current-term leader. Leave the R7 noop for T10.

### T08 · Simulated cluster + election tests
`sim/cluster.py`, `tests/test_election.py`. `SimCluster` as in DESIGN.md (`write` comes in T11). Tests:
exactly one leader is elected within a bound, re-election after crashing the leader, split votes resolve across many seeds,
`test_outdated_log_cannot_win` (set up logs through `storages` directly), `test_follower_keeps_leader_under_load`.

### T09 · Log replication
`raft/node.py`, `tests/test_node_replication.py`. `propose` (leader only), AppendEntries carrying entries, the follower consistency
check, R5 truncation, replies with `match_index`/`last_log_index`, and leader backtracking. Include
`test_reordered_append_does_not_truncate`.

### T10 · Commit
`raft/node.py`, `tests/test_node_replication.py`. The leader advances `commit_index` with R6, followers follow `leader_commit`,
`take_committed` hands out `(index, entry)` once, and the R7 noop is appended on election. Include `test_new_leader_commits_previous_entries`.

### T11 · Replication through the cluster
`sim/cluster.py`, `tests/test_replication.py`. `SimCluster.write` plus applying to the per-node `KVStore`. Tests: a write applies
on all nodes, a lagging follower catches up after restart, conflicting uncommitted entries get overwritten, and all KV stores converge.

### T12 · Failure tests
`tests/test_failures.py` (small cluster helpers allowed). `test_restart_does_not_double_vote`, minority leader can't commit,
`test_stale_leader_steps_down` after heal with its entries overwritten, no majority means no leader and no commits, restoring a node brings progress back.

### T13 · Figure 8
`tests/test_failures.py`. Script the Figure 8 scenario from the Raft paper deterministically (partitions/crashes to control
who gets which entries). Assert that a previous-term entry replicated on a majority is **not** reported committed until a
current-term entry commits. Test-only helpers in `sim/` are fine. Don't change node logic to make the test easier.

### T14 · Invariants
`tests/invariants.py`, wired into every `SimCluster` test (fixture or `checkers`). The 5 invariants from DESIGN.md, each with
a clear failure message that includes the tick and node IDs.

### T15 · Fuzz
`tests/test_fuzz.py`. Per seed: random crashes and restarts (never more than a minority down at once for the liveness check),
partitions and heals, drop/dup/delay, random writes. Invariants are checked every tick. At the end: heal, restart everyone, and
require a leader, every acknowledged write applied everywhere, and equal KV stores. Seeds come from `FUZZ_SEEDS` (default sized so the
whole suite runs in about 30 s). A failure message prints the seed.

### T16 · Duplicate detection
`raft/kv.py`, `tests/test_kv.py`. Skip a command whose `(client_id, seq)` is ≤ the last applied seq for that client.

### T17 · FileStorage
`server/file_storage.py`, `tests/test_file_storage.py`. Implements `Storage`, using temp file + fsync + `os.replace`. Tests use `tmp_path`:
reopening the same path restores term, vote and log, and a leftover `.tmp` file is ignored.

### T18 · Node server
`server/node_server.py`, `tests/test_node_server.py` (aiohttp test client). Configured by environment variables. `step()` helper, tick loop,
`POST /raft`, `GET /state`, `POST /admin/partition`, the blocked-peer filter, events stamped with time.
It may add `pytest-aiohttp` to the dev group in `pyproject.toml` (via `uv add --dev`).

### T19 · Client writes
`server/node_server.py`, `tests/test_node_server.py`. `POST /client/write` with waiting writes resolved on apply (same term → 200,
different term → 503), a 5 s timeout → 504, and not leader → 421 with a leader hint.

### T20 · Docker
`Dockerfile`, `docker-compose.yml`, `.dockerignore`. Check with `docker compose up --build` and `curl localhost:<port>/state`.
**Blocked until the user installs Docker Desktop or OrbStack.**

### T21 · Dashboard backend
`dashboard/app.py`, `tests/test_dashboard.py` (fake nodes; the Docker socket calls can be stubbed).

### T22 · Dashboard UI
`dashboard/static/index.html`. Plain HTML/CSS/JS, no CDN. See the mockup in `.lavish/design.html` §7.

### T23 · README
`README.md`. Design summary with the trade-offs, how to run the tests and the demo, the four demo scenarios, and how AI was used
(summarized from `AI_NOTES.md`).
