# Design: Raft KV

This is the canonical spec for implementers. It condenses the reviewed design in
`.lavish/design.html` (draft 3), which is the visual version with diagrams and rationale.
If the two disagree, **this file wins**. Change it only together with the user.

A hand-written Raft implementation in Python 3.12 that replicates a key-value store across 5 nodes.
- **Correctness evidence:** a deterministic simulator with randomized fault injection and invariant checks.
- **Demo:** the *same* Raft code running in Docker containers over HTTP, with a live dashboard.

## Decisions

Q1–Q10 were not answered explicitly, so the recommended options are in force. The user can override any of them.

| # | Decision |
|---|---|
| Q1 | **5 nodes** in the demo |
| Q2 | Dashboard crashes nodes with **Docker kill/start via the mounted docker.sock** |
| Q3 | Partitions = **a blocked-peer list inside each node** (app level) |
| Q4 | **One-way messages**: every Raft message is `POST /raft` → 204; a reply is a separate POST |
| Q5 | **aiohttp** is the only runtime dependency |
| Q6 | **uv + Python 3.12** (`uv run pytest`) |
| Q7 | Dashboard is **plain HTML/CSS/JS**, no CDN, no build step |
| Q8 | Extras in scope: client duplicate detection, dashboard event feed, auto-write toggle |
| Q10 | The node **queues outputs** (outbox); drivers collect them with `take_*()` after every call |
| Storage | Tests: `MemoryStorage`. Demo: one JSON file per node, temp file + fsync + `os.replace` on every change |

## Architecture: one core, two drivers

```
raft/    pure algorithm: no sockets, threads, clocks or files
sim/     deterministic driver for tests (fake clock, seeded fake network)
server/  real driver for the demo (aiohttp, 100 ms tick, HTTP, FileStorage)
```

The core never does I/O. Time arrives as `tick()`. Messages arrive as `receive(msg)`. Outputs are
queued inside the node and collected by the driver after **every** call:

```python
result = node.receive(msg)               # or node.tick() / node.propose(cmd)
for m in node.take_messages(): transport.send(m)
for index, entry in node.take_committed(): kv.apply(entry.command)
for event in node.take_events(): log(event)
```

There's no threading anywhere. The simulator is a loop, and the server is one asyncio event loop. Never put an `await`
between calling the node and collecting its outputs.

## `raft/messages.py`

All messages are `@dataclass(frozen=True, slots=True)` and carry `src: str`, `dst: str`, `term: int`.

| Class | Extra fields |
|---|---|
| `Entry` | `term: int`, `command: dict` (not a message: no src/dst) |
| `RequestVote` | `last_log_index: int`, `last_log_term: int` |
| `RequestVoteReply` | `granted: bool` |
| `AppendEntries` | `prev_log_index: int`, `prev_log_term: int`, `entries: tuple[Entry, ...]`, `leader_commit: int` |
| `AppendEntriesReply` | `success: bool`, `match_index: int`, `last_log_index: int` |

- `Message = RequestVote | RequestVoteReply | AppendEntries | AppendEntriesReply`
- `to_dict(msg) -> dict` adds `"type": <class name>`. `from_dict(d) -> Message` reverses it. JSON-safe.
- An entry's index is implied by position: `prev_log_index + 1 + i`.
- `AppendEntriesReply` describes itself, because messages are one-way:
  - On success, `match_index = prev_log_index + len(entries)`.
  - `last_log_index` is the follower's last index, sent as a backtracking hint on failure.

Commands: `{"op": "set", "key": str, "value": str}`, `{"op": "delete", "key": str}`, `{"op": "noop"}`.
They may also carry `"client_id": str, "seq": int` for duplicate detection.

## `raft/storage.py`

```python
class Storage(Protocol):
    term: int                    # read-only property
    voted_for: str | None        # read-only property
    def set_term_and_vote(self, term: int, voted_for: str | None) -> None: ...
    def last_index(self) -> int: ...              # 0 when empty
    def term_at(self, index: int) -> int: ...     # term_at(0) == 0
    def entry_at(self, index: int) -> Entry: ...  # 1-based
    def entries_from(self, index: int) -> list[Entry]: ...  # index..last (may be empty)
    def append(self, entries: Sequence[Entry]) -> None: ...  # after last_index
    def truncate_from(self, index: int) -> None: ...          # delete index..last
```

**Bounds (decided in T03):** `term_at` raises `IndexError` outside `0..last_index`, and `entry_at` outside `1..last_index`.
`entries_from` / `truncate_from` raise below 1, and are a harmless `[]` / no-op past the end. **The node must check
`prev_log_index <= last_index()` before calling `term_at(prev_log_index)`.**

**Contract:** when a mutating method returns, the change is durable. `MemoryStorage` keeps the
log as a list with a sentinel `Entry(0, {"op": "noop"})` at index 0. The simulator keeps a
crashed node's `MemoryStorage` and gives it back on restart; that's how a "disk" survives in tests.

## `raft/node.py`

```python
class Role(str, Enum): FOLLOWER = "follower"; CANDIDATE = "candidate"; LEADER = "leader"

@dataclass
class RaftConfig:
    heartbeat_interval: int = 10      # ticks
    election_timeout_min: int = 30    # ticks; actual timeout is random in [min, max)
    election_timeout_max: int = 60

class RaftNode:
    def __init__(self, node_id: str, peers: list[str], storage: Storage,
                 rng: random.Random, config: RaftConfig = RaftConfig()): ...
    # inputs
    def tick(self) -> None
    def receive(self, msg: Message) -> None
    def propose(self, command: dict) -> tuple[int, int] | None   # (index, term) if leader, else None
    # outputs (each returns the queued items and clears them)
    def take_messages(self) -> list[Message]
    def take_committed(self) -> list[tuple[int, Entry]]          # (index, entry), in log order
    def take_events(self) -> list[str]                           # e.g. "became leader (term 4)"
    # read-only state
    node_id, peers, role, term, voted_for, leader_id, commit_index, last_applied, storage
```

- `peers` excludes `node_id`. Majority = `(len(peers) + 1) // 2 + 1`.
- Volatile state (`role`, `commit_index`, `last_applied`, `next_index`, `match_index`, timers) is
  rebuilt from scratch on restart. `term`, `voted_for` and the log live in `storage`.
- `propose` on a leader appends the entry and immediately sends AppendEntries to all peers.

### Rules (each one has a named test)

| # | Rule | Test |
|---|---|---|
| R1 | Save term/vote/log **before** queuing any message that depends on it (falls out of the outbox design) | `test_restart_does_not_double_vote` |
| R2 | Any message, request *or reply*, with a higher term → adopt that term, clear the vote, become follower | `test_stale_leader_steps_down` |
| R3 | Grant at most one vote per term, only if the candidate's log is at least as up to date (last term, then last index) | `test_outdated_log_cannot_win` |
| R4 | Reset the election timer only on: a valid AppendEntries from the current-term leader, granting a vote, starting an election | `test_follower_keeps_leader_under_load` |
| R5 | A follower truncates only on a real conflict (same index, different term), never just because entries extend past `prev_log_index` | `test_reordered_append_does_not_truncate` |
| R6 | The leader advances `commit_index` by counting replicas **only for entries of its current term** | `test_figure8` |
| R7 | A new leader immediately appends a `noop` entry in its own term | `test_new_leader_commits_previous_entries` |

Also:
- Ignore messages with a lower term. Requests still get a reply carrying the receiver's term.
- Ignore stale replies: wrong term, or a `match_index` lower than already known.
- Backtracking: on failure, `next_index = max(1, min(next_index - 1, reply.last_log_index + 1))`.
- A follower sets `commit_index = max(commit_index, min(leader_commit, index of last new entry))`.

## `raft/kv.py`

`KVStore.apply(command) -> None` handles set, delete and noop. `KVStore.data: dict[str, str]`.
Duplicate detection (a separate task): skip a command whose `(client_id, seq)` is ≤ the last applied seq for that client.

## `sim/`

```python
class SimNetwork:
    def __init__(self, rng, min_delay=1, max_delay=3, drop_rate=0.0, dup_rate=0.0): ...
    def send(self, msg, now: int) -> None             # may drop or duplicate; schedules delivery
    def deliver_due(self, now: int) -> list[Message]  # due messages, by (due time, send order)
    def partition(self, groups: list[set[str]]) -> None   # cross-group messages dropped AT DELIVERY
    def heal(self) -> None

class SimCluster:
    def __init__(self, n=5, seed=0, config=RaftConfig(), **network_kwargs): ...
    ids: list[str]                        # "n1".."n5"
    nodes: dict[str, RaftNode | None]     # None while crashed
    storages: dict[str, MemoryStorage]    # survive crashes
    kvs: dict[str, KVStore]               # rebuilt on restart
    now: int
    checkers: list[Callable[["SimCluster"], None]]   # run after every step (invariants)
    def step(self) -> None    # now+=1; tick live nodes; deliver due msgs to live nodes; collect outputs; apply; run checkers
    def run(self, ticks: int) -> None
    def run_until(self, cond: Callable[[], bool], max_ticks: int = 2000) -> bool
    def leader(self) -> str | None        # live leader with the highest term
    def crash(self, node_id) / restart(self, node_id) / partition(*groups) / heal()
    def write(self, key, value, max_ticks=1000) -> bool   # propose on the leader, run until applied there with the same term
```

All randomness comes from one `random.Random(seed)`, so the same seed gives the same run.

`SimNetwork` details (decided in T05):
- Delays are inclusive `[min_delay, max_delay]`, and a duplicate gets its own delay.
- Nodes not named in `partition()` form one extra group together, so `partition([{"n1"}])` isolates n1.
- A new `partition()` replaces the old one.
- A message blocked at delivery is gone for good.

## Invariants (`tests/invariants.py`, checked after every simulator step)

1. **Election Safety:** at most one leader per term, ever (keep a record of term → leader).
2. **Log Matching:** if two logs share (index, term), they are identical up to that index.
3. **Leader Completeness:** every committed entry is in the log of every later-term leader.
4. **State Machine Safety:** no two nodes apply different entries at the same index.
5. **No lost acknowledged writes:** an acknowledged write stays applied on every node that has caught up that far.

## `server/` (demo runtime)

- Environment variables: `NODE_ID`, `PEERS` (`id=host:port,...`), `PORT` (8000), `DATA_DIR` (`/data`), `TICK_MS` (100).
- One `step(call)` helper wraps every call into the node:
  - It sends `take_messages()` as fire-and-forget `asyncio.create_task(post)` calls with a 500 ms timeout, ignoring errors.
  - It applies `take_committed()` to the KV store, resolves waiting client writes, and stamps `take_events()` with wall-clock time.
- `FileStorage(path)` implements `Storage`. On every mutation it writes `{term, voted_for, log}` to `path + ".tmp"`, flushes, fsyncs, then `os.replace`s it over `path`.
- Partition filter: a `blocked` set of peer IDs. Drop outbound messages to them and inbound messages from them.
  Control endpoints are never filtered.

| Endpoint | Behavior |
|---|---|
| `POST /raft` | One message (JSON) → 204. Dropped if the sender is blocked |
| `POST /client/write` | `{key, value[, client_id, seq]}`. Not leader → `421 {leader}`. Applied with the same term → `200 {index, term}`. Applied with a different term (overwritten) → `503`. Not applied within 5 s → `504` |
| `GET /state` | `{id, role, term, voted_for, leader_id, commit_index, last_applied, log, kv, blocked, events}` |
| `POST /admin/partition` | `{blocked: [ids]}` → 204. `[]` heals |

## `dashboard/`

- `app.py` (aiohttp):
  - `GET /` serves `static/index.html`.
  - `GET /api/cluster` sends `GET /state` to every node (300 ms timeout; no answer = down).
  - `POST /api/write` writes to the leader, following a `421`.
  - `POST /api/partition {groups}` and `POST /api/heal`.
  - `POST /api/nodes/{id}/kill|start` use the Docker Engine API over `/var/run/docker.sock`. Kill sends SIGKILL.
- `static/index.html`: polls every 500 ms. It shows one card per node (role, term, up/down, log boxes solid when committed and dashed when not, KV contents), a write box, an auto-write toggle, partition presets (isolate leader / leader + 1 | rest / heal), Kill/Start per node, and a merged event feed.

## Docker

- One image (`python:3.12-slim` + aiohttp) for all containers.
- `docker-compose.yml` defines `node1`–`node5` (fixed `container_name`, a named volume at `/data`, `restart: "no"`)
  and `dashboard` (port 8080, `/var/run/docker.sock` mounted).
- `docker compose down -v` resets everything.

## Demo scenarios

1. Kill the leader.
2. Kill a follower, write, restart it.
3. Partition {leader + 1} | {3}, write to both sides, heal.
4. Kill 3 of 5 (no majority), then restart one.

## Out of scope

Snapshots, membership changes, pre-vote/check-quorum, linearizable reads, real network fault injection, auth/TLS, performance.
