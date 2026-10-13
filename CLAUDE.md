# CLAUDE.md

A hand-written Raft implementation in Python that replicates a key-value store. It's a take-home assignment.

## Read these first
- @ASSIGNMENT.md: the requirements and the evaluation criteria.
- `DESIGN.md`: the canonical spec, with interfaces, rules R1–R7 and safety properties. Code must match it.
- `PLAN.md`: the task list, statuses, review gates and **the protocol for working a task**. Follow it exactly.
- `AI_NOTES.md`: a log of how AI was used. Append to it when something notable happens.
- `.lavish/design.html`: the reviewed visual design (98 KB). Only open it when DESIGN.md isn't enough.

## Non-negotiables
- Consensus and replication logic is written by hand. Never add a dependency that implements consensus, replication or
  coordination (Raft/Paxos libraries, etcd, ZooKeeper, etc.). The standard library, aiohttp, pytest and general tooling are fine.
- The author has to explain and defend every line at the panel. Prefer small, readable code over clever code,
  and call out any decision that affects correctness so it can be checked.
- **Simplicity first.** Keep it small and done well. Respect each task's line budget in PLAN.md, prefer the simple version even if it
  handles fewer edge cases (and write the limitation down), write only the tests the task lists, and keep comments to one line.
  Don't add scope that isn't in PLAN.md.
- `raft/` never does I/O: no sockets, threads, clocks, files or asyncio.

## Workflow
- Python 3.12 via uv. Run tests with `uv run pytest -q`. The whole suite must pass before every commit.
- One PLAN.md task = one commit, with message format `T07: <imperative summary>`. Stage files by name.
- Never push, and never mark tasks `done` or gates `approved`. Only the user does that.
