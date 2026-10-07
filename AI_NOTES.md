# AI usage log

A running log of how AI tools were used: what was delegated, how it was verified, and where the tools got it wrong.
The README's AI section is summarized from this file. Newest entries go at the bottom.

## 2026-10-06 · Requirements and background
- **Delegated:** Claude Code turned the assignment PDF into `ASSIGNMENT.md`, a structured checklist written in its own words.
  It also wrote `notes/raft-primer.md` and `notes/consensus-alternatives.md`, which explain Raft and why the alternatives were ruled out.
- **Verified:** I read the primer and the comparison, and asked follow-up questions before choosing Raft.

## 2026-10-06 · Design review in Lavish
- **Delegated:** Claude drafted the design as an annotated HTML page (`.lavish/design.html`, using the Lavish skill), with diagrams,
  a dashboard mockup, the test plan, risks and ten decisions.
- **My pushback, and what changed:**
  - *"Why do we need the latest Python?"* We don't. We need a supported version: 3.9 is end-of-life, and the Docker image uses 3.12.
  - *"Do we even need file storage?"* In tests, no. In the demo, yes: a killed container loses its memory, so without a file it could vote twice or forget acknowledged entries (rule R1). I asked to keep it to one file with an fsync per write. Claude kept two extra lines for an atomic rename and explained the half-written-file problem they prevent.
  - *"Isn't the outbox complicating things, and can threads interfere?"* There are no threads: a single asyncio loop, and no `await` inside the node. Claude added a side-by-side comparison of outbox and callbacks.
  - **Where the AI got it wrong:** to justify the outbox, Claude claimed callbacks would let the simulator deliver a message into the node mid-handler. My question made it re-check. The simulator always queues messages with a delay, so that can't happen, and the argument was withdrawn. The remaining argument (saving before sending is guaranteed by the structure) still holds.

## 2026-10-06 · Working method
- The work is split into small tasks in `PLAN.md`, each one commit with tests. Tasks can be handed to fresh agents, which read only
  `CLAUDE.md`, `DESIGN.md` and `PLAN.md` instead of the whole conversation. I review each commit, with review gates after the foundations,
  election, replication, correctness tests, Docker and the final state.

## 2026-10-07 · Review of T02 (messages, written by Claude Sonnet)
- **What I caught:** every serialization test either round-tripped (`to_dict` then `from_dict`) or compared `to_dict` with its own
  output. A mistake shared by both functions would pass, such as writing and reading `prevLogIndex` instead of `prev_log_index`, and
  the JSON format that other containers and the dashboard depend on wasn't pinned anywhere.
- **Fix:** a hand-written table of the wire format for every message type, with serialization and deserialization each tested
  against it on their own.
- **How it was verified:** I injected exactly that symmetric bug with a throwaway pytest plugin. All the old round-trip tests still
  passed, and the new serialization tests failed for both AppendEntries cases.
- **Also from review:** I asked what `frozen=True, slots=True` do and why dataclasses instead of plain classes, and asked for each
  message to document when it's used. Both are now in `raft/messages.py`'s docstrings.
