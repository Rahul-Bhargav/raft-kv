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

## 2026-10-08 · Leader election (T06–T08, written by Claude Opus)
- **Judgment call on R4:** the election timer is reset only on the three events R4 lists. Two tempting extra resets were left
  out on purpose: on adopting a higher term (a candidate with an outdated log could then keep every timer from expiring, so
  nobody electable would ever stand), and when a leader steps down (its paused countdown resumes instead). Both have unit tests.
- **How it was verified:** besides the unit tests, each election rule was broken on purpose in a scratch copy (re-grant refused,
  log check comparing index first, `>` instead of `>=`, timer reset on a refused vote, majority off by one, votes not cleared
  between elections, and others) and the suite was rerun. Every mutation failed at least one test.
- **Spec gap:** `test_follower_keeps_leader_under_load` (R4) names a load, but there are no client writes before T11. For now the
  load is the network (half of all messages duplicated, delays up to 8 ticks, no drops), so any election after the first is a
  timer bug. T11 should add writes to it.
- **Test bounds are measured, not guessed:** each "elected within N ticks" bound is about twice the worst case seen over the
  test's seeds. The split-vote test narrows timeouts to [30, 35) and checks that most seeds really did split, so it can't pass
  without exercising the tie-break.

## 2026-10-08 · Log replication (T09, written by Claude Opus)
- **Judgment call, heartbeats carry entries:** every AppendEntries carries all entries from `next_index` on, so a lost one is
  retried by the next heartbeat with no extra bookkeeping. The cost is resending unconfirmed entries every 10 ticks, which is
  fine at this size.
- **A subtle stale reply:** a failure reply doesn't say which request it answers. A late copy of an early failure, arriving
  after the follower caught up, would push `next_index` back, and the leader would keep resending entries the follower has.
  DESIGN.md's "ignore a `match_index` lower than known" already covers it, because a failure carries `match_index` 0. The
  comment in `_on_append_entries_reply` explains why a failure after a success can only be stale.
- **R5, one step further:** a follower doesn't even rewrite entries it already has. Deleting and re-adding them ends with the
  same log, but a crash in between would lose entries it had already confirmed to the leader. A test with a storage that
  records every write checks that a duplicate AppendEntries writes nothing.
- **How it was verified:** 19 mutations of `node.py` in a scratch copy (calling `term_at` before the length check, truncating
  after `prev_log_index` on every message, confirming our last index instead of what the message covered, no stale-reply check,
  ignoring the backtracking hint, no floor at 1, no retry, and others). Every one failed at least one test.

## 2026-10-08 · Commit (T10, written by Claude Opus)
- **Finding that matters for the Figure 8 test (T13):** R7 appends the noop at once, and every AppendEntries runs to the end of
  the leader's log, so every reply in a leader's term already covers its noop. R6's term check then never changes the outcome
  on its own: removing it in a scratch copy failed only the unit test that feeds the leader hand-written replies. The check is
  still required (a leader that sent entries in smaller batches, or had no R7, would hit Figure 8), so it stays, with that test.
- **Judgment call:** a follower asserts it never deletes a committed entry. Correct Raft never does, so the assert can only
  fire on a bug, and then it stops loudly instead of letting nodes quietly disagree.
- **How it was verified:** 15 mutations of the commit code (no term check, not counting the leader's own copy, `>` for `>=`,
  the lowest index instead of the highest, no noop, a follower committing to its last index instead of the last new entry,
  no `max()`, committing on a failed check, `take_committed` not advancing, and others). Every one failed at least one test.
  The 19 T09 mutations were rerun on the T10 code and all still fail.

## 2026-10-08 · Replication through the cluster (T11, written by Claude Opus)
- **Spec gap from T08, closed with evidence:** `test_follower_keeps_leader_under_load` now runs about 470 client writes per
  seed on the same duplicating, reordering network. A follower that reset its election timer only on empty heartbeats passes
  the old network-only version of the test and fails the new one.
- **One of my tests was wrong, not the code:** the first version of the "overwritten write returns False" test (the 503 path)
  timed out instead. The old leader had been cut off right after its election, so its own noop never left it and the new
  leader's log was shorter. After the heal, the old leader's log was truncated below the lost write's index, and nothing was
  applied there until a later write. `write()` correctly returned False, but as a timeout (504). The test now commits a write
  first, so the new leader's noop lands on that index. The demo server (T19) will behave the same way: such a client waits for
  its timeout instead of getting a quick 503.
- **A weak test made stronger:** the first convergence test crashed a random node. Instrumenting it showed the victim was never
  the leader on any seed, so nothing interesting happened. It now crashes the leader in the middle of a write. Even then every
  write succeeds (the entry had reached a majority before the crash), and the comment says so, so nobody reads the test as
  covering failed writes.
- **How it was verified:** mutations of `write()` (no term check, returning right after propose, waiting on any node instead of
  the leader) each fail a test in `tests/test_replication.py`.

## 2026-10-10 · Failure scenarios (T12, written by Claude Opus)
- **Scripting instead of hoping:** a double vote needs a precise order of events (a voter crashes after granting, then a rival
  in the *same* term asks it), which random timeouts almost never produce. A small test helper, `SimCluster.time_out()`, makes
  one chosen node stand for election, and scripted tests use 1000-tick timeouts so nobody else does. No node logic changed.
- **Checked that the test has teeth:** in a scratch copy, making the simulator forget a node's vote on restart fails
  `test_restart_does_not_double_vote` on every seed. With its direct vote checks removed, the same mutation shows the real
  damage: n2 wins term 1 while n1 already leads it.
- **Judgment call:** "no majority" crashes the leader along with two followers. Pre-vote and check-quorum are out of scope, so
  a leader that survived in a minority would keep calling itself leader; it still couldn't commit, which is what
  `test_minority_leader_cannot_commit` shows.
- **Other mutations caught:** a majority of 2 instead of 3 fails all four partition/no-majority tests, and a leader that
  ignores higher terms, or adopts them without stepping down, fails `test_stale_leader_steps_down`.

## 2026-10-10 · Batch cap (T13a, written by Claude Opus)
- **Why it exists (user decision, option A):** with unlimited batches and the R7 noop, R6's term check never changed an outcome,
  so a Figure 8 test couldn't fail. `RaftConfig.max_entries_per_append` (default None, today's behavior) caps entries per
  AppendEntries, as real implementations do. All existing tests pass with no test code changed.
- **Judgment call:** the cap only limits message size. A success reply doesn't send the next batch; the next heartbeat or
  proposal does. The alternative (send at once on success) also fires with no cap and changes default traffic. In a scratch copy
  it pushed one T12 seed past its election bound.
- **R6 now has a unit test with real replies:** with a cap of 1 or 2, followers confirm 20 term-1 entries long before the new
  leader's noop, and the leader's commit index must stay at 0 until the noop is on 3 of 5. Removing R6's term check in a scratch
  copy now fails that test, not only the one with hand-written replies. Ignoring the cap or going one past it fails the cap tests.
