---
name: next-task
description: Pick up and finish the next task from PLAN.md in this repo, following its protocol, as one small, tested commit. Use when the user says "next task", "continue the plan", "do T07", or similar.
argument-hint: "[task ID, e.g. T07]"
---

# Next task

1. Read `DESIGN.md` and `PLAN.md` in full. `CLAUDE.md` is already loaded.
2. Choose the task:
   - If a task ID was given below, do that one.
   - Otherwise follow step 2 of "How to work a task" in PLAN.md.
   - If the next task is behind a gate with status `waiting`, do nothing. Tell the user which gate is ready for review and what to look at.
3. Run `git status`. If there are uncommitted changes you didn't make, stop and ask.
4. Work the task exactly as PLAN.md's protocol says: stay in scope, tests with the code, the full suite passes,
   update PLAN.md (and AI_NOTES.md if something notable happened), then make one commit in the `Txx: ...` format.
5. Finish with the report from protocol step 7: 10 lines or fewer.

Requested task: $ARGUMENTS
