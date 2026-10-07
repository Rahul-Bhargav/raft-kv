# Backend Engineer Take-Home: Requirements

> Captured from `backend-take-home.pdf` on 2026-10-06. This is the source of truth for scope.
> Re-read it before making scope or design decisions.

- **Panel date:** _TBD_
- **Submit by:** _TBD_ (at least 24h before the panel)

## The task in one sentence

Build a toy consensus algorithm of my choice, use it to replicate state across a small cluster of
nodes, and write tests that exercise it, including the failure modes I think matter.

## Goal

Show understanding of distributed systems, specifically **how consensus is reached** and **how state
is replicated reliably**. Do it in a way that shows skill and judgment efficiently. The focus is on
that, not on volume.

## What to build

1. A toy implementation of a consensus algorithm. I pick which one.
2. Use it to replicate state across a small cluster of nodes.
3. Tests that put it through its paces, including the failure modes that matter.

## Ground rules

### I choose the approach
- The algorithm, the scope, and how nodes and the network between them are modeled are all my call.
- There is deliberately no reference implementation or scaffold. They want to see how I frame the problem.
- Expect to be asked about every trade-off.

### I write the consensus and replication logic myself
- **Allowed:** any language, its standard library, test frameworks, general-purpose tooling.
- **Not allowed:** building on an existing consensus or replication library (a Raft or Paxos
  package, an embedded coordination service like etcd or ZooKeeper, etc.). Those do the
  interesting part, and the interesting part is what's being evaluated.

### AI tools are encouraged and evaluated
- They evaluate *how* I use AI, not *whether* I do.
- I must be able to explain and defend every part of the code.
- Be ready to say what I delegated, how I verified it, where the tools got it wrong, and how I
  caught it.
- A short README note on how I worked with AI is welcome.

## Quality bar

- Open-ended. Not finishing is acceptable.
- Polish should reflect my style and sensibility. I should be able to walk through it and explain
  the reasoning, not only the functionality.
- **Testability beats claimed correctness.** A tested implementation with known bugs is better than
  a "working" one that can't easily be tested.
- **Small and done well beats big and half-done.** Volume doesn't count for anything.

## Time and delivery

- **Effort:** about one working day.
- **Deadline:** code delivered at least 24 hours before the panel.
- **Format:** final code, ideally as a GitHub repository.
- **Bonus:** commit at logical moments. The history shows how the work evolved.

## The panel (1 hour)

1. **First 10 to 15 min:** I walk through the design and trade-offs. Slides are optional; walking
   through the code is fine.
2. **The rest:** they run my tests, read my code, probe failure modes, dig into how I used AI, and
   **extend the design with me live**.

## What this implies (my interpretation, not quoted from the brief)

- [ ] Tests run with one command and need no setup surprises, because they will run them.
- [ ] Failure-mode tests are easy to find and clearly named.
- [ ] The README covers the design, trade-offs, deliberate scope cuts, how to run it, and how I used AI.
- [ ] The code is structured so it can be extended live (likely prompts: snapshots, membership
      changes, read paths, pre-vote).
- [ ] There are commits at each logical milestone.
- [ ] I've verified every AI-assisted piece and can explain it line by line.
- [ ] I keep notes on AI mistakes as they happen, for the README and the panel.
- [ ] The code is submitted at least 24h before the panel.
