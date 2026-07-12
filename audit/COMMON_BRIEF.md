# Leveled fork correctness audit — common contract (all layers)

CONTEXT: This is routine software QA of our own fork of leveled, an
open-source Erlang key-value store (Riak's backend). The goal is
finding functional regressions vs upstream and writing regression
tests. It is ordinary database reliability engineering — durability,
restart recovery, concurrency correctness — on code we own. Use
standard database-engineering terminology throughout your notes and
findings.

TARGET: /Users/dvse/projects/agents/leveled (branch codex/standard-mode-batchput,
HEAD as checked out). BASELINE: upstream fork point commit
7f08bba703c9c4f635da0f53e0f18bae0df59dbb (martinsumner develop-3.4).
The audit surface is `git diff 7f08bba..HEAD` (69 commits, ~27K lines).

## Hard rules
1. The leveled tree is STRICTLY READ-ONLY — no edits, no new files, no
   `rebar3` runs inside it, nothing that touches its _build. You may
   READ everything including docs/ and test/ and the prebuilt beams at
   `_build/default/lib/{leveled,lz4,zstd}/ebin`.
2. Every finding you claim MUST come with a RUNNABLE reproduction that
   lives in YOUR working directory (out of tree): an escript or a
   standalone eunit module that loads leveled via `-pa` flags pointing
   at the prebuilt ebins above (or compiles a COPY of sources into your
   own dir if you need instrumentation). No repro = not a finding;
   report it separately as "suspicion (unreproduced)" with reasoning.
3. Each repro: one file `repro_<slug>.escript` (or .erl + run script) +
   entry in FINDINGS.md stating: the invariant violated, exact
   expected vs actual output, severity (data-integrity > wrong-result >
   process-failure > liveness > perf), and the offending code cite
   (file:line at HEAD).
4. Contracts of record (read these first): docs/TARGET_API.md,
   docs/FTS.md, docs/NATIVE_CAS.md, docs/STANDARD_BATCHPUT.md in the
   leveled repo. Upstream behavior at 7f08bba is the semantic baseline
   for any path a stock caller can hit.
5. Focus dimensions: restart-durability windows (abrupt process
   termination between write phases), journal replay/restart
   equivalence, concurrency (concurrent callers + writer
   interleavings), encode/decode round-trips and length-field bounds,
   fallback paths actually falling back, resource leaks
   (ETS/persistent_term/process), and silent-wrong-answer paths.
6. Write your final report to FINDINGS.md in your working directory.
   Rank findings most-severe first. Include a coverage note: what you
   audited, what you did NOT get to.
