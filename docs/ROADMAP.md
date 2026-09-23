# Pulse — Build Order

**Last updated:** 2026-09-23

The design lives in [TDD.md](TDD.md). This document covers the order things get
built, the milestones each stage breaks into, and what "working" means at each
step.

---

## How a milestone is sized

A milestone is one sitting's work ending at a gate you can run. The gate is an
observation — a command, a signal, a directory listing — so that "done" is
something the machine shows you.

Milestones inside a stage run in order, each one standing on the one above it.
Stages run in order too, with one exception noted under [Prerequisites](#prerequisites).

---

## Milestone index

| # | Milestone | Gate |
|---|---|---|
| 1.1 | Package skeleton and config | `pulse --help` runs; resolved paths print; the filesystem stays as it was |
| 1.2 | Channels bundle and counters | A spawned child's increments are visible in the parent |
| 1.3 | `PulseProcess` and its contract | A stub child exits through the Event and through EOF, cleaning up each time |
| 1.4 | TUI shell and the exit paths | Four exit paths return cleanly, terminal intact |
| 1.5 | Hub, lock and bootstrap seam | Second instance exits 3; a killed child stops the tree |
| 1.6 | Three children and synthetic load | Counters climb; the feed scrolls at one cadence across two rates |
| 1.7 | Lifecycle matrix as tests | The nine criteria pass, repeatedly |
| 2.1 | Live tail proving ground | Real events counted from Jetstream v2 with filters applied |
| 2.2 | Segment writer | Sealed segments sort by cursor; graceful exit leaves the spool clean |
| 2.3 | Cursor persistence and reconnection | A restart resumes; a stale cursor reports a discontinuity and continues |
| 2.4 | Real counters and feed | Metrics show live throughput; synthetic mode survives as a flag |
| 3.1 | DuckLake bootstrap | A cold machine bootstraps once; a warm one skips ahead |
| 3.2 | Segment to transaction | Segments become queryable tables; a killed transform recovers |
| 3.3 | Deduplication | A segment replayed twice yields one row per event |
| 3.4 | Compaction | An hour of ingest leaves a bounded file count |
| 4.1 | Streamlit inside a `Process` | The server exits through every stage 1 path, releasing its port |
| 4.2 | Reading beside a live writer | Dashboard queries land while transform commits |
| 4.3 | The URL in the TUI | The TUI shows a URL that opens |
| 5.1 | Data directory version marker | An older layout is detected and reported with an action |
| 5.2 | Teardown | Teardown returns the machine to its pre-install state |
| 5.3 | Bootstrap steps named | A cold first run reads as a sequence of named steps |
| 5.4 | Packaging and the landing page | `uvx pulse` reaches live data on a machine new to Pulse |

---

## Stage 1 — Walking skeleton

The process tree with trivial contents: the TUI, the hub, and three children
that increment counters and emit log lines. Jetstream, DuckLake and Streamlit
arrive later.

The plumbing is the part whose failures present as silence, so it gets built
alone, while it is the only thing in the system that could be responsible.

### 1.1 — Package skeleton and config

- `pulse/cli.py` holding `main()`, a light `pulse/__init__.py`, `pulse/__main__.py`
- The console script pointed at `pulse.cli:main`; both `__init__.py` and
  `cli.py` sit on the path every spawned child re-executes, so heavy imports
  live inside function bodies from the start
- `main()` returning an `int`, with the console script and `pulse/__main__.py`
  each wrapping it in `sys.exit()`, so criterion 8's exit code holds on both
  routes
- The frozen config dataclass: `frozen=True, slots=True, kw_only=True`, tuple
  fields for collection and DID filters
- `__post_init__` normalising through `object.__setattr__`, then validating
- Resolution from argv, environment and `platformdirs` with `ensure_exists=False`
- A dev dependency group with pytest

**Done when** `pulse --help` and `python -m pulse --help` both run from a clean
checkout, a config resolved from argv and environment prints its paths, and the
filesystem is left exactly as it was. That last check is the one worth automating: `ensure_exists=True` is a
one-character slip that creates directories at import time, and the symptom
shows up much later, as a teardown that leaves debris behind.

### 1.2 — Channels bundle and counters

- The `ctypes.Structure` with one section per process, so the single-writer rule
  is visible in the type
- Per-section fields: monotonic totals, a heartbeat from
  `time.clock_gettime(time.CLOCK_MONOTONIC)`, a state value
- The bounded log queue, the bounded feed queue, the stop `Event`
- The frozen `Channels` dataclass bundling all of it
- Creation and initialisation owned by the TUI

**Done when** a parent creates the bundle, spawns a child that increments and
heartbeats, and reads climbing totals and a fresh heartbeat. A `kill -9` of that
child leaves the totals at their last value, which is the property that keeps
counters meaningful across a death.

Worth proving here, while the boundary is the only thing under test: these
objects cross the process boundary at construction only, enforced by `assert_spawning`. A test
that tries to hand one to an already-started process documents the boundary.

### 1.3 — `PulseProcess` and its contract

- `get_context("spawn")` set explicitly
- `run()` establishing the contract in order: stream redirection into
  `user_log_dir`, `QueueHandler` onto the root logger, `logging.raiseExceptions`
  disabled, the EOF watchdog, then `work()`, then cleanup in a `finally`
- The uniform constructor: channels bundle, own liveness pipe end, config
- One teardown routine reached by both the stop Event and `EOFError`

**Done when** a stub subclass — a counter loop, spawned straight from a test —
satisfies all five: it exits when the Event is set, it exits when the parent
closes the pipe's write end, its stdout lands in a file under the log dir, its
log records arrive in the queue, and its cleanup runs in every one of those
cases.

Two ordering details carry silent failures. Stream redirection comes before the
watchdog starts, so a traceback from the watchdog thread lands in the log
file; the reverse order puts it on the Textual canvas. And the pipe's write end is held by exactly one
process: the hub closes its own copy of each read end after spawning, since a
second holder keeps the pipe open and the child waits forever. That second one
presents as a child that waits indefinitely, with everything else looking
healthy — the stop Event exists as the second path precisely for it.

### 1.4 — TUI shell and the exit paths

- The Textual app with both panes, the log pane bounded
- Counter sampling on a timer, rates over a ten-second sliding window, heartbeat
  age shown alongside
- **Ctrl+Q** bound to quit; Ctrl+C raising a notification that names Ctrl+Q
- Handlers installed for SIGINT, SIGTERM and SIGHUP
- `try/finally` around `app.run()` in `main()`

**Done when** the TUI runs alone against a hand-filled counters structure and
returns cleanly through Ctrl+Q, `kill -INT`, `kill -TERM`, and a closed
terminal — leaving a usable terminal behind each time. A wrecked terminal after
exit is the visible form of a `finally` that was skipped, so it doubles as the
assertion.

### 1.5 — Hub, lock and bootstrap seam

- Data directory creation
- The exclusive `flock` held for the hub's whole lifetime, with exit code 3 on
  conflict, read back by the TUI from `Process.exitcode`
- A bootstrap stub occupying the seam that 3.1 fills
- One liveness pipe per child, with the hub closing its own copy of each read end
- The channels bundle and config relayed whole to each child
- Children joined; a child death setting the Event, joining the survivors, and
  exiting with a code the TUI reports

**Done when** the TUI spawns the hub, the hub spawns three sleeping stubs while
holding the lock, a second `pulse` reports that Pulse is already running and
exits 3, and killing a stub stops the whole tree with a reported code.

Signal deaths arrive as negative exit codes, which is what keeps 3 unambiguous;
a test that kills the hub and reads the code back is cheap insurance on that.

### 1.6 — Three children and synthetic load

- The ingest stub generating events at a configurable rate
- Feed projection to handle, collection and snippet, done inside ingest
- The feed rate-limited on a time interval inside ingest
- Transform and dashboard stubs incrementing counters and logging
- All three honouring both shutdown paths through the shared routine

**Done when** five processes run, counters climb at the configured rate, the log
pane carries lifecycle lines, and the feed scrolls at the same cadence whether
the generator runs at 200/sec or 2000/sec. That equality is the whole point of
the generator: it is how you learn whether the sampling window and the display
pump are tuned sensibly, with the network uninvolved.

The generator keeps earning afterwards as a test fixture and as an offline demo
mode, so it is built to survive stage 2.

### 1.7 — Lifecycle matrix as tests

The nine criteria below, each spawning a real tree against a `tmp_path` data
directory and asserting that every pid in it is gone afterwards:

1. Five processes run; counters climb; logs scroll
2. Ctrl+Q exits every process in the tree
3. `kill -INT` on the TUI exits every process in the tree
4. `kill -TERM` on the TUI exits every process in the tree
5. Closing the terminal exits every process in the tree
6. `kill -9` on the hub leaves children to exit through EOF
7. `kill -9` on the TUI cascades through hub and children
8. A second instance reports that Pulse is already running, exit code 3
9. A killed child is reported by the hub, which stops the tree

**Done when** all nine pass, and keep passing across repeated runs. Repetition
is the gate: teardown races are probabilistic, so a flake here is a finding
about the design.

---

## Stage 2 — Ingest

Replace the synthetic generator with a real Jetstream v2 live tail.

### 2.1 — Live tail proving ground

- The `atproto` Jetstream client connected to a public v2 instance
- Collection filters applied server-side
- Messages counted and a handful logged, with the disk uninvolved

**Done when** a short run counts real events with filters visibly narrowing the
stream. Confirming the v2 endpoint and parameter names against the current API
belongs here, while this is the only moving part.

### 2.2 — Segment writer

- The open JSONL segment, named by its first cursor value
- Sealing every 5 seconds by atomic rename into the spool
- Sealing on both shutdown paths, inside the shared teardown routine

**Done when** a run produces sealed segments whose names sort by cursor, a
graceful exit leaves the spool holding sealed segments alone, and a `kill -9`
leaves at most one partial file under its pre-rename name, which a reader
ignores.

The rename is what makes a segment either invisible or complete, so the
transformer always sees whole files. The cost of that guarantee is visible here:
a `kill -9` discards up to 5 seconds of events, which is the deliberate trade.

### 2.3 — Cursor persistence and reconnection

- The cursor persisted and resumed on restart
- Reconnection from the stored cursor, handled inside ingest by the SDK
- Lookback discontinuity detected and reported to the TUI

**Done when** a restart resumes from the stored cursor, and a hand-written stale
cursor produces a discontinuity report in the TUI followed by a session that
carries on from the present.

Writing an old cursor by hand is the practical way to reach this path, since the
natural route requires leaving Pulse closed for longer than the lookback window.

### 2.4 — Real counters and feed

- Counters wired to real throughput
- The feed carrying projected real events
- The synthetic generator moved behind a flag, keeping it available

**Done when** the metrics pane shows live event rates, the feed carries real
posts, and the synthetic flag still produces the stage 1 behaviour.

---

## Stage 3 — Transform

### 3.1 — DuckLake bootstrap

- Extension install with `extension_directory` pinned into the cache directory
- The SQLite catalog created
- The spool directory created
- All of it inside the hub, under the lock, before any child exists

**Done when** a cold machine bootstraps in a single run, a second run finds
everything present and proceeds, and the extension sits in Pulse's own cache
directory. Pinning the directory is what keeps 5.2's teardown complete, so a
test that asserts the location earns its place.

### 3.2 — Segment to transaction

- One sealed segment loaded per transaction
- Deletion on commit, after a short trailing retention
- Quarantine for a segment that fails to load

**Done when** segments land in queryable tables, a transform killed
mid-transaction reprocesses the surviving segment on restart, and a deliberately
malformed segment moves to quarantine while the pipeline carries on.

Crash recovery here is a directory listing: any segment present is unprocessed.
That property holds only while commit-then-delete stays in that order, which the
kill test is checking.

### 3.3 — Deduplication

- The idempotency key is Jetstream's monotonic per-message cursor, which
  identifies an **event**
- Deduplication applied once, at the DuckLake write

**Done when** a segment replayed twice produces one row per event, and a create
followed later by an update of the same record remains two rows sharing one
`at://` URI.

The two keys answer different questions, which is why the gate checks both: the
`at://` URI identifies a *record* across its lifetime and serves as a grouping
dimension for queries, while the cursor identifies the *event* and is exactly
what a reconnect repeats. [TDD §4.4](TDD.md) carries the full reasoning.

### 3.4 — Compaction

- `ducklake_merge_adjacent_files` running on a schedule, as its own maintenance
  step
- A decision on which process owns that schedule, closing one of the TDD's open
  questions

**Done when** an hour of ingest leaves a file count under a stated ceiling.
Segment-sized commits produce roughly 720 files an hour, so the ceiling is the
measurement that tells you compaction is running.

---

## Stage 4 — Dashboard

### 4.1 — Streamlit inside a `Process`

- The Streamlit bootstrap called inside `work()`
- A watchdog thread translating EOF and the stop Event into a server shutdown
- A placeholder page, so lifecycle is the only variable

**Done when** the dashboard serves its placeholder and exits through every path
in stage 1, releasing its port each time.

A held port after teardown is the tell here, and it is the exact failure that
running Streamlit as a `subprocess` would have made permanent. Checking the port
is free and catches it immediately.

### 4.2 — Reading beside a live writer

- A read path onto DuckLake while transform commits
- SQLite catalog concurrency understood and settled

**Done when** the dashboard queries the store over a sustained run with transform
committing throughout, and the catalog serves both for the duration.

### 4.3 — The URL in the TUI

- Host and port surfaced once the server is listening

**Done when** the TUI shows a URL that opens in a browser.

---

## Stage 5 — Finish

### 5.1 — Data directory version marker

- A layout version written into the data directory
- The read path on startup, and the action reported when versions differ

**Done when** a directory written under an older layout is detected and reported
with a clear next step. This closes a TDD open question.

### 5.2 — Teardown

- A documented command removing the data directory and the cache directory in
  full
- Safe to run while the lock is free

**Done when** teardown returns the machine to its pre-install state, verified by
listing both directories. This closes a TDD open question, and it is the reason
`extension_directory` was pinned inside Pulse's own cache directory in 3.1.

### 5.3 — Bootstrap steps named

- Each bootstrap phase reported to the TUI as it completes

**Done when** a cold first run reads as a sequence of named steps. This matters
most on the run where it takes longest, which is the first one an operator ever
sees.

### 5.4 — Packaging and the landing page

- README serving as the PyPI landing page
- Packaging metadata, licence, classifiers

**Done when** `uvx pulse` on a machine new to Pulse reaches live data on screen.

---

## Prerequisites

The analytical data model — which collections are subscribed to, the table
shapes, what the dashboard presents — is designed separately, and two milestones
consume it:

- **2.1** needs the collection list, since the filters are what it applies
- **3.2** needs the table shapes, since the loader writes into them

A provisional collection list is enough to reach 2.4, so the design can run
alongside stage 2 as long as it lands before 3.2.

---

## Deferred

Held open deliberately, listed so the reasoning survives:

- **Restart supervision** — revisit once real failure rates are observed. The
  pieces exist: the hub sees deaths through `Process.sentinel`, and counters
  survive a child being killed, so a replacement continues the same totals.
- **Runtime reconfiguration** — Jetstream v2 accepts filter updates on a live
  subscription, so changing collections mid-session is available whenever
  a control channel justifies itself.
- **Windows support** — would require replacing `fcntl.flock` and the fd-level
  readiness integration in ingest's event loop.
