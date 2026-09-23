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
Stages run in order too, with one exception noted under
[Prerequisites](#prerequisites).

---

## Milestone index

| # | Milestone | Gate |
|---|---|---|
| 1.1 | Package skeleton, config and the lock | `pulse --help` runs; resolved paths print; a second `pulse` exits 3 |
| 1.2 | Channels bundle and counters | A spawned child's increments are visible in the parent |
| 1.3 | `PulseProcess` and its contract | A stub child exits through the stop flag and through EOF, cleaning up each time |
| 1.4 | TUI shell and the exit paths | Four exit paths return cleanly, terminal intact |
| 1.5 | Lock handover, bootstrap seam and the startup view | Second instance exits 3 before drawing; startup shows each step; a killed child stops the tree |
| 1.6 | Three children and synthetic load | Counters climb; the feed scrolls at one cadence across two rates |
| 1.7 | The shutdown view | `q` shows each child stopping, then the app exits on its own |
| 1.8 | Lifecycle matrix as tests | The nine criteria pass, repeatedly |
| 2.1 | Live tail proving ground | Real events counted from Jetstream v2 with filters applied |
| 2.2 | Segment writer | Sealed segments sort by seq; graceful exit leaves the spool clean |
| 2.3 | Cursor resumption and stale cursors | A restart resumes; a stale cursor reports a discontinuity and continues |
| 2.4 | Real counters and feed | Metrics show live throughput; synthetic mode survives as a flag |
| 3.1 | DuckLake bootstrap | A cold machine bootstraps once; a warm one skips ahead |
| 3.2 | Segment to transaction | Segments become queryable tables; a killed transform recovers |
| 3.3 | Deduplication | A segment replayed twice yields one row per event |
| 3.4 | Compaction | An hour of ingest leaves bounded file and snapshot counts |
| 4.1 | Streamlit inside a `Process` | The server exits through every stage 1 path, releasing its port |
| 4.2 | Reading beside a live writer | Dashboard queries land while transform commits |
| 4.3 | The URL in the TUI | The TUI shows a URL that opens |
| 5.1 | Data directory version marker | An older layout is detected and reported with an action |
| 5.2 | Uninstall on `u` | `u` returns the machine to its pre-install state, bar uv's cache |
| 5.3 | Bootstrap steps named | A cold first run reads as a sequence of named steps |
| 5.4 | Packaging and the landing page | `uvx pulse` reaches live data on a machine new to Pulse |

---

## Stage 1 — Walking skeleton

The process tree with trivial contents: the TUI and three children that
increment counters and emit log lines. Jetstream, DuckLake and Streamlit
arrive later.

The plumbing is the part whose failures present as silence, so it gets built
alone, while it is the only thing in the system that could be responsible.

### 1.1 — Package skeleton, config and the lock

- `pulse/cli.py` holding `main()`, a light `pulse/__init__.py`, and
  `pulse/__main__.py`
- The console script pointed at `pulse.cli:main`; both `__init__.py` and
  `cli.py` sit on the path every spawned child re-executes, so heavy imports
  live inside function bodies from the start
- `main()` returning an `int`, with the console script and `pulse/__main__.py`
  each wrapping it in `sys.exit()`, so criterion 7's exit code holds on both
  routes
- The frozen `Config` dataclass in `config.py`, `frozen=True, slots=True,
  kw_only=True`, holding one field to start: the data directory, defaulting
  through `default_factory` to `platformdirs` with `ensure_exists=False`, so
  `Config()` computes the path without creating anything
- No config instance at module level; `cli.main()` constructs the one `Config()`
- `lock.py`, holding everything about the instance lock: the data directory
  created with `mkdir(parents=True, exist_ok=True)` on the path passed in from
  the config, the lockfile opened and locked without blocking, and a held lock
  reported so `cli.main()` prints "already running" and returns 3. It imports
  only `os` and `fcntl` and keeps its state inside function calls, since every
  child loads it through `cli.py`.
- `cli.main()` in order: arguments parsed, `Config()` built, the lock taken
- A dev dependency group with pytest

`Config` grows a field in the milestone that first reads it, working towards
the finished set in TDD §4.7. The known ones will likely land around 1.5 (log
directory, join deadlines), 2.1 (collection filters), 2.2 (seal interval) and
3.1 (cache directory), and others will turn up along the way; each arrives when
something needs it. Validation in `__post_init__` comes with the first field
that can hold a bad value.

**Done when** `pulse --help` and `python -m pulse --help` both run from a clean
checkout, `Config()` prints its paths, a test's `Config()` built after
redirecting `HOME` resolves under `tmp_path`, and a second `pulse` started while
the first holds the lock exits 3.

`--help` and building a `Config` must leave the filesystem exactly as it was;
only taking the lock creates anything, and then only the data directory and its
lockfile. That check is the one worth automating: `ensure_exists=True` in the
config's factory is a one-word slip that creates directories whenever a config
is built, and the symptom shows up much later, as an uninstall that leaves
debris behind.

### 1.2 — Channels bundle and counters

- The `ctypes.Structure` with one section per child, so the single-writer rule
  is visible in the type
- Per-section fields: monotonic totals, a heartbeat from `time.monotonic()`, a
  state value, and the wall-clock shutdown times, stop seen and stopped
- The stop flag beside the sections, the one field any process may write, with
  the wall-clock time it was set
- The bounded log queue and the bounded feed queue
- The frozen `Channels` dataclass bundling all of it
- Creation and initialisation owned by the TUI

**Done when** a parent creates the bundle, spawns a child that increments and
heartbeats, and reads climbing totals and a fresh heartbeat. A `kill -9` of that
child leaves the totals at their last value, which is the property that keeps
counters meaningful across a death.

Worth proving here, while the boundary is the only thing under test: these
objects cross the process boundary at construction only, enforced by
`assert_spawning`. A test that tries to hand one to an already-started process
documents the boundary.

### 1.3 — `PulseProcess` and its contract

- One `get_context("spawn")` context as the source of every child and every
  `multiprocessing` object (TDD §4.1)
- `run()` establishing the contract in order: SIGINT and SIGHUP ignored, stream
  redirection into `user_log_dir` in append mode, logging configured,
  `logging.raiseExceptions` disabled, the watchdog, then `work()`, then cleanup
  in a `finally`
- One logging configuration function, shared with the TUI's `main()`: structlog
  over the standard library, with a `QueueHandler` on the root logger whose
  `ProcessorFormatter` renders each record to JSON
- The watchdog as one thread, waiting on the liveness pipe with a short timeout
  and reading the stop flag each time it wakes
- The uniform constructor: channels bundle, own liveness pipe end, a duplicate
  of the lock's descriptor, config
- One teardown routine reached by both the stop flag and `EOFError`

**Done when** a stub subclass — a counter loop, spawned straight from a test —
satisfies all six:

1. It exits when the stop flag is set.
2. It exits when the parent closes the pipe's write end.
3. Its stdout lands in a file under the log dir.
4. Its log records arrive in the queue as JSON carrying its process name, from
   a structlog call and a plain `logging` call alike.
5. It survives a SIGHUP sent to it.
6. Its cleanup runs in every one of those cases.

Three ordering details carry silent failures. Stream redirection comes before
the watchdog starts, so a traceback from the watchdog thread lands in the log
file; the reverse order puts it on the Textual canvas. Logging is configured
after redirection too, and nothing logs at import time, since unconfigured
structlog prints to stdout, which is the same canvas.

And the pipe's write end is held by exactly one process: the TUI closes its own
copy of each read end after spawning, since a second holder keeps the pipe open
and the child waits forever. That one presents as a child that waits
indefinitely, with everything else looking healthy — the stop flag exists as the
second path precisely for it.

### 1.4 — TUI shell and the exit paths

- The Textual app with both panes, the log pane bounded
- The log pane parsing each record's JSON and rendering it, with the TUI's own
  logging configured in `main()` onto the same queue
- Counter sampling on a timer, rates over a ten-second sliding window, heartbeat
  age shown alongside
- **`q`** bound to quit, Textual's **Ctrl+Q** kept; Ctrl+C raising a
  notification that names `q`
- One handler for SIGINT, SIGTERM and SIGHUP: the first two quit as `q` does,
  SIGHUP exits the app at once
- `try/finally` around `app.run()` in `main()`

**Done when** the TUI runs alone against a hand-filled counters structure and
hand-queued log records, renders both, and returns cleanly through `q`,
`kill -INT`, `kill -TERM`, and a closed terminal — leaving a usable terminal
behind each time. A wrecked terminal after exit is the visible form of a
`finally` that was skipped, so it doubles as the assertion.

### 1.5 — Lock handover, bootstrap seam and the startup view

Before `app.run()`:

- The lock from 1.1, now held until `main()` returns, with the app starting
  only once it is taken, so a refused instance draws no screen
- The channels bundle from 1.2 created after the lock, so the resource tracker
  starts only for an instance that holds it

Inside the app:

- The startup view, driven from a worker thread started in the app's
  `on_mount`: a bootstrap stub occupying the seam that 3.1 fills, then the log
  directory created with its marker file, then the three children spawned, each
  step shown as it completes, with each child's spawn time and first heartbeat
- The worker reporting each step as a posted message, leaving widgets to the
  event loop, and owned by the app, so it lives through every screen switch
- A duplicate of the locked descriptor passed to each child, so the lock
  outlives a killed TUI until its children finish
- One liveness pipe per child, with the TUI closing its own copy of each read
  end
- The channels bundle and config handed whole to each child
- The TUI draining both queues all session
- On EOF, each child cancelling its queue joins and moving its warnings and
  errors from the queue to its own stderr, its log file
- Warnings and errors drained after the pane is gone printed to the restored
  terminal
- Every join carrying a deadline, escalating to SIGTERM and then SIGKILL
- Each child's exit time and code recorded by the TUI from `Process.sentinel`
- A child that exits while the stop flag is clear setting off the stop, and
  `main()` returning a nonzero code

**Done when** the startup view shows the bootstrap stub and three sleeping
stubs coming up, a second `pulse` prints that Pulse is already running and
exits 3 without drawing a screen, and killing a stub stops the whole tree with
a reported code. A `kill -9` of the TUI, followed at once by a second `pulse`,
still exits 3 while a stub with a slow teardown winds down. An error that stub
logs during its orphan teardown lands in its own log file.

Signal deaths arrive as negative exit codes, keeping a child's code in the view
distinct from every code Pulse chooses. A test that kills a child and reads the
code back is cheap insurance on that.

### 1.6 — Three children and synthetic load

- The ingest stub generating events at a configurable rate
- Feed projection to DID, collection and snippet, done inside ingest
- The feed rate-limited on a time interval inside ingest
- Transform and dashboard stubs incrementing counters and logging
- All three honouring both shutdown paths through the shared routine

**Done when** four processes run, counters climb at the configured rate, the log
pane carries lifecycle lines, and the feed scrolls at the same cadence whether
the generator runs at 200/sec or 2000/sec. That equality is the whole point of
the generator: it is how you learn whether the sampling window and the display
pump are tuned sensibly, with the network uninvolved.

The generator keeps earning afterwards as a test fixture and as an offline demo
mode, so it is built to survive stage 2.

### 1.7 — The shutdown view

- `q`, SIGINT and SIGTERM opening the view, with the app running on until the
  children are down
- One row for each child: stop seen, stopped, exited, as wall-clock times with
  their offsets from the moment the flag was set
- Rows still waiting counting up their elapsed time; deadline escalation shown
  as terminated, then killed
- Everything driven from the sampling timer — the children's sentinels, the join
  deadlines, queue draining — so the event loop stays free
- The log pane kept visible beneath the rows
- The total shutdown time shown, a brief hold, then the app exits
- The view opening on its own when a child dies unexpectedly

**Done when** `q` shows every row filling in and the app exits on its own once
every child has gone. Three variations are part of the gate:

1. A stub that sleeps through its deadline shows the escalation in its row, and
   the app still exits.
2. A stub killed during shutdown leaves its "stopped" time blank while its
   "exited" time and signal code appear.
3. A stub killed mid-session opens the view without `q`.

The view is where the stage 1 plumbing becomes visible, which makes it the
debugging tool for 1.8: a flaky criterion shows which row stalled.

### 1.8 — Lifecycle matrix as tests

The nine criteria below each spawn a real tree with `HOME` pointed at
`tmp_path` and the XDG variables cleared, and assert that every pid in it is
gone afterwards, the resource tracker that `multiprocessing` starts under
`spawn` included:

1. Four processes run; counters climb; logs scroll
2. `q` exits every process in the tree, with every row of the shutdown view
   complete
3. `kill -INT` on the TUI exits every process in the tree
4. `kill -TERM` on the TUI exits every process in the tree
5. Closing the terminal exits every process in the tree through the graceful
   path, every child's cleanup included
6. `kill -9` on the TUI leaves every child to exit through EOF, none of them
   held by an unread queue
7. A second instance reports that Pulse is already running, exit code 3, with
   no screen drawn
8. A killed child is reported in the shutdown view, and the tree stops
9. A second instance started while a killed TUI's children wind down is
   refused with exit code 3

**Done when** all nine pass, and keep passing across repeated runs. Repetition
is the gate: teardown races are probabilistic, so a flake here is a finding
about the design.

Criterion 8 kills a child while it is busy, since the lock hazard that retired
the stop `Event` (TDD §5) sits on the busy path. Criterion 5 checks cleanup
alongside pids, since children killed by SIGHUP also leave no pids behind.

A `kill -9` of the TUI leaves the resource tracker to remove the queues' named
semaphores, and it says so on the terminal it inherited. That warning is
expected.

---

## Stage 2 — Ingest

Replace the synthetic generator with a real Jetstream v2 live tail.

### 2.1 — Live tail proving ground

- The `atproto` Jetstream client connected to a public v2 instance
- Collection filters applied server-side
- Messages counted and a handful logged, with the disk uninvolved

**Done when** a short run counts real events with filters visibly narrowing the
stream. Confirming the v2 endpoint and parameter names against the current API
belongs here, while this is the only moving part. The installed SDK (atproto
0.0.72) speaks v2 alone and dials `wss://jetstream.us-east.bsky.network/xrpc`
by default.

### 2.2 — Segment writer

- The open JSONL segment, named by the zero-padded seq of its first event
- Sealing every 5 seconds: fsync, then atomic rename into the spool
- Sealing on both shutdown paths, inside the shared teardown routine
- A leftover pre-rename file deleted at startup

**Done when** a run produces sealed segments whose names sort by seq, a graceful
exit leaves the spool holding sealed segments alone, and a `kill -9` leaves at
most one partial file under its pre-rename name, which the next start deletes.

The rename is what makes a segment either invisible or complete, so the
transformer always sees whole files, and the fsync ahead of it carries that
through power loss. A `kill -9` discards the open segment, up to 5 seconds of
events, which 2.3 re-fetches.

### 2.3 — Cursor resumption and stale cursors

- The resume cursor derived from sealed data: the newest segment's last seq, or
  the store's highest once transform empties the spool
- Reconnection within a session handled by the SDK, which drops the redelivered
  event itself
- The host recorded beside the data, and a change of host treated as a stale
  cursor
- `JetstreamCursorTooOldError` caught: the cursor discarded, the discontinuity
  reported to the TUI, the subscription resumed from the live tip

**Done when** a restart resumes from the newest sealed seq and re-fetches what a
`kill -9` discarded, and a hand-written stale cursor produces a discontinuity
report in the TUI followed by a session that carries on from the present.

Writing an old cursor by hand is the practical way to reach this path at
startup, since the natural route requires leaving Pulse closed for longer than
the lookback window. The same error arrives mid-session when the SDK reconnects
after a gap longer than the window, such as a laptop waking from sleep. A test
reaches that route by raising the error from a stubbed client.

### 2.4 — Real counters and feed

- Counters wired to real throughput
- The feed carrying projected real events
- The synthetic generator moved behind a flag, keeping it available

**Done when** the metrics pane shows live event rates, the feed carries real
posts, and the synthetic flag still produces the stage 1 behaviour.

---

## Stage 3 — Transform

### 3.1 — DuckLake bootstrap

- Both extensions, `ducklake` and `sqlite_scanner`, installed with
  `extension_directory` pinned into the cache directory
- `extension_directory` and `temp_directory` set on every connection, in every
  process
- The cache directory created, with its marker file for uninstall
- The SQLite catalog created
- The spool directory created, with `done/` and `quarantine/`
- All of it in the TUI's startup worker, under the lock, before any child exists

**Done when** a cold machine bootstraps in a single run, a second run finds
everything present and proceeds, and the extensions sit in Pulse's own cache
directory with `~/.duckdb` absent afterwards. Pinning the directory is what
keeps 5.2's uninstall complete, so a test that asserts the location earns its
place.

### 3.2 — Segment to transaction

- One sealed segment loaded per transaction
- The segment moved into `done/` on commit, and `done/` pruned by age
- Quarantine for a segment that fails to load

**Done when** segments land in queryable tables, a transform killed
mid-transaction reprocesses the surviving segment on restart, and a deliberately
malformed segment moves to quarantine while the pipeline carries on.

Crash recovery here is a directory listing: any segment in the spool itself is
unprocessed. That property holds only while commit-then-move stays in that
order, which the kill test is checking.

### 3.3 — Deduplication

- The idempotency key is Jetstream's seq, which identifies an **event**
- Deduplication applied once, at the DuckLake write, as an anti-join limited to
  the segment's seq range, since DuckLake rejects `PRIMARY KEY` and `UNIQUE`
  constraints

**Done when** a segment replayed twice produces one row per event, the first
event of a resumed session lands once, and a create followed later by an update
of the same record remains two rows sharing one `at://` URI.

The two keys answer different questions, which is why the gate checks both: the
`at://` URI identifies a *record* across its lifetime and serves as a grouping
dimension for queries, while the seq identifies the *event* and is exactly what
a replay repeats. [TDD §4.4](TDD.md) carries the full reasoning.

The seq-range limit keeps the anti-join's cost flat as the store grows, so a
write late in a long run is worth timing against one early in it.

### 3.4 — Compaction

- Maintenance in three steps on a schedule: `ducklake_merge_adjacent_files`,
  then `ducklake_expire_snapshots`, then `ducklake_cleanup_old_files` with a
  grace period longer than the slowest dashboard query
- A decision on which process owns that schedule, closing one of the TDD's open
  questions

**Done when** an hour of ingest leaves file and snapshot counts under stated
ceilings. Segment-sized commits produce roughly 720 of each an hour. Merging
alone raises the file count, because the originals stay on disk until their
snapshots expire and cleanup runs (TDD appendix). Both ceilings are needed to
show the whole sequence is running.

---

## Stage 4 — Dashboard

### 4.1 — Streamlit inside a `Process`

- The Streamlit bootstrap called inside `work()`
- Server options set explicitly: a loopback `server.address`, `server.headless`
  on, `browser.gatherUsageStats` off
- A watchdog thread translating EOF and the stop flag into a server shutdown
- Streamlit's own SIGINT, SIGTERM and SIGQUIT handlers, installed by its
  bootstrap over the child contract's SIGINT ignore, accounted for
- A placeholder page, so lifecycle is the only variable

**Done when** the dashboard serves its placeholder on loopback alone and exits
through every path in stage 1, releasing its port each time.

A held port after teardown is the tell here, and it is the exact failure that
running Streamlit as a `subprocess` would have made permanent. Checking the port
is free and catches it immediately.

### 4.2 — Reading beside a live writer

- A read path onto DuckLake while transform commits
- SQLite catalog concurrency understood and settled

**Done when** the dashboard queries the store over a sustained run with
transform committing throughout, and the catalog serves both for the duration.

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

### 5.2 — Uninstall on `u`

- **`u`** bound in the TUI, opening a confirmation dialog that names the three
  directories
- On confirmation, the shutdown view as for `q`, plus one cleanup step once
  every child has exited: the TUI's own lock descriptor closed, the lockfile
  opened afresh and locked without blocking, then each marked directory
  removed, with a row for each
- The fresh lock held through the removal, the lockfile removed last in the
  data directory, and the descriptor closed only once everything is gone
- Nothing removed when the fresh lock fails or a marker is missing
- An exit message listing what was removed, ending with `uv cache clean pulse`
- `u` available when bootstrap failed or refused the data directory

**Done when** `u` returns the machine to its pre-install state apart from uv's
cache, verified by listing all three directories. Four refusals are part of the
gate:

1. Cancelling the dialog removes nothing.
2. A process still holding a duplicate of the lock's descriptor makes `u`
   remove nothing.
3. A data directory without its lockfile survives.
4. `u` still works after bootstrap has refused a data directory.

A fifth check covers the race the lock order exists for: a second `pulse`
started while the removal is under way is refused until it finishes.

Pinning `extension_directory` and `temp_directory` inside Pulse's own cache
directory in 3.1 is what lets uninstall be complete.

### 5.3 — Bootstrap steps named

- The startup view from 1.5 carrying the real bootstrap phases from 3.1 and
  5.1, each reported as it completes

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
  pieces exist: the TUI sees deaths through `Process.sentinel`, and counters
  survive a child being killed, so a replacement continues the same totals.
- **Runtime reconfiguration** — Jetstream v2 accepts filter updates on a live
  subscription, so changing collections mid-session is available whenever
  a control channel justifies itself.
- **Platforms other than Linux** — macOS would need its `spawn`, clock and
  signal behaviour re-verified; native Windows would require replacing
  `fcntl.flock` and the fd-level readiness integration in ingest's event loop.
  WSL2 is Linux and needs neither.
