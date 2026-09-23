# Pulse — Technical Design

**Status:** design settled, implementation pending
**Last updated:** 2026-09-23

---

## 1. Context and scope

Bluesky runs on the AT Protocol, whose repositories emit a continuous stream of
changes: posts, likes, follows, profile edits, account events. **Jetstream** is
Bluesky's public rendering of that firehose as plain JSON, filtered server-side,
delivered over a websocket. Two public v2 instances serve the full network, and
live-tail consumption is unauthenticated.

Pulse observes that stream, lands it on disk, transforms it into an analytical
store, and presents it. It runs as a local single-user tool, installed and
launched with one `uvx` command, and holds its lifetime to a single session: it
starts when the operator opens it and stops when they close it.

This document covers the process architecture, the channels between processes,
the lifecycle contract, and the data path. The analytical data model — which
collections are subscribed to, the table shapes, what the dashboard presents —
is designed separately.

---

## 2. Goals and non-goals

### Goals

- **One command reaches live data.** `uvx pulse` takes an operator from an empty
  machine to events on screen, resolving its own dependencies and storage.
- **The pipeline is legible while it runs.** Each stage reports throughput and
  liveness, so the operator can see which stage is doing what.
- **Ingested data lands somewhere queryable.** The analytical store is a real
  lakehouse that outlives the session and answers SQL.
- **The process tree starts and stops cleanly**, through every exit path
  available to an operator, including abrupt ones.
- **Ordinary laptop resources suffice.** Bounded memory, bounded disk, bounded
  CPU at firehose rates.

### Non-goals

These are outcomes Pulse could reasonably pursue and deliberately holds outside
its scope.

- **Historical backfill.** Pulse observes from the moment it starts. Reaching
  further back means the metered archive, which places a credential and a
  metered bill between a new operator and a running pipeline.
- **Completeness across sessions.** A cursor older than Jetstream's lookback
  window resumes at the present. Pulse reports the discontinuity and continues.
- **Cryptographic verification.** Jetstream delivers decoded JSON, carrying
  repository signatures and MST proofs alone in the raw firehose. Verified
  ingestion would mean `FirehoseSubscribeReposClient` and CAR decoding.
- **Windows support.** The design uses `fcntl.flock` and fd-level readiness in
  an asyncio loop, both of which are POSIX facilities.
- **Multi-user or server operation.** One operator, one machine, one instance
  per data directory.
- **Supervised restart of failed children.** A child exiting reports and stops
  the tree.
- **Runtime reconfiguration.** Filters and intervals are fixed for a session.

---

## 3. Overview

### System context

```
Bluesky network (AT Protocol repositories)
        │
        ▼
Jetstream v2  ── public websocket, JSON, unauthenticated live tail
        │
        ▼
┌──────────────────────── Pulse (one machine, one session) ───────────────────┐
│                                                                             │
│   ingest ──▶ JSONL spool ──▶ transform ──▶ DuckLake ──▶ dashboard           │
│      │        (5s segments)      │         (Parquet +       (Streamlit)     │
│      │                           │          SQLite catalog)      │          │
│      └───────── counters, logs ──┴──────────────────────────────┐│          │
│                                                                 ▼▼          │
│                                                          TUI (Textual)      │
└─────────────────────────────────────────────────────────────────┬───────────┘
                                                                  ▼
                                                              operator
```

Data moves left to right through files. Observability moves upward through
shared memory and queues. The operator interacts with the TUI, which owns the
lifetime of everything below it.

### Process tree

```
TUI (Textual)
└── hub
    ├── ingest      Jetstream v2 live tail → JSONL segments
    ├── transform   sealed segment → DuckLake
    └── dashboard   Streamlit server reading DuckLake
```

| Process | Owns |
|---|---|
| **TUI** | The view, the lifetime, and the channel objects. Spawns the hub, samples counters, renders. |
| **hub** | The data directory: the instance lock, the bootstrap, and the three children. |
| **ingest** | One Jetstream live-tail connection and the open JSONL segment. |
| **transform** | Loading sealed segments into DuckLake and maintaining the catalog. |
| **dashboard** | The Streamlit server. |

---

## 4. Detailed design

### 4.1 Process model

All processes start with the `spawn` start method, set explicitly through
`get_context("spawn")` at each level. The method propagates through spawn
preparation data; setting it explicitly keeps each level correct when started
directly, such as from a test.

Every process is a `PulseProcess` — a `multiprocessing.Process` subclass whose
`run()` establishes the cross-cutting contract (logging, stream redirection, the
parent watchdog, cleanup in a `finally`) and then calls the subclass's `work()`.
Subclasses implement `work()` alone, which also keeps the process boundary thin
enough to test `work()` directly.

Each child receives a uniform constructor: the channels bundle, its own liveness
pipe end, and the config.

### 4.2 Channels

Four channels, separated by frequency and by behaviour under pressure.

| Channel | Kind | Direction | Carries | Under pressure |
|---|---|---|---|---|
| **Counters** | `ctypes.Structure` in shared memory, lock-free | child writes, TUI reads | totals, heartbeat, state | Sampled; each writer proceeds at its own pace |
| **Stop** | `Event` | any process → all | The graceful shutdown signal | Broadcast; one `set()` reaches everyone |
| **Liveness** | `Pipe(duplex=False)` | parent holds write end | Its closure is the message | Silent by design |
| **Logs** | bounded `Queue` | all → TUI | `LogRecord`s via `QueueHandler` | Drops on full, through `put_nowait` |
| **Feed** | bounded `Queue` | ingest → TUI | Sampled event projections | Drops on full |

#### Counters carry the hot path

Counters live in a `ctypes.Structure` with one section per process, so the
single-writer rule is visible in the type. The TUI samples at its own rate;
sampling decouples the two paces, letting ingest write as fast as events arrive
whatever speed the TUI reads at. This protects the cursor, since ingest falling
behind its websocket risks a gap past the lookback window.

Children increment monotonic totals and write a heartbeat from
`time.clock_gettime(time.CLOCK_MONOTONIC)`, which is system-wide on POSIX and
therefore comparable across processes. The TUI derives rates from successive
samples over a ten-second sliding window, and reads the heartbeat to distinguish
an idle child from a stalled one — a distinction a flat rate leaves ambiguous.

Shared memory belongs to the TUI, so counters survive a child dying and a
replacement continues the same totals. Children increment alone; initialisation
belongs to the TUI at creation.

#### The liveness pipe

The hub creates one pipe per child, passes the read end to the child, keeps the
write end, and closes its own copy of the read end. Nothing is ever written to
it; the child's `EOFError` on parent death is the entire protocol.

**The write end is held by exactly one process — the parent whose death it
represents.** Another living process holding it keeps the pipe open and the
child waits indefinitely. This failure presents as an absence, which is why the
stop Event doubles as a second path (§4.3).

#### Two queues, separately bounded

Logs and the sampled feed hold separate capacity, so sampled post text, which is
decorative, leaves error records, which are load-bearing, untouched.

The feed is rate-limited **in ingest**, on a time interval, so the scroll cadence
stays constant whether the firehose runs at 200/sec or 2000/sec. Ingest projects
each sample to a handle, a collection and a snippet before it crosses the
boundary.

#### Crossing the boundary

`Event`, `Queue`, `Value` and `Array` cross between processes at construction
only, enforced by `assert_spawning`, travelling as `Process` constructor
arguments and relayed through the hub for the children one level down. A
`Connection` may additionally be handed over after `start()` by fd-passing.

The TUI bundles the shared objects into one frozen `Channels` dataclass and the
hub relays it whole.

### 4.3 Lifecycle

#### Startup

1. TUI resolves config and creates the channels bundle
2. TUI spawns the hub
3. Hub creates the data directory
4. Hub acquires the instance lock
5. Hub bootstraps under the lock: extension, catalog, spool directory
6. Hub creates one liveness pipe per child and spawns the three children
7. TUI samples counters and renders

Bootstrap happens once, in one process, under the lock, before any child exists.
Children open what already exists.

#### Graceful shutdown

The TUI sets the stop `Event`. Children finish their current unit of work and
exit — ingest seals its open segment, transform completes its in-flight
transaction. The hub joins its children, then exits. The TUI joins the hub and
returns, releasing the lock as the hub's file descriptors close.

#### Orphan teardown

A parent dying before setting the Event leaves its children with `EOFError` on
their liveness pipes, and they run the same cleanup. Both paths converge on one
routine, so the path exercised least shares an implementation with the path
exercised most.

EOF propagates as each parent dies, so a process winding its children down
*while staying alive to supervise them* sets the Event. When the hub sees
`EOFError` from the TUI it sets the stop Event, joins its children, and exits —
giving each child the graceful path and leaving the hub alive long enough to
know they finished.

A leaf child seeing `EOFError` sets the stop Event and tears itself down. Its
siblings hold their own signals, and the Event covers the case where a sibling's
pipe stays silent: a liveness pipe whose write end is held by another living
process reads as open indefinitely. The cost is one `set()` per teardown, and it
can fire only once the parent is already gone.

#### Exit paths

| Trigger | Path |
|---|---|
| **Ctrl+Q** | `app.run()` returns → `finally` → graceful |
| **`kill -INT`** | `KeyboardInterrupt` reaches `finally` → graceful |
| **`kill -TERM`** | Graceful, through an installed handler |
| **Terminal closed (`SIGHUP`)** | Graceful, through an installed handler |
| **`kill -9`** | Children tear down through EOF |
| **A child exits unexpectedly** | Hub sets the Event, joins the survivors, exits with a code the TUI reports |

Teardown belongs in a `try/finally` around `app.run()` in `main()`, covering an
unhandled exception in the app as well as an ordinary quit.

Textual binds **Ctrl+Q** to quit. Ctrl+C displays a notification naming that
binding, since Ctrl+C means copy inside `Input` and `TextArea` widgets. Textual's
Linux driver leaves SIGINT, SIGTERM and SIGHUP to the application.

#### Single instance

The hub holds an exclusive `flock` on a lockfile inside the data directory for
its whole lifetime. A second hub exits with a distinct code, which the TUI reads
from `Process.exitcode` to separate "already running" from a crash — signal
deaths arrive negative, so the code space stays unambiguous.

The lock lives in the data directory so that it guards the resource: two
instances pointed at different data directories run side by side. `flock`
releases on process death of any kind, so a stale lock resolves itself.

### 4.4 Data path

#### Ingest

One Jetstream v2 live-tail connection through the `atproto` SDK,
unauthenticated. Events append to an open JSONL segment, **sealed every 5
seconds by atomic rename**. Segments are named by their first cursor value, so
ordering holds across restarts.

Sealing by rename means a segment is either invisible or complete. The
transformer sees whole files.

#### Transform

One sealed segment maps to **one transaction**. On commit the segment is
deleted, so any segment present is unprocessed and crash recovery is a directory
listing. A segment that fails to load moves to a quarantine directory, and
successful loads delete after a short trailing retention, leaving a window in
which a transform bug stays recoverable.

#### An observation log

Pulse records the events that crossed the wire during the sessions it ran.
`create`, `update` and `delete` operations all land as rows: a delete is an
event that was observed, and the create it refers to remains an event that was
also observed.

This follows from live-tail ingestion. Pulse begins at the present, so its
earliest row is the first event it observed, and a delete usually refers to a
record created while Pulse was elsewhere. The analytical value lives in the stream itself — rates,
distributions, and what moved through the network during the observed window.

#### Deduplication

Jetstream delivers at-least-once, and a crash between commit and delete
reprocesses a segment. Both redeliver *the same event*, so the idempotency key
identifies an event rather than a record: Jetstream's monotonic per-message
cursor serves directly, and it is what a reconnect repeats.

A record's `at://` URI (DID, collection, rkey) identifies a record across its
lifetime, so a create and a later update of one post share it while remaining
two distinct events. The URI is a grouping dimension for queries; event identity
is what deduplication keys on.

Deduplication happens once, at the DuckLake write, so the layers above stay
simple.

#### DuckLake

SQLite catalog, Parquet data, `extension_directory` pinned inside Pulse's cache
directory. Segment-sized commits produce roughly 720 files an hour, so
`ducklake_merge_adjacent_files` runs on a schedule as its own maintenance step.

### 4.5 Storage layout

Paths come from `platformdirs` with `ensure_exists=False`, keeping directory
creation an explicit step at a chosen moment.

| Contents | Location |
|---|---|
| DuckLake catalog, Parquet, JSONL spool, lockfile | `user_data_dir` |
| DuckDB extension | `user_cache_dir` |
| Child stdout/stderr | `user_log_dir` |

The JSONL spool sits under `user_data_dir` although its contents are
short-lived: with live-tail ingestion and delete-after-commit, a pending segment
holds the only copy of those events, and cache directories are cleared by users
and tools as a matter of routine.

### 4.6 Module layout

Under `spawn`, two import paths execute in every child:

1. The console script runs `from pulse.cli import main` at module level, outside
   its `__main__` guard, as `runpy` re-executes it under `__mp_main__`
2. Unpickling a `PulseProcess` subclass imports the module defining it

Both are load-bearing, because `import atproto` alone costs 1.04s and a shared
module reaching heavy dependencies multiplies that across every process in the
tree.

- `pulse/__init__.py` and the entry module hold light imports alone
- Each `PulseProcess` subclass lives in its own module, with heavy imports inside
  `run()`

The entry module shares `__init__.py`'s universal reach, since the console
script's module-level import runs in every child. A module-level
`from pulse.tui import ...` there would reach the hub, and through it the three
child modules, seating every heavy dependency in every process; that import
belongs in the body of `main()`.

`pulse/__main__.py` sits outside that path, since `multiprocessing.spawn`
returns early for a module whose name ends in `.__main__`, leaving a child under
`python -m pulse` to reconstruct itself through unpickling alone. Both entry
points carry the same two lines — `from pulse.cli import main`, then
`sys.exit(main())` — and `main()` returns an `int`, which keeps the
single-instance exit code identical across the two routes and leaves `main()`
callable from a test.

Children inherit the TUI's stdout and stderr, so a stray write lands on the
Textual canvas. Each child redirects both streams in `PulseProcess.run()`, and
`logging.raiseExceptions` is disabled so a full log queue stays quiet.

### 4.7 Configuration

Config resolves once, at the edge, in the TUI: argv, environment and
`platformdirs` collapse into a frozen dataclass of plain values travelling
alongside the channels bundle.

`frozen=True, slots=True, kw_only=True`. Collection and DID filters are tuples,
since `frozen` guards rebinding alone and a list field stays mutable.
`__post_init__` normalises through `object.__setattr__` and validates, catching
malformed input where input enters.

Passing config keeps every process operating on the directory the hub locked,
and lets a test construct a config pointing at `tmp_path` and hand it straight
in.

---

## 5. Alternatives considered

### Ingestion

**The metered archive.** Jetstream's `snapshot()` and `replay()` reach
arbitrarily far back and transition seamlessly into live tail — the natural
primitive for a backfill. Rejected because the archive requires an API token
from bsky.network and bills per byte downloaded. That places account creation
and a metered bill between a new operator and a running pipeline, against the
first goal. Live tail costs nothing and starts immediately.

**The raw firehose with CAR decoding.** Carries signatures and MST proofs, so
records are verifiable. Rejected because verification falls outside the goals,
and DAG-CBOR decoding adds dependency weight and CPU per event.

### Process model

**`fork` or `forkserver`.** `fork` is faster and was Python 3.13's POSIX default.
Rejected because Textual runs an asyncio loop with threads, and forking a
multithreaded process is documented as problematic — `os.fork()` raises a
`DeprecationWarning` from 3.12 in that situation. Python 3.14 moved the POSIX
default to `forkserver`, so an explicit `spawn` context also keeps behaviour
stable across interpreter versions.

**Daemonic children.** The obvious way to have children die with their parent.
Rejected on two counts: daemonic children are terminated only when the parent
exits *normally*, leaving them orphaned by a crash or a kill; and termination
offers no cleanup window, while ingest must seal its open segment. A daemonic
process is also forbidden from creating children, which rules it out for the hub
regardless.

**`PR_SET_PDEATHSIG` or polling `os.getppid()`.** Both detect orphaning without
a pipe. `PR_SET_PDEATHSIG` is Linux-only and fires on the death of the *thread*
that spawned. `getppid()` polling carries a startup race: a parent dying before
the child records its original ppid leaves the child comparing against the
reaper forever. File descriptor state is definitive where a sampled integer is
approximate, so the liveness pipe won.

**Supervised restart.** Rejected because the transient failure it would address —
a dropped websocket — is already handled inside ingest, where the SDK reconnects
from the stored cursor. A dead ingest *process* therefore signals a bug, which
restarting conceals. The three children also want different policies: ingest
restarts cheaply but loses lookback window while down, transform restarts safely
because DuckLake rolls back and the segment survives until commit, and the
dashboard is cosmetic. Three policies wearing one name suggests the feature is
premature.

### Inter-process communication

**One duplex pipe carrying everything.** The original design, with status,
control and logs on a single connection per child. Rejected because `send()`
blocks once the OS buffer fills, and the resulting chain — slow TUI, hub stops
draining, ingest blocks, websocket falls behind, cursor passes the lookback
window — converts a rendering delay into permanent data loss. Moving counters to
shared memory removes the channel the chain depends on.

**Locked shared counters.** `Value(..., lock=True)` guards against concurrent
writers. Rejected because each counter has exactly one writer by construction,
so the lock protects against a case the design forbids, at a measured 304ns
against 80ns per increment — a cost paid per event at firehose rates.

**`Manager()` proxies for shared state.** Hold arbitrary Python objects, which
shared memory cannot. Rejected because every read and write is an IPC round trip
to a server process, reintroducing the channel and its backpressure for data
that changes thousands of times a second.

**One queue for logs and the sampled feed.** Simpler, one fewer object to
thread through the tree. Rejected because `put_nowait` drops on a full queue and
capacity would be shared, letting decorative post text displace error records.

### Lifecycle

**A `pulse init` command.** The conventional place for directory creation,
catalog setup and a version check. Rejected because live-tail-only ingestion
removed everything it would have asked — with the credential question and the
backfill window both settled elsewhere, it became a step that delays the first
run. Its remaining
responsibilities move into the hub, under the lock.

**The lock on the TUI.** Simpler to report, since the conflict surfaces before
anything spawns. Rejected because the lock's lifetime would then exceed the
pipeline's: a hub still winding down inside its EOF detection window would sit
unprotected, and a fresh instance could spawn a second hub beside it. The hub
holding it makes lock lifetime and pipeline lifetime identical, and a distinct
exit code carries the conflict back to the TUI.

### Packaging

**The dashboard as an optional extra.** Streamlit and its transitive
dependencies account for roughly 101 MiB of the 143 MiB dependency set, so
`pulse[dashboard]` would cut first-run download from ~155 MiB to ~54 MiB.
Rejected because the saving is a one-time twelve seconds on a 100 Mbit link,
paid against a degraded-mode code path in the TUI and a second installation story
to document.

**Streamlit as a `subprocess`.** Its natural interface is the `streamlit run`
CLI. Rejected because a subprocess cannot participate in EOF teardown, so a
SIGKILLed hub would leave an orphaned server holding its port — a failure that
is confusing precisely because everything else cleaned up. Running its bootstrap
inside a `Process` with a watchdog thread gives it the same teardown as its
siblings.

---

## 6. Cross-cutting concerns

### Security and data handling

Live-tail ingestion runs unauthenticated against a public endpoint, so Pulse's
stored state is its data directory alone. Records are written locally and
transmitted nowhere; the dashboard binds to localhost. A documented teardown
path removes the data directory in full, which is part of why
`extension_directory` is pinned inside it. The DuckLake extension is fetched from DuckDB's
repository during bootstrap and cached inside Pulse's own cache directory.
Jetstream records arrive as decoded JSON from a third party and are treated as
data throughout, reaching the TUI only as projected text.

### Observability

The TUI is the primary instrument. Counters and heartbeats give per-stage
throughput and liveness, the log pane carries lifecycle events and errors, and
child stdout and stderr are redirected to `user_log_dir` for anything that
escapes the logging path.

### Resource bounds

Memory is bounded by construction: fixed-size shared memory, bounded queues,
a bounded `RichLog`, and a fixed-length sample window in the TUI. Disk is
bounded by delete-after-commit on the spool. The hot path is a memory increment
per event, with the feed rate-limited by time.

### Failure modes that present as silence

These fail silently, which is why each carries a named countermeasure.

| Failure | Countermeasure |
|---|---|
| Liveness pipe whose write end is held elsewhere | The stop Event, set by any child detecting EOF |
| A wedged child, its counters flat | Heartbeat age, read by the TUI |
| Log queue overflowing | `raiseExceptions` disabled, drop-on-full, bounded capacity |
| A child writing over the TUI canvas | stdout and stderr redirected in `PulseProcess.run()` |
| Cursor ageing past the lookback window | Reported as a discontinuity in the TUI |

---

## 7. Open questions

- A layout version marker for the data directory, and the migration path
- The teardown command, since directory creation is implicit
- Which process schedules DuckLake compaction
- The analytical data model: collections, table shapes, dashboard contents

---

## Appendix: measurements

Measured 2026-09-22 on Python 3.14.7, DuckDB 1.5.5, over a 100 Mbit connection.

| Measurement | Value |
|---|---|
| `import atproto` | 1.04s |
| `import textual.app` | 0.37s |
| `import streamlit` | 0.44s |
| `import duckdb` | 0.07s |
| Shared counter increment, lock-free | 80 ns |
| Shared counter increment, locked | 304 ns |
| DuckLake extension, on the wire | 12.1 MiB (34.7 MiB on disk) |
| DuckLake extension, cold install | 1.36s |
| Full dependency set | 142.7 MiB across 58 packages |
| Streamlit's share of that set | ~101 MiB |
| First run, total download | ~155 MiB, ~17s |

Roughly 16 of those 17 seconds belong to uv fetching packages before the
interpreter starts. The bootstrap phase the TUI controls runs a few seconds,
dominated by imports and process spawning.
