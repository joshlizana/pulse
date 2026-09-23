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
- **Ordinary laptop resources suffice.** Bounded memory and CPU at firehose
  rates, and a bounded spool. The store grows with observed history; its
  retention is an open question (§7).

### Non-goals

These are outcomes Pulse could reasonably pursue and deliberately holds outside
its scope.

- **Historical backfill.** Pulse observes from the moment it starts. Reaching
  further back means the metered archive, which places a credential and a
  metered bill between a new operator and a running pipeline.
- **Completeness across sessions.** Jetstream refuses a cursor older than its
  lookback window. Pulse reports the discontinuity and resumes from the live
  tip (§4.4).
- **Cryptographic verification.** Jetstream delivers decoded JSON, carrying
  repository signatures and MST proofs alone in the raw firehose. Verified
  ingestion would mean `FirehoseSubscribeReposClient` and CAR decoding.
- **Platforms other than Linux.** Pulse targets Linux, run natively or under
  WSL2, which runs a real Linux kernel. The design leans on Linux behaviour
  throughout — `fcntl.flock`, fd-level readiness in an asyncio loop,
  `CLOCK_MONOTONIC`, and the `spawn` behaviour measured in the appendix — and
  macOS and native Windows are neither tested nor supported.
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
├── ingest      Jetstream v2 live tail → JSONL segments
├── transform   sealed segment → DuckLake
└── dashboard   Streamlit server reading DuckLake
```

| Process | Owns |
|---|---|
| **TUI** | The view, the lifetime, the channel objects, and the data directory: the instance lock, the bootstrap, and the three children. |
| **ingest** | One Jetstream live-tail connection and the open JSONL segment. |
| **transform** | Loading sealed segments into DuckLake and maintaining the catalog. |
| **dashboard** | The Streamlit server. |

`multiprocessing` adds one process of its own under `spawn`: the resource
tracker, which removes named semaphores left behind by a process that died
without releasing them. Pulse neither starts nor stops it directly, but it
belongs to the tree and exits with it.

---

## 4. Detailed design

### 4.1 Process model

The TUI starts every child with the `spawn` start method, through one
`get_context("spawn")` context, and creates every `multiprocessing` object from
that context. The module-level default on Linux under Python 3.14 is
`forkserver`, and spawn preparation data carries only the *global* start method
(`get_start_method(allow_none=True)`), which `get_context()` leaves unset, so
code in a child reaching for a bare `multiprocessing.Queue` would get
`forkserver` too. The explicit context also keeps a child correct when a test
starts it directly.

Every child is a `PulseProcess`: a `multiprocessing.Process` subclass whose
`run()` establishes the cross-cutting contract and then calls the subclass's
`work()`. The contract covers terminal signals, stream redirection, logging
(§4.8), the parent watchdog, and cleanup in a `finally`. Subclasses implement
`work()` alone, which also keeps the process boundary thin enough to test
`work()` directly.

Each child receives a uniform constructor: the channels bundle, its own liveness
pipe end, a duplicate of the instance lock's descriptor (§4.3), and the config.

#### Terminal signals

Spawned processes stay in the TUI's process group, and a closing terminal
delivers SIGHUP to the whole group: an interactive shell resends it to every
job before exiting. A child taking the default action would die without its
`finally`, abandoning its open segment or its transaction while the tree
appeared to exit cleanly.

`PulseProcess.run()` therefore ignores SIGINT and SIGHUP before anything else.
That leaves the TUI as the one process that reacts to the terminal, and the tree
follows it through the graceful path. SIGTERM keeps its default action, so a
child can still be terminated deliberately (§4.3).

### 4.2 Channels

Five channels, separated by frequency and by behaviour under pressure.

| Channel | Kind | Direction | Carries | Under pressure |
|---|---|---|---|---|
| **Counters** | `ctypes.Structure` in shared memory, lock-free | child writes, TUI reads | totals, heartbeat, state | Sampled; each writer proceeds at its own pace |
| **Stop** | Flag in the same shared memory, lock-free | any process → all | The graceful shutdown signal | Written once, polled; a dead writer holds nothing |
| **Liveness** | `Pipe(duplex=False)` | TUI holds write end | Its closure is the message | Silent by design |
| **Logs** | bounded `Queue` | all → TUI | structlog events as JSON, in `LogRecord`s via `QueueHandler` | Drops on full, through `put_nowait` |
| **Feed** | bounded `Queue` | ingest → TUI | Sampled event projections | Drops on full |

#### Counters carry the hot path

Counters live in a `ctypes.Structure` with one section per child, so the
single-writer rule is visible in the type. The TUI samples at its own rate;
sampling decouples the two paces, letting ingest write as fast as events arrive
whatever speed the TUI reads at. This protects the cursor, since ingest falling
behind its websocket risks a gap past the lookback window.

Children increment monotonic totals and write a heartbeat from
`time.monotonic()`. The TUI derives rates from successive samples over a
ten-second sliding window, and reads the heartbeat to distinguish an idle child
from a stalled one — a distinction a flat rate leaves ambiguous.

Heartbeats are compared across processes: a child writes one and the TUI
subtracts it from its own reading. Python documents only the difference between
two `time.monotonic()` calls as meaningful, so this relies on the clock behind
it being system-wide. On Linux it is: `time.monotonic()` reads
`CLOCK_MONOTONIC`, one clock for the whole system. Heartbeat ages and rates are
computed all session long, so they need a clock that NTP cannot step and that
pauses through a suspend along with the processes it times.

Shared memory belongs to the TUI, so counters survive a child dying and a
replacement continues the same totals. Children increment alone; initialisation
belongs to the TUI at creation.

Each section also carries two times for the shutdown view (§4.3): when the child
saw the stop, and when its cleanup finished. They are written once per session,
by the section's owner like everything else in it. The third time the view
shows, the child's exit, is the TUI's own observation and stays in the TUI's
memory.

#### The stop flag

The stop signal is a single field beside the counter sections, and the one field
in the structure with more than one writer. Any process may set it, and every
write stores the same value, so writers agree without a lock. Beside it sits the
wall-clock time it was set, written by whoever sets it. Concurrent setters write
times moments apart, and whichever lands last is the one shown.

Each child's watchdog thread waits on its liveness pipe with a short timeout and
reads the flag each time it wakes. One thread thereby observes both signals, and
a stop reaches every child within one timeout. That polling latency is the price
of holding no lock: a lock held by a process that dies stays held, which is why
the stop signal is a lock-free flag (§5).

#### The liveness pipe

The TUI creates one pipe per child, passes the read end to the child, keeps the
write end, and closes its own copy of the read end. Nothing is ever written to
it; the child's `EOFError` on the TUI's death is the entire protocol.

**The write end is held by exactly one process — the TUI, whose death it
represents.** Another living process holding it keeps the pipe open and the
child waits indefinitely. Under `spawn` a child inherits only the descriptors
passed to it, so a second holder takes a mistake in what the TUI passes. That
failure presents as an absence, which is why the stop flag doubles as a second
path (§4.3).

#### Two queues, separately bounded

Logs and the sampled feed hold separate capacity, so sampled post text, which is
decorative, leaves error records, which are load-bearing, untouched.

The feed is rate-limited **in ingest**, on a time interval, so the scroll
cadence stays constant whether the firehose runs at 200/sec or 2000/sec. Ingest
projects each sample to a DID, a collection and a snippet before it crosses the
boundary. Commit events carry the author's DID and no handle; handles arrive
only on identity events, when one changes. Showing handles would mean resolving
them against the network, which stays out of ingest's hot path (§7).

#### Queues and exit

A queue write passes through a feeder thread in the writing process, and that
process exits only once the thread has flushed its buffer into the pipe. A
reader that stops reading can therefore hold a writer's exit indefinitely.

While the TUI lives, it drains both queues for its whole life, including while
the shutdown view waits on the children. Once the TUI has died, no reader will
ever return, so a child seeing EOF calls `cancel_join_thread()` on both queues
before tearing down. Its exit then skips the flush, and what it drops has no
one left to read it. Where an orphaned child's log records go instead is covered
in §4.8.

#### Crossing the boundary

`Queue`, `Value` and `Array` cross between processes at construction only,
enforced by `assert_spawning`, travelling as `Process` constructor arguments. A
`Connection` may additionally be handed over after `start()` by fd-passing. A
raw descriptor, such as the instance lock's, crosses at construction through
`multiprocessing.reduction.DupFd`, the mechanism a `Connection` uses.

The TUI bundles the shared objects into one frozen `Channels` dataclass and
hands it whole to each child.

### 4.3 Lifecycle

#### Startup

Before the app starts:

1. TUI resolves config
2. TUI creates the data directory, then opens the lockfile, creating it, and
   locks it. A held lock exits with code 3 before any screen is drawn.
3. TUI creates the channels bundle

Then `app.run()`, and the startup view, driven from a worker thread started in
`on_mount`:

4. Bootstrap under the lock: the cache directory with its marker file,
   extensions, catalog, spool directory, each step shown as it completes
5. The log directory created, with its marker file, since the children's stream
   redirection is its first use
6. One liveness pipe per child, and the three children spawned, each holding a
   duplicate of the lock's descriptor. Each child's row shows when it was
   spawned and when its first heartbeat arrived.
7. The worker ends, and the main view takes over: counters sampled, panes
   rendered, children supervised from the sampling timer

Each directory is created immediately before its first use, so a refused second
instance creates nothing beyond the data directory it needed for the lock.

Bootstrap here means only what must happen under the lock. Creating the data
directory comes before it, since the lockfile lives there and must exist before
it can be locked. That one step is safe to run unlocked:
`Path.mkdir(parents=True, exist_ok=True)` leaves an existing directory as it
is, and two racing instances both succeed and end up with the same empty
directory. Opening the lockfile creates it, and the first instance to `flock`
it wins; everything that could conflict comes after.

The lock comes before the channels because creating the first queue starts the
resource tracker, a process a second instance about to exit has no use for. The
worker starts inside the app because it reports progress through
`app.call_from_thread()`, which needs the event loop running, and because a
cold bootstrap takes seconds worth showing on screen.

Bootstrap happens once, in one process, under the lock, before any child exists.
Children open what already exists.

The channels bundle is created before `app.run()` by necessity. While it runs,
Textual replaces `sys.stderr` with a capture object whose `fileno()` returns
`-1`. The first `multiprocessing` object that needs a semaphore starts the
resource tracker, and that launch passes `sys.stderr.fileno()` to the new
process, so inside the app it fails with `bad value(s) in fds_to_keep`. Once the
tracker is running, spawning from inside the app is safe: `spawn` launches a
fresh interpreter, so Textual's threads carry over nothing. Everything that
creates a `multiprocessing` primitive therefore happens in step 3, and spawning
happens in step 6, from a worker thread, while the TUI shows it.

The lock comes before the app so that a second instance fails fast and plainly:
it prints that Pulse is already running and exits, with no screen drawn and
nothing spawned. Log files open in append mode, so a crash's logs survive the
next session's start.

A bootstrap step that fails, a data directory in an older layout (§7) say, is
reported in the startup view, and no child is spawned. The lock is already
held, so `u` can still remove the directory.

#### The app and its screens

The TUI is an `App` subclass. Its constructor takes the config, the channels
bundle and the lock descriptor, all created in `cli.main()` before the app
exists, since creating the channels inside the running app fails (above). The
startup, main and shutdown views are `Screen` subclasses, switched as the
lifecycle moves on. The app itself owns the lifecycle: the startup worker, the
sampling timer, and the children.

The startup worker is started from the app's `on_mount`. Textual ties a worker
to the node that started it and cancels it when that node is unmounted. For a
thread worker, cancelling only sets a flag — Textual documents that cancelled
work may still be running — so a worker owned by the startup screen would be
marked cancelled the moment the screen was replaced, and could be left
half-finished mid-spawn. Started from the app, it lives as long as the app
does.

The worker reports each step by posting a message, such as a bootstrap step
completing or a child spawning, which the screen on display handles. Widgets
belong to the event loop's thread and are updated there alone, and messages keep
the worker ignorant of how its progress is drawn, which also lets a test drive
the worker and read its messages without a screen.

#### Graceful shutdown

The TUI sets the stop flag and opens the shutdown view. Children finish their
current unit of work and exit — ingest seals its open segment, transform
completes its in-flight transaction. The TUI waits on each child from the view,
draining both queues, and exits the app once all three have gone. The lock
releases once the TUI and every child have closed their copies of its
descriptor.

#### The shutdown view

`q` opens a shutdown view, sets the stop flag, and keeps the app running until
the children are down, so the teardown can be watched as it happens.

The view has one row for each child, filled in as the child reports:

- **Stop seen:** when the child noticed the stop flag or EOF, from shared memory
- **Stopped:** when its cleanup finished, the last write in its `finally`, from
  shared memory
- **Exited:** when the process ended, which only the parent can see. The TUI
  records it, with the exit code, when the child's `Process.sentinel` becomes
  ready.

The times come from `time.time()` and are shown as they are, each with its
offset from the moment the flag was set. Shutdown lasts about a second, so a
wall-clock step inside it is unlikely, and would cost one odd-looking row.
Heartbeats stay monotonic because they are compared all session long (§4.2).

"Stopped" and "exited" differ because a process keeps working after its last
write. After `run()` returns, `multiprocessing` runs finalizers, waits for each
queue's feeder thread to flush (§4.2), and runs `atexit` handlers before
`os._exit()`. None of that is Pulse's code, so only the parent can time the real
exit. The gap between the two times places a hang: stopped but not exited means
the process cannot leave, usually a queue flush; neither means the teardown
itself is stuck.

A row still waiting shows its elapsed time counting up. A child that misses its
join deadline shows the escalation, terminated and then killed, in its row. A
child killed outright leaves its "stopped" time blank, so the gap points at the
child that died or hung. The log pane stays visible beneath the rows, so each
child's teardown lines arrive alongside its times.

The view keeps the event loop free. On the sampling timer it checks each
child's sentinel, applies the join deadlines, and drains both queues, as the TUI
does all session. Once every child has exited, the view shows the total
shutdown time, holds briefly so the final state can be read, and exits the app.
The `finally` in `main()` then finds the children already down.

The view also opens when the stop comes from elsewhere. A child that exits while
the stop flag is clear has died unexpectedly: the TUI sets the flag itself,
switches to the view, and the dead child's row shows its exit code, negative for
a signal.

#### Join deadlines

Every join carries a deadline longer than the slowest unit of work, a transform
transaction. A child that misses its deadline is sent SIGTERM, then SIGKILL, and
is reported. The deadlines bound unanticipated hangs, so a teardown that goes
wrong still ends.

#### Orphan teardown

The TUI dying before setting the flag leaves each child with `EOFError` on its
liveness pipe, and they run the same cleanup. Both paths converge on one
routine, so the path exercised least shares an implementation with the path
exercised most.

A child seeing `EOFError` sets the stop flag, cancels its queue joins (§4.2),
and tears itself down. The children are siblings with one parent, so they see
EOF together, and each is reaped by the system once the TUI is gone. Setting the
flag covers the case where a sibling's pipe stays silent: a liveness pipe whose
write end is held by another living process reads as open indefinitely. The
cost is one write per teardown, and it can happen only once the TUI is already
gone.

#### Exit paths

| Trigger | Path |
|---|---|
| **`q`** (or Ctrl+Q) | Shutdown view → graceful → app exits |
| **`u`**, confirmed | Shutdown view → graceful → uninstall → app exits |
| **`kill -INT`** | Installed handler opens the shutdown view → graceful |
| **`kill -TERM`** | Installed handler opens the shutdown view → graceful |
| **Terminal closed (`SIGHUP`)** | Installed handler exits the app at once; `finally` → graceful, unseen; children ignore SIGHUP (§4.1) |
| **`kill -9`** | Children tear down through EOF |
| **A child exits unexpectedly** | TUI sets the stop flag and opens the shutdown view; `main()` returns a nonzero code |

The TUI installs one handler for SIGINT, SIGTERM and SIGHUP. The first two open
the shutdown view, as `q` does. SIGHUP exits the app at once, since the terminal
the view would draw on is gone.

A `try/finally` around `app.run()` in `main()` stays as the backstop. When the
app exits before the children are down, on SIGHUP or an unhandled exception,
the `finally` sets the stop flag and waits on the children with the same
deadlines, unseen. After the view has run, it finds nothing left to do.

The TUI binds **`q`** to quit and **`u`** to uninstall, and keeps Textual's
own **Ctrl+Q**. Single-key bindings work because the TUI has no text input to
take the keystrokes. Ctrl+C displays a notification naming `q`, since Textual
reserves Ctrl+C for copy. Textual's Linux driver leaves SIGINT, SIGTERM and
SIGHUP to the application.

#### Exit codes

`main()` returns a member of `ExitCode`, an `IntEnum` in `cli.py`, so every code
Pulse can return has a name and one definition. The codes are part of Pulse's
interface: scripts and tests check for them.

| Code | Name | Meaning |
|---|---|---|
| 0 | `SUCCESS` | A clean exit |
| 2 | `USAGE_ERROR` | Bad arguments, the argument parser's convention |
| 3 | `ALREADY_RUNNING` | Another instance holds the lock |

A child dying unexpectedly gets its own code when 1.5 builds that path. Signal
deaths keep their separate space: the shell reports them as 128 plus the signal
number, and `Process.exitcode` as the negative signal number.

#### Uninstall

`u` is `q` with one extra step. After a confirmation dialog, it opens the
shutdown view as quit does. Once every child has exited, the view runs the
cleanup step, removing everything Pulse has written — the data, cache and log
directories (§4.5) — with a row for each, then exits the app. Cleanup runs only
from the view: an uninstall cut short by SIGHUP leaves the directories in
place.

The cleanup step has two guards, and if either fails it removes nothing:

- **The lock.** The TUI closes its own lock descriptor, opens the lockfile
  afresh, and takes the lock without blocking. The fresh lock succeeds only when
  no copy of the old descriptor remains open anywhere, which proves every child
  has exited whatever the TUI observed. It also holds the directory against a
  new instance while the cleanup runs.
- **The markers.** Each directory is removed only if it carries Pulse's marker:
  the lockfile in the data directory, and a marker file written when the cache
  and log directories are created. The XDG environment variables can move any
  of the three directories anywhere, and the marker keeps a misdirected path
  from being deleted.

The fresh lock is held through the whole removal and released only after it.
On Linux a file can be unlinked, and its emptied directory removed, while a
descriptor holds it open, and the lock belongs to the file itself. Releasing
first would leave a window in which a new instance could take the lock and
start writing into a directory being deleted.

The lockfile is therefore removed last. Once its name is gone, a new instance
would create a fresh lockfile and lock it without conflict, so the lock protects
the directory only while the name still points at the locked file. The order
is: the rest of the data directory, then the lockfile, then the empty directory,
then the cache and log directories, and finally the descriptor closed.

The confirmation guards against a stray keypress, since one key would otherwise
delete the whole observed history.

Pulse itself stays in uv's cache, where `uvx` keeps the package. The exit
message ends with `uv cache clean pulse` for that last step.

#### Single instance

The TUI holds an exclusive `flock` on a lockfile inside the data directory from
before the app starts until `main()` returns. A second instance finds it held,
prints that Pulse is already running, and exits with code 3.

The TUI passes a duplicate of the locked descriptor to each child. A `flock`
belongs to the open file description that every duplicate shares, so the lock
holds until the last of them closes. A TUI killed outright leaves its children
holding the lock through their EOF teardown, and a new instance is refused
until they finish. Lock lifetime and pipeline lifetime are therefore the same.

The lock lives in the data directory so that it guards the resource: two
instances pointed at different data directories run side by side. `flock`
releases on process death of any kind, so a stale lock resolves itself.

### 4.4 Data path

#### Ingest

One Jetstream v2 live-tail connection through the `atproto` SDK,
unauthenticated. Events append to an open JSONL segment, **sealed every 5
seconds: fsync, then atomic rename**. Segments are named by the seq of their
first event, zero-padded to a fixed width, so names sort as numbers and ordering
holds across restarts.

Sealing by rename means a segment is either invisible or complete, and the fsync
ahead of it carries that guarantee from process death through to power loss. The
transformer sees whole files.

#### The cursor

The cursor is Jetstream's `seq`, a monotonic per-event sequence number. Pulse
keeps no separate cursor file. The resume position is the highest seq in sealed
data: the newest segment in the spool, or the store when the spool holds none.

Resuming there re-fetches the events of a segment that was still open when the
process died, as long as the restart falls inside the lookback window. The
pre-rename file a `kill -9` leaves behind is therefore deleted at startup rather
than salvaged.

The server replays inclusively from the cursor, so the first event of a resumed
session is one already stored, and deduplication absorbs it. Within a session
the SDK tracks the cursor itself and drops the event each reconnect redelivers.

A seq is assigned by the Jetstream instance serving the connection, and whether
the two public instances number alike is unconfirmed (§7). The host that
produced the data is recorded in the data directory. A session configured for a
different host treats its cursor as stale.

#### Stale cursors

Jetstream refuses a seq cursor older than its lookback window before the
websocket upgrade, and the SDK raises `JetstreamCursorTooOldError`, which ends
the subscription. The SDK's own reconnects carry the cursor too, so the refusal
can arrive mid-session as well as at startup: a laptop asleep for longer than
the window wakes to it.

Ingest catches the error, discards the cursor, reports the discontinuity to the
TUI, and resubscribes from the live tip. Left uncaught, the error would end
ingest and with it the tree.

A timestamp cursor (a value of 10^15 or more, read as unix microseconds)
clamps up to the oldest retained event and would replay the whole window. Pulse
resumes by seq alone.

#### Transform

One sealed segment maps to **one transaction**. On commit the segment moves into
`spool/done/`, so any segment in the spool itself is unprocessed and crash
recovery is a directory listing. `done/` is pruned by age, leaving a window in
which a transform bug stays recoverable. A segment that fails to load moves to
`spool/quarantine/`.

#### An observation log

Pulse records the events that crossed the wire during the sessions it ran.
`create`, `update` and `delete` operations all land as rows: a delete is an
event that was observed, and the create it refers to remains an event that was
also observed.

This follows from live-tail ingestion. Pulse begins at the present, so its
earliest row is the first event it observed, and a delete usually refers to a
record created while Pulse was elsewhere. The analytical value lives in the
stream itself — rates, distributions, and what moved through the network during
the observed window.

#### Deduplication

Jetstream delivers at-least-once: a resumed session replays its first event, and
a crash between commit and the move to `done/` reprocesses a segment. Both
redeliver *the same event*, so the idempotency key identifies an event rather
than a record. The seq serves directly, and it is what a replay repeats.

A record's `at://` URI (DID, collection, rkey) identifies a record across its
lifetime, so a create and a later update of one post share it while remaining
two distinct events. The URI is a grouping dimension for queries; event identity
is what deduplication keys on.

Deduplication happens once, at the DuckLake write, so the layers above stay
simple. DuckLake rejects `PRIMARY KEY` and `UNIQUE` constraints, so the write is
an anti-join against stored rows whose seq falls in the segment's range. Because
seq is monotonic, Parquet min/max statistics exclude every older file, keeping
the check proportional to the segment.

#### DuckLake

SQLite catalog, Parquet data. The SQLite catalog takes two extensions,
`ducklake` and `sqlite_scanner`, both installed into an `extension_directory`
pinned inside Pulse's cache directory. The pin is a per-connection setting, so
every DuckDB connection in every process sets it. A connection without it
autoinstalls into `~/.duckdb/extensions`, outside anything uninstall removes.
`temp_directory` is pinned into the cache directory for the same reason, since
an in-memory connection otherwise spills into `.tmp` under the working
directory.

Segment-sized commits produce roughly 720 files and 720 snapshots an hour.
Compaction is a maintenance step of its own, run on a schedule, in three parts:

1. `ducklake_merge_adjacent_files` writes merged files beside the originals,
   which older snapshots still reference
2. `ducklake_expire_snapshots` releases those snapshots
3. `ducklake_cleanup_old_files` deletes the files they held

Merging alone raises the file count (appendix). Expiry ends time travel past its
horizon, and cleanup runs with a grace period longer than the slowest dashboard
query, which may still be reading an expired snapshot.

### 4.5 Storage layout

Paths come from `platformdirs`, as the defaults of the config's path fields
(§4.7), resolved with `ensure_exists=False` when the config is constructed, so
constructing a config only computes paths. Each directory is created from the
config's own path, with `mkdir(parents=True, exist_ok=True)`, immediately
before its first use: the data directory before the lock, the cache directory
during bootstrap, and the log directory just before the children are spawned
(§4.3). Creating from the config's path creates whatever path the config holds,
including a test's.

| Contents | Location |
|---|---|
| DuckLake catalog, Parquet, JSONL spool with `done/` and `quarantine/`, lockfile | `user_data_dir` |
| DuckDB extensions (`ducklake`, `sqlite_scanner`), DuckDB temp files, marker file | `user_cache_dir` |
| Child stdout/stderr, marker file | `user_log_dir` |

These three directories hold everything Pulse writes, which is what lets
uninstall remove it completely (§4.3).

Under WSL2 they belong on the Linux filesystem, which is where `platformdirs`
puts them by default: the home directory is ext4. A `HOME` or XDG variable
pointing into `/mnt/c` would move them onto the Windows filesystem, where
`flock`, atomic rename and `fsync` pass through a translation layer outside
this design's testing.

The JSONL spool sits under `user_data_dir` although its contents are
short-lived: a pending segment holds the only copy of its events once they age
past the lookback window. Cache directories are cleared by users and tools as a
matter of routine.

### 4.6 Module layout

Under `spawn`, two import paths execute in every child:

1. The console script runs `from pulse.cli import main` at module level, outside
   its `__main__` guard, as `runpy` re-executes it under `__mp_main__`
2. Unpickling a `PulseProcess` subclass imports the module defining it

Both are load-bearing, because `import atproto` alone costs 1.04s and a shared
module reaching heavy dependencies multiplies that across every process in the
tree.

- `pulse/__init__.py` and the entry module hold light imports alone, as do
  `config.py` and `lock.py`, which the entry module imports at module level
  and every child therefore loads
- Each `PulseProcess` subclass lives in its own module, with heavy imports
  inside `run()`

The entry module shares `__init__.py`'s universal reach, since the console
script's module-level import runs in every child. A module-level
`from pulse.tui import ...` there would reach the three child modules through
the TUI's spawning code, seating every heavy dependency in every process; that
import belongs in the body of `main()`.

`pulse/__main__.py` sits outside that path, since `multiprocessing.spawn`
returns early for a module whose name ends in `.__main__`, leaving a child under
`python -m pulse` to reconstruct itself through unpickling alone. Both entry
points reduce to the same two lines — `from pulse.cli import main`, then
`sys.exit(main())` — and `main()` returns an `int`, which keeps the
single-instance exit code identical across the two routes and leaves `main()`
callable from a test.

Children inherit the TUI's stdout and stderr, so a stray write lands on the
Textual canvas. Each child redirects both streams in `PulseProcess.run()`, and
`logging.raiseExceptions` is disabled so a full log queue stays quiet.

### 4.7 Configuration

Config is one frozen dataclass in `config.py`, `frozen=True, slots=True,
kw_only=True`, and nothing about it varies from the command line or the
environment. Its fields carry every tunable with its default in view: the
collection filters, the seal interval, the sampling window, queue capacities,
join deadlines, the Jetstream endpoint. The three path fields default through
`default_factory` to `platformdirs`, with `ensure_exists=False`, so they are
computed when a config is constructed.

`cli.main()` constructs it once, as `Config()`, and the TUI passes that instance
to every child alongside the channels bundle. Children use that instance
alone, so the whole tree runs on the paths the TUI locked. Filters are tuples,
since `frozen` guards rebinding alone and a list field stays mutable.
`__post_init__` validates, catching a bad value where it enters.

The tunables are fields because tests must change them in spawned children: a
short seal interval, short join deadlines, a stub that overruns its deadline in
seconds. A test patching a module constant
changes it only in its own process, since each child re-imports `config.py`
into a fresh interpreter. A config passed down reaches every child: a test
constructs `Config(seal_interval=0.1, join_deadline=2.0)` and hands it in.
Values no test would change, such as the application name and the lockfile's
name, stay as module constants.

`config.py` holds no config instance at module level. `cli.py` imports it at
module level, so every child imports it too, and unpickling the config a child
receives needs it anyway. A module-level instance would re-run resolution in
every child, and would look like one shared config while being four
independent copies.

Tests keep Pulse out of the real home directory through the environment
`platformdirs` reads. A test points `HOME` at `tmp_path` and clears
`XDG_DATA_HOME`, `XDG_CACHE_HOME` and `XDG_STATE_HOME`, which take precedence
over `HOME` when set, before constructing its `Config()`. All three directories
then resolve under `tmp_path`. Constructing a config first and redirecting
afterwards would leave its paths pointing at the real home directory.

### 4.8 Logging

Logging goes through structlog, configured on top of the standard library. One
path then carries Pulse's own events and those of its libraries — `atproto`,
`websockets`, Streamlit — which log through `logging`. Bound fields travel as
fields, so the log pane can colour and filter by them.

#### One configuration, every process

Logging configuration is process-global, and `spawn` starts each child in a
fresh interpreter, so none of it crosses the boundary. One function configures
logging, called from `PulseProcess.run()` in each child and from `main()` in the
TUI. Every process's root logger carries a `QueueHandler` onto the shared log
queue, the TUI's included, so the log pane has one source.

structlog calls and plain `logging` records pass through the same processors:
level, a UTC timestamp, and the process name, which is the name each `Process`
is constructed with. The handler's formatter is structlog's
`ProcessorFormatter`, ending in `JSONRenderer`. Every process imports structlog,
at 0.10s (appendix), since every process logs.

#### JSON across the boundary

`QueueHandler.prepare()` formats a record through its handler's formatter before
queueing it, and clears the unpicklable fields. Each record therefore
crosses as a `LogRecord` whose message is one JSON object: the event, its bound
fields, level, timestamp, process name, and any exception, rendered to text in
the child. The JSON renderer falls back to `repr()` for a value it cannot
serialise, so no log call fails to cross. The TUI parses the JSON and renders
the record for the pane.

Logging is for lifecycle events and errors. Per-event facts belong in counters,
which keeps logging, and the JSON rendering it now includes, off the hot path.

#### Before configuration

Unconfigured, structlog prints to `sys.stdout`, which in a child is the TUI's
canvas until `run()` redirects it. `run()` therefore configures logging after
redirecting the streams, and nothing logs at import time: unpickling a
`PulseProcess` subclass imports its module before `run()` begins. A module-level
`structlog.get_logger()` stays safe, since it returns a lazy proxy that binds on
first use.

#### Records after the pane is gone

On an ordinary quit, the log pane stays up through the shutdown view, so
teardown records land in it. The pane is gone only when the app exits before the
children are down, on SIGHUP or an unhandled exception, leaving the `finally` to
drain the log queue (§4.3). Warnings and errors drained then are printed to
stderr, through structlog's console renderer, once Textual has restored the
terminal, and the rest are dropped.

A child orphaned by the TUI's death has no reader for its queue. On EOF it
replaces its `QueueHandler` with a handler writing warnings and errors to its
own stderr, which is its log file (§4.5). An orphan teardown's failures
therefore outlive it.

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

**A hub process between the TUI and the children.** The earlier design: the TUI
spawned a hub, which held the lock, ran the bootstrap and supervised the three
children. It kept the lock's lifetime tied to the pipeline's, since a lock on
the TUI would have released while children were still tearing down. Rejected
once each child held a duplicate of the lock's descriptor, which ties the two
lifetimes together from the TUI directly. What remained was a second level of
EOF propagation, the channels relayed through a middle process, and exit times
reported back through shared memory, none of them required by the problem. The
hub also made a live startup sequence no easier: spawning from inside the
running app is safe once the channels exist (§4.3).

**`fork` or `forkserver`.** `fork` is faster and was the Linux default through
Python 3.13. Rejected because Textual runs an asyncio loop with threads, and
forking a multithreaded process is documented as problematic — `os.fork()`
raises a `DeprecationWarning` from 3.12 in that situation. Python 3.14 moved the
Linux default to `forkserver`, so an explicit `spawn` context also keeps
behaviour stable across interpreter versions.

**Daemonic children.** The obvious way to have children die with their parent.
Rejected on two counts: daemonic children are terminated only when the parent
exits *normally*, leaving them orphaned by a crash or a kill; and termination
offers no cleanup window, while ingest must seal its open segment.

**`PR_SET_PDEATHSIG` or polling `os.getppid()`.** Both detect orphaning without
a pipe. `PR_SET_PDEATHSIG` is Linux-only and fires on the death of the *thread*
that spawned. `getppid()` polling carries a startup race: a parent dying before
the child records its original ppid leaves the child comparing against the
reaper forever. File descriptor state is definitive where a sampled integer is
approximate, so the liveness pipe won.

**Supervised restart.** Rejected because the transient failures it would
address are already handled inside ingest. The SDK reconnects a dropped
websocket from its cursor, and ingest catches a cursor that has aged out
(§4.4). A dead ingest *process* therefore signals a bug, which restarting
conceals. The three children also want different policies: ingest restarts
cheaply but loses lookback window while down, transform restarts safely because
DuckLake rolls back and the segment survives until commit, and the dashboard is
cosmetic. Three policies wearing one name suggests the feature is premature.

### Inter-process communication

**One duplex pipe carrying everything.** The original design, with status,
control and logs on a single connection per child. Rejected because `send()`
blocks once the OS buffer fills, and the resulting chain — slow TUI stops
draining, ingest blocks, websocket falls behind, cursor passes the lookback
window — converts a rendering delay into permanent data loss. Moving counters to
shared memory removes the channel the chain depends on.

**Locked shared counters.** `Value(..., lock=True)` guards against concurrent
writers. Rejected because each counter has exactly one writer by construction,
so the lock protects against a case the design forbids, at a measured 304ns
against 80ns per increment — a cost paid per event at firehose rates.

**A `multiprocessing.Event` for the stop signal.** The earlier design, with
`wait()` offering an immediate wake-up. Rejected because `is_set()`, `set()` and
`wait()` each take a lock shared across processes, and a process killed while
holding it leaves it held. Killing a child that checked the Event in a loop left
the next `set()` blocked for good in 89 of 150 trials (appendix). A killed
child — the event the stop signal exists to handle — would then leave a tree
that never stops. A flag in shared memory takes no lock, at the price of
polling latency.

**`Manager()` proxies for shared state.** Hold arbitrary Python objects, which
shared memory cannot. Rejected because every read and write is an IPC round trip
to a server process, reintroducing the channel and its backpressure for data
that changes thousands of times a second.

**One queue for logs and the sampled feed.** Simpler, one fewer object to
thread through the tree. Rejected because `put_nowait` drops on a full queue and
capacity would be shared, letting decorative post text displace error records.

**structlog's event dict across the boundary.** Leaving the event dict in the
record, rendering nothing in the child, would keep every value's type. Rejected
because a queue pickles in its feeder thread, where an unpicklable value drops
the record and leaves only a traceback on the child's stderr. Every value bound
into a log call would then have to pickle. JSON keeps the fields and cannot
fail.

**Console text rendered in the child.** Rejected because the TUI would receive
finished strings, losing the fields it colours and filters by, carrying ANSI
codes meant for a terminal the child never sees.

### Lifecycle

**A `pulse init` command.** The conventional place for directory creation,
catalog setup and a version check. Rejected because live-tail-only ingestion
removed everything it would have asked — with the credential question and the
backfill window both settled elsewhere, it became a step that delays the first
run. Its remaining responsibilities move into the TUI's startup, under the lock.

**Closing the app first, then shutting down.** `q` returns from `app.run()`
and the `finally` stops the tree, perhaps printing a summary afterwards.
Simpler, since no screen has to supervise anything. Rejected because the
teardown then happens with nothing on screen: a hang shows as a frozen terminal
with no hint of which process is holding it, and the times, however complete,
arrive only after the fact. The shutdown view keeps the event loop running
through the teardown, and the `finally` remains for the exits that cannot use
it.

### Packaging

**The dashboard as an optional extra.** Streamlit and its transitive
dependencies account for roughly 101 MiB of the 143 MiB dependency set, so
`pulse[dashboard]` would cut first-run download from ~167 MiB to ~66 MiB.
Rejected because the saving is a one-time twelve seconds on a 100 Mbit link,
paid against a degraded-mode code path in the TUI and a second installation
story to document.

**Streamlit as a `subprocess`.** Its natural interface is the `streamlit run`
CLI. Rejected because a subprocess cannot participate in EOF teardown, so a
SIGKILLed TUI would leave an orphaned server holding its port — a failure that
is confusing precisely because everything else cleaned up. Running its bootstrap
inside a `Process` with a watchdog thread gives it the same teardown as its
siblings.

---

## 6. Cross-cutting concerns

### Security and data handling

Live-tail ingestion runs unauthenticated against a public endpoint, so Pulse
holds no credentials, and its stored state is its three directories (§4.5).
Records are written locally and transmitted nowhere.

The dashboard binds to a loopback address through `server.address`, since
Streamlit listens on every interface when it is unset. It runs with
`browser.gatherUsageStats` off, since Streamlit's default sends usage
statistics, and with `server.headless` on, which suppresses both the automatic
browser tab and the first-run email prompt.

Uninstall (§4.3) removes all three directories in full, which is why
`extension_directory` and `temp_directory` are pinned inside Pulse's cache
directory. The DuckDB extensions are fetched from DuckDB's repository during
bootstrap. Jetstream records arrive as decoded JSON from a third party and are
treated as data throughout, reaching the TUI only as projected text.

### Observability

The TUI is the primary instrument. Counters and heartbeats give per-stage
throughput and liveness, the log pane carries lifecycle events and errors from
every process as structured records (§4.8), and child stdout and stderr are
redirected to `user_log_dir` for anything that escapes the logging path.

### Resource bounds

Memory is bounded by construction: fixed-size shared memory, bounded queues,
a bounded `RichLog`, and a fixed-length sample window in the TUI. The spool is
bounded by the move on commit and the age pruning of `done/`. The store grows
with every observed event, and its retention is an open question (§7).
The hot path is a memory increment per event, with the feed rate-limited by
time.

### Failure modes that present as silence

These fail silently, which is why each carries a named countermeasure.

| Failure | Countermeasure |
|---|---|
| Liveness pipe whose write end is held elsewhere | The stop flag, set by any child detecting EOF |
| A process killed while holding a `multiprocessing` lock | No lock on the stop path; every join has a deadline |
| A writer's exit held by an unread queue | The TUI drains all session; an orphaned child cancels its queue joins |
| A wedged child, its counters flat | Heartbeat age, read by the TUI |
| Log queue overflowing | `raiseExceptions` disabled, drop-on-full, bounded capacity |
| A child writing over the TUI canvas | stdout and stderr redirected in `PulseProcess.run()`; nothing logs at import time, before structlog is configured |
| A `multiprocessing` object created inside the running app | All channels created before `app.run()` (§4.3) |
| Terminal signals reaching children | SIGINT and SIGHUP ignored in `PulseProcess.run()` |
| Cursor ageing past the lookback window | `JetstreamCursorTooOldError` caught in ingest; discontinuity reported in the TUI |

---

## 7. Open questions

- A layout version marker for the data directory, and the migration path
- Which process schedules DuckLake compaction
- Retention for the store, which otherwise grows with every observed event
- Handle resolution for the feed: whether to resolve, where, and with what cache
- Whether seq is comparable across the two public Jetstream instances
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
| First run, total download | ~155 MiB, ~17s, before `sqlite_scanner` was counted |

Roughly 16 of those 17 seconds belong to uv fetching packages before the
interpreter starts. The bootstrap phase the TUI controls runs a few seconds,
dominated by imports and process spawning.

Measured 2026-09-23 on the same versions.

| Measurement | Value |
|---|---|
| `import structlog` (26.1.0) | 0.10s |
| `sqlite_scanner` extension, on the wire | 11.7 MiB (33.2 MiB on disk) |
| First run, total download, both extensions counted | ~167 MiB |
| `Event.set()` after SIGKILL of a child polling `is_set()` | Blocked for good in 89 of 150 trials |
| DuckLake, 60 single-insert commits | 60 Parquet files, 62 snapshots |
| After `ducklake_merge_adjacent_files` | 61 files, 63 snapshots |
| After `ducklake_expire_snapshots` and `ducklake_cleanup_old_files` | 1 file, 1 snapshot |
| Three children spawned from a Textual worker thread, channels created before `app.run()`, headless and under a real pty | All ran and exited 0 |
| Signal mask those children inherited from the worker thread | Nothing blocked (`SigBlk` 0) |
| Liveness pipes and lock duplicates passed from the worker, write ends closed later from the main thread | Every child saw EOF; the lock stayed held until the last child exited |
| A `Queue` created inside the running app | `ValueError: bad value(s) in fds_to_keep` |
