import ctypes
import multiprocessing
import os
import signal
import time

import pytest

from pulse.channels import Channels, Dashboard, Extract, Load, Transform

INCREMENTS = 1000
QUEUE_CAPACITY = 8


@pytest.fixture
def ctx():
    return multiprocessing.get_context("spawn")


@pytest.fixture
def channels(ctx):
    return Channels(
        extract=ctx.RawValue(Extract),
        transform=ctx.RawValue(Transform),
        load=ctx.RawValue(Load),
        dashboard=ctx.RawValue(Dashboard),
        stop_event=ctx.RawValue(ctypes.c_bool),
        stop_time=ctx.RawValue(ctypes.c_double),
        logs=ctx.Queue(QUEUE_CAPACITY),
        feed=ctx.Queue(QUEUE_CAPACITY),
        raw=ctx.Queue(QUEUE_CAPACITY),
        rows=ctx.Queue(QUEUE_CAPACITY),
    )


# Spawned children find their target by importing this module, so each target
# lives at module level.


def increment(channels, ready):
    for _ in range(INCREMENTS):
        channels.extract.counter += 1
    ready.send(True)
    time.sleep(60)


def beat(channels):
    channels.extract.heartbeat = time.monotonic()


def wait_for_stop(channels):
    while not channels.stop_event.value:
        time.sleep(0.01)


def test_child_increments_are_visible_in_the_parent(ctx, channels):
    ready, child_end = ctx.Pipe(duplex=False)
    child = ctx.Process(target=increment, args=(channels, child_end))
    child.start()
    try:
        assert ready.poll(5)
        assert channels.extract.counter == INCREMENTS
    finally:
        child.kill()
        child.join()


def test_kill_leaves_totals_at_their_last_value(ctx, channels):
    ready, child_end = ctx.Pipe(duplex=False)
    child = ctx.Process(target=increment, args=(channels, child_end))
    child.start()
    assert ready.poll(5)
    os.kill(child.pid, signal.SIGKILL)
    child.join()
    assert child.exitcode == -signal.SIGKILL
    assert channels.extract.counter == INCREMENTS


def test_child_heartbeat_is_fresh(ctx, channels):
    before = time.monotonic()
    child = ctx.Process(target=beat, args=(channels,))
    child.start()
    child.join()
    assert child.exitcode == 0
    assert before <= channels.extract.heartbeat <= time.monotonic()


def test_stop_flag_reaches_the_child(ctx, channels):
    child = ctx.Process(target=wait_for_stop, args=(channels,))
    child.start()
    channels.stop_event.value = True
    child.join(5)
    assert child.exitcode == 0


@pytest.mark.parametrize(
    "name",
    [
        "extract",
        "transform",
        "load",
        "dashboard",
        "stop_event",
        "stop_time",
        "logs",
        "feed",
        "raw",
        "rows",
    ],
)
def test_shared_memory_crosses_at_construction_only(ctx, channels, name):
    """A started process can be reached only by sending over a connection.

    Sending pickles the object outside process construction, which
    `assert_spawning` refuses for shared memory and queues alike, so each
    reaches a child only as a `Process` argument.
    """
    sender, receiver = ctx.Pipe()
    try:
        with pytest.raises(RuntimeError, match="through inheritance"):
            sender.send(getattr(channels, name))
    finally:
        sender.close()
        receiver.close()


def fill_raw(channels, capacity):
    for i in range(capacity + 1):
        channels.raw.put(i)
        channels.extract.counter += 1


def test_full_raw_queue_blocks_its_writer_until_read(ctx, channels):
    """A put onto a full data queue waits, which is the pipeline's backpressure."""
    child = ctx.Process(target=fill_raw, args=(channels, QUEUE_CAPACITY))
    child.start()
    try:
        deadline = time.monotonic() + 5
        while channels.extract.counter < QUEUE_CAPACITY:
            assert time.monotonic() < deadline, "writer never filled the queue"
            time.sleep(0.01)
        time.sleep(0.2)
        assert channels.extract.counter == QUEUE_CAPACITY, "put did not block"
        channels.raw.get(timeout=5)
        child.join(5)
        assert child.exitcode == 0
        assert channels.extract.counter == QUEUE_CAPACITY + 1
    finally:
        if child.is_alive():
            child.kill()
            child.join()
