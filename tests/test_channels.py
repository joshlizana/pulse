import ctypes
import multiprocessing
import os
import signal
import time

import pytest

from pulse.channels import Channels, Counter, Heartbeat

INCREMENTS = 1000


@pytest.fixture
def ctx():
    return multiprocessing.get_context("spawn")


@pytest.fixture
def channels(ctx):
    return Channels(
        counter=ctx.RawValue(Counter),
        stop_event=ctx.RawValue(ctypes.c_bool),
        heartbeat=ctx.RawValue(Heartbeat),
    )


# Spawned children find their target by importing this module, so each target
# lives at module level.


def increment(channels, ready):
    for _ in range(INCREMENTS):
        channels.counter.ingest += 1
    ready.send(True)
    time.sleep(60)


def beat(channels):
    channels.heartbeat.ingest = time.monotonic()


def wait_for_stop(channels):
    while not channels.stop_event.value:
        time.sleep(0.01)


def test_child_increments_are_visible_in_the_parent(ctx, channels):
    ready, child_end = ctx.Pipe(duplex=False)
    child = ctx.Process(target=increment, args=(channels, child_end))
    child.start()
    try:
        assert ready.poll(5)
        assert channels.counter.ingest == INCREMENTS
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
    assert channels.counter.ingest == INCREMENTS


def test_child_heartbeat_is_fresh(ctx, channels):
    before = time.monotonic()
    child = ctx.Process(target=beat, args=(channels,))
    child.start()
    child.join()
    assert child.exitcode == 0
    assert before <= channels.heartbeat.ingest <= time.monotonic()


def test_stop_flag_reaches_the_child(ctx, channels):
    child = ctx.Process(target=wait_for_stop, args=(channels,))
    child.start()
    channels.stop_event.value = True
    child.join(5)
    assert child.exitcode == 0
