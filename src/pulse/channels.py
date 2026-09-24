import ctypes
from dataclasses import dataclass
from multiprocessing.queues import Queue


class Extract(ctypes.Structure):
    _fields_ = [
        ("counter", ctypes.c_int64),
        ("heartbeat", ctypes.c_double),
        ("state", ctypes.c_char),
        ("stop_seen", ctypes.c_double),
        ("stopped", ctypes.c_double),
    ]


class Transform(ctypes.Structure):
    _fields_ = [
        ("counter", ctypes.c_int64),
        ("heartbeat", ctypes.c_double),
        ("dropped", ctypes.c_int64),
        ("state", ctypes.c_char),
        ("stop_seen", ctypes.c_double),
        ("stopped", ctypes.c_double),
    ]


class Load(ctypes.Structure):
    _fields_ = [
        ("counter", ctypes.c_int64),
        ("heartbeat", ctypes.c_double),
        ("pipeline_latency", ctypes.c_double),
        ("e2e_latency", ctypes.c_double),
        ("state", ctypes.c_char),
        ("stop_seen", ctypes.c_double),
        ("stopped", ctypes.c_double),
    ]


class Dashboard(ctypes.Structure):
    _fields_ = [
        ("heartbeat", ctypes.c_double),
        ("state", ctypes.c_char),
        ("stop_seen", ctypes.c_double),
        ("stopped", ctypes.c_double),
    ]


@dataclass(frozen=True, slots=True, kw_only=True)
class Channels:
    extract: Extract
    transform: Transform
    load: Load
    dashboard: Dashboard
    stop_event: ctypes.c_bool
    stop_time: ctypes.c_double
    logs: Queue
    feed: Queue
    raw: Queue
    rows: Queue
