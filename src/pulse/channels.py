import ctypes
from dataclasses import dataclass


class Counter(ctypes.Structure):
    _fields_ = [
        ("ingest", ctypes.c_int),
        ("etl", ctypes.c_int),
    ]


class Heartbeat(ctypes.Structure):
    _fields_ = [
        ("ingest", ctypes.c_double),
        ("etl", ctypes.c_double),
    ]


@dataclass(frozen=True, slots=True, kw_only=True)
class Channels:
    counter: Counter
    stop_event: ctypes.c_bool
    heartbeat: Heartbeat
