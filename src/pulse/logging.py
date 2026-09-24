import multiprocessing

from pathlib import Path

import structlog


def configure_logging(
        name: str | None = None,
        queue: multiprocessing.Queue | None = None,
        log_file_path: Path | None = None,
        context_provider: object | None = None
) -> structlog.BoundLogger:
    processors=[
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.add_log_level,
        lambda logger, level, event_dict: event_dict.update({"name": name}) or event_dict,
        lambda logger, level, event_dict: (
            event_dict.update({"cid": context_provider.current_cid})
            if context_provider is not None and getattr(context_provider, "current_cid", None) is not None
            else event_dict
        ),
        structlog.processors.JSONRenderer()
    ]

    if queue is not None:
        logger_factory = lambda: lambda msg, *a, **k: queue.put(msg)
    else:
        log_path = log_file_path / "app.log" if log_file_path is not None else Path("app.log")
        log_file = open(log_path, "a", encoding="utf-8", buffering=1)
        logger_factory = structlog.PrintLoggerFactory(file=log_file)

    structlog.configure(
        processors=processors,
        logger_factory=logger_factory,
        wrapper_class=structlog.BoundLogger,
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger()
