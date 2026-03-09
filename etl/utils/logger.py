"""
Structured JSON logger with context variable injection.
Each log line emits valid JSON for ingestion by any log aggregator (ELK, CloudWatch, etc.)
"""
from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

# Context variables propagate through async/threaded Airflow tasks
_run_id: ContextVar[str] = ContextVar("run_id", default="unknown")
_task_id: ContextVar[str] = ContextVar("task_id", default="unknown")
_dag_id: ContextVar[str] = ContextVar("dag_id", default="unknown")


def set_context(run_id: str, task_id: str = "", dag_id: str = "") -> None:
    """Call at the start of each Airflow task callable to inject correlation IDs."""
    _run_id.set(run_id)
    _task_id.set(task_id)
    _dag_id.set(dag_id)


class _JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": _run_id.get(),
            "task_id": _task_id.get(),
            "dag_id": _dag_id.get(),
            "module": record.module,
            "line": record.lineno,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    """Return a structured JSON logger. Safe to call multiple times for the same name."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
    return logger
