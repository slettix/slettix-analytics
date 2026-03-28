"""
Shared structured JSON logger for all Spark jobs.

Each log record is emitted as a single JSON line to both stdout and
$SPARK_LOG_DIR/<job_name>.log (default: /opt/spark/logs).

Usage:
    from spark_logger import get_logger
    log = get_logger("my_job")
    log.info("Processing", extra={"rows": 42, "source": "s3a://..."})
"""

import json
import logging
import os
from datetime import datetime, timezone

LOG_DIR = os.environ.get("SPARK_LOG_DIR", "/opt/spark/logs")

# LogRecord attributes that are internal to the logging framework — never
# included as extra fields in the JSON output.
_INTERNAL_ATTRS = frozenset({
    "args", "created", "exc_info", "exc_text", "filename", "funcName",
    "levelname", "levelno", "lineno", "message", "module", "msecs",
    "msg", "name", "pathname", "process", "processName", "relativeCreated",
    "stack_info", "taskName", "thread", "threadName",
})


class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single, valid JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        data: dict = {
            "ts":    datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "job":   record.name,
            "msg":   record.getMessage(),
        }
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        for key, val in record.__dict__.items():
            if key not in _INTERNAL_ATTRS:
                data[key] = val
        return json.dumps(data, default=str)


def get_logger(job_name: str) -> logging.Logger:
    """
    Return a structured JSON logger for *job_name*.

    Writes to:
      - stdout (always)
      - $SPARK_LOG_DIR/<job_name>.log  (created if it doesn't exist)
    """
    logger = logging.getLogger(job_name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = _JsonFormatter()

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    os.makedirs(LOG_DIR, exist_ok=True)
    fh = logging.FileHandler(os.path.join(LOG_DIR, f"{job_name}.log"))
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger
