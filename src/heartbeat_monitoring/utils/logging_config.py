"""
Logging setup.

Long-running stream processors are debugged almost entirely through their
logs, so every entrypoint configures logging once, at startup, before doing
anything else. Library modules only ever call logging.getLogger(__name__) and
never configure handlers themselves.
"""

from __future__ import annotations

import logging
import sys

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_logging(log_level: str = "INFO") -> None:
    """
    Configure root logging for an entrypoint process.

    `force=True` replaces any handlers a dependency installed on import, which
    otherwise results in duplicated log lines.
    """
    level = getattr(logging, log_level.strip().upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
        stream=sys.stdout,
        force=True,
    )

    # These clients are extremely chatty at INFO and drown out pipeline logs.
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("psycopg").setLevel(logging.WARNING)
    logging.getLogger("psycopg.pool").setLevel(logging.WARNING)
