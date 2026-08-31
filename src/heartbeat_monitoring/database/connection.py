"""
PostgreSQL connection management.

Uses a small psycopg connection pool rather than one long-lived connection.
A single connection held open for the lifetime of a stream consumer is a
liability: one network blip leaves it permanently broken, and every
subsequent insert fails. The pool transparently replaces dead connections.

`open=False` plus an explicit `.open()` is deliberate -- constructing a
Database must not perform I/O, so tests can build one without a live server.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from types import TracebackType

from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)


class Database:
    """Owns the connection pool and hands out connections."""

    def __init__(
        self,
        dsn: str,
        min_size: int = 1,
        max_size: int = 4,
        connect_timeout_seconds: float = 30.0,
    ) -> None:
        self._dsn = dsn
        self._connect_timeout_seconds = connect_timeout_seconds
        self._pool = ConnectionPool(
            conninfo=dsn,
            min_size=min_size,
            max_size=max_size,
            open=False,
        )

    def open(self) -> None:
        """
        Open the pool and block until at least one connection is live.

        Waiting here means a misconfigured database fails fast at startup with
        a clear error, instead of surfacing as a confusing failure on the first
        message hours into a run.
        """
        self._pool.open()
        self._pool.wait(timeout=self._connect_timeout_seconds)
        logger.info("PostgreSQL connection pool ready")

    def close(self) -> None:
        self._pool.close()
        logger.info("PostgreSQL connection pool closed")

    @contextmanager
    def connection(self) -> Iterator[object]:
        """
        Yield a pooled connection inside a transaction.

        psycopg commits on clean exit and rolls back if the block raises, so
        callers never have to remember to do either.
        """
        with self._pool.connection() as conn:
            yield conn

    # --- context manager sugar so entrypoints can't forget to close ---

    def __enter__(self) -> Database:
        self.open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
