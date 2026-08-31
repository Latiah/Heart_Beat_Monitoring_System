"""
Fixtures for infrastructure-dependent tests.

These talk to the real Kafka broker and real PostgreSQL from
docker-compose -- no mocks. Mocks here would only prove the code calls the
methods we told it to call; the whole point of these tests is to catch wiring
mistakes (wrong column name, wrong topic, wrong serialisation) that unit tests
structurally cannot see.

Requires `make up` and `make db-init` first. If the infrastructure is absent
the tests skip rather than fail, so `make test-unit` stays usable offline.
"""

from __future__ import annotations

import uuid

import pytest

from heartbeat_monitoring.config import Settings
from heartbeat_monitoring.database import Database, HeartRateRepository

pytestmark = pytest.mark.integration

CONNECT_TIMEOUT_SECONDS = 10.0


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="session")
def database(settings: Settings):
    """A live connection pool, or skip the whole module if Postgres is down."""
    db = Database(dsn=settings.postgres_dsn, connect_timeout_seconds=CONNECT_TIMEOUT_SECONDS)
    try:
        db.open()
    except Exception as exc:
        pytest.skip(f"PostgreSQL unavailable ({exc}); run `make up` and `make db-init`")

    # Fail loudly rather than mysteriously if the schema was never applied.
    try:
        with db.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM heart_rate_readings LIMIT 1")
    except Exception as exc:
        db.close()
        pytest.skip(f"heart_rate_readings table missing ({exc}); run `make db-init`")

    yield db
    db.close()


@pytest.fixture
def repository(database) -> HeartRateRepository:
    return HeartRateRepository(database)


@pytest.fixture
def test_customer_id() -> str:
    """
    A unique customer per test.

    Scoping assertions to a per-test ID means these tests neither interfere
    with each other nor with data left behind by a real producer run, so they
    never need to truncate the table.
    """
    return f"TEST_{uuid.uuid4().hex[:12]}"


@pytest.fixture(autouse=True)
def cleanup_test_rows(database, test_customer_id):
    """Remove only this test's own rows, leaving any demo data intact."""
    yield
    with database.connection() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM heart_rate_readings WHERE customer_id = %s",
            (test_customer_id,),
        )
