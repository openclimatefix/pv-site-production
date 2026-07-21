"""
Integration conftest for Data Platform tests.

Spins up a real Data Platform Docker container (and its backing Postgres)
using testcontainers, exactly as site-forecast-app does.
"""

import time
from importlib.metadata import version
from unittest.mock import patch

import betterproto
import pytest
from freezegun.api import real_datetime
from testcontainers.core.container import DockerContainer
from testcontainers.postgres import PostgresContainer


@pytest.fixture(autouse=True, scope="module")
def unfreeze_betterproto():
    """Patch betterproto's datetime reference to use the real datetime class.

    The root conftest.py has a session-scoped autouse freeze_time fixture that
    replaces the global datetime class with FakeDatetime. betterproto's internal
    default_gen factory calls datetime() with no arguments which fails because
    FakeDatetime requires a 'year' argument.

    We patch betterproto.__init__.datetime with the real datetime class for the
    duration of the integration test module.
    """
    with patch.object(betterproto, "datetime", real_datetime):
        yield


@pytest.fixture(scope="module")
def dp_address():
    """Spin up a PostgreSQL + Data Platform container pair for the module.

    Yields (host, port) for the Data Platform gRPC server.
    Mirrors the fixture in site-forecast-app/tests/integration/conftest.py.
    """
    with PostgresContainer(
        f"ghcr.io/openclimatefix/data-platform-pgdb:{version('dp_sdk')}",
        username="postgres",
        password="postgres",  # noqa: S106
        dbname="postgres",
        env={"POSTGRES_HOST": "db"},
    ) as postgres:
        database_url = postgres.get_connection_url()
        database_url = database_url.replace("postgresql+psycopg2", "postgres")
        database_url = database_url.replace("localhost", "host.docker.internal")

        with DockerContainer(
            image=f"ghcr.io/openclimatefix/data-platform:{version('dp_sdk')}",
            env={"DATABASE_URL": database_url},
            ports=[50051],
        ) as data_platform_server:
            time.sleep(1)  # Give the server time to start

            port = data_platform_server.get_exposed_port(50051)
            host = data_platform_server.get_container_host_ip()
            yield host, port
