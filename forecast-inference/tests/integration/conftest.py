"""
Integration conftest for Data Platform tests.

Spins up a real Data Platform Docker container (and its backing Postgres)
using testcontainers, exactly as site-forecast-app does.
"""

import datetime as dt
import time
from importlib.metadata import version
from unittest.mock import patch

import betterproto
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.postgres import PostgresContainer


@pytest.fixture(autouse=True, scope="module")
def unfreeze_betterproto():
    """Patch betterproto's datetime serialization and deserialization during freeze_time.

    The root conftest.py has a session-scoped autouse freeze_time fixture that
    replaces the global datetime class with FakeDatetime. When freezegun is active,
    betterproto's internal default factories and type checks fail because FakeDatetime
    requires arguments when instantiated.
    """
    orig_get_field_default = betterproto.Message._get_field_default
    orig_postprocess = betterproto.Message._postprocess_single

    def patched_get_field_default(self, field_name: str):
        try:
            return orig_get_field_default(self, field_name)
        except TypeError:
            return None

    def patched_postprocess(self, wire_type, meta, field_name, value):
        if meta.proto_type == betterproto.TYPE_MESSAGE:
            cls = self._betterproto.cls_by_field.get(field_name)
            if cls is not None and isinstance(cls, type) and issubclass(cls, dt.date):
                return betterproto._Timestamp().parse(value).to_datetime()
        return orig_postprocess(self, wire_type, meta, field_name, value)

    with patch.object(betterproto.Message, "_get_field_default", patched_get_field_default), \
         patch.object(betterproto.Message, "_postprocess_single", patched_postprocess):
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
