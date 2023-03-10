"""
Fixtures for testing
"""

import os
from datetime import datetime, timedelta, timezone

import pytest
from pvsite_datamodel.connection import DatabaseConnection
from pvsite_datamodel.sqlmodels import Base, ClientSQL, GenerationSQL, SiteSQL, StatusSQL
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session", autouse=True)
def database_connection():
    """Database engine, this includes the table creation."""
    with PostgresContainer("postgres:14.5") as postgres:
        url = postgres.get_connection_url()
        os.environ["OCF_PV_DB_URL"] = url

        database_connection = DatabaseConnection(url, echo=False)

        engine = database_connection.engine

        Base.metadata.create_all(engine)

        yield database_connection

        engine.dispose()


@pytest.fixture()
def db_session(database_connection):
    """Creates a new database session for a test.

    We automatically roll back whatever happens when the test completes.
    """

    with database_connection.get_session() as session:
        with session.begin():
            yield session
            session.rollback()


@pytest.fixture(scope="session", autouse=True)
def db_data(database_connection):
    """Fill in the database with some initial data."""

    # Those fixtures are inspired from:
    # https://github.com/openclimatefix/pvsite-datamodel/blob/ab7478b0a8c6f0bc06d998817622f7a80e6f6ca3/sdk/python/tests/conftest.py
    with database_connection.get_session() as session:

        n_clients = 2
        n_sites = 3
        n_generations = 100

        # Clients
        clients = []
        for i in range(n_clients):
            client = ClientSQL(
                client_name=f"testclient_{i}",
                created_utc=datetime.now(timezone.utc),
            )
            session.add(client)
            clients.append(client)

        session.commit()

        # Sites
        sites = []
        for i in range(n_sites):
            site = SiteSQL(
                client_uuid=clients[i % n_clients].client_uuid,
                client_site_id=i + 1,
                latitude=51,
                longitude=3,
                capacity_kw=4,
                created_utc=datetime.now(timezone.utc),
                ml_id=i,
            )
            session.add(site)
            sites.append(site)

        session.commit()

        # Generation
        # Start time will be up to 2022-01-01 11:50, so test should run from then.
        start_times = [
            datetime(2022, 1, 1, 11, 50) - timedelta(minutes=x) for x in range(n_generations)
        ]

        for site in sites:
            for i in range(n_generations):
                generation = GenerationSQL(
                    site_uuid=site.site_uuid,
                    generation_power_kw=i,
                    start_utc=start_times[i],
                    end_utc=start_times[i] + timedelta(minutes=5),
                )
                session.add(generation)

        session.commit()

        for i in range(4):
            status = StatusSQL(
                status="OK",
                message=f"Status {i}",
            )
            session.add(status)

        session.commit()
