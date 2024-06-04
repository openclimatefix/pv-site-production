"""
Fixtures for testing
"""

import os
from datetime import datetime, timedelta

import pytest
from freezegun import freeze_time
from pvsite_datamodel.connection import DatabaseConnection
from pvsite_datamodel.sqlmodels import Base, GenerationSQL, SiteSQL, StatusSQL
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session", autouse=True)
def now():
    """Set a deterministic "now" time for all the tests."""
    with freeze_time("2020-01-01 12:00"):
        yield datetime.utcnow()


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
def db_data(database_connection, now):
    """Fill in the database with some initial data."""

    with database_connection.get_session() as session:

        n_sites = 3
        n_generations = 100

        # Sites
        sites = []
        for i in range(n_sites):
            site = SiteSQL(
                client_site_id=i + 1,
                latitude=51.0, # type: ignore
                longitude=3, # type: ignore
                capacity_kw=4, # type: ignore
                ml_id=i,
            )
            session.add(site)
            sites.append(site)

        session.commit()

        # Generation
        start_times = [now - timedelta(minutes=x + 1) for x in range(n_generations)]

        for site in sites:
            for i in range(n_generations):
                generation = GenerationSQL(
                    site_uuid=site.site_uuid,
                    generation_power_kw=i, # type: ignore
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
