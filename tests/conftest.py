"""
Fixtures for testing
"""

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from pvsite_datamodel.sqlmodels import (
    Base,
    ClientSQL,
    ForecastSQL,
    GenerationSQL,
    LatestForecastValueSQL,
    SiteSQL,
    StatusSQL,
)
from pvsite_datamodel.write.datetime_intervals import (
    get_or_else_create_datetime_interval,
)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session", autouse=True)
def engine():
    """Database engine, this includes the table creation."""
    with PostgresContainer("postgres:14.5") as postgres:
        url = postgres.get_connection_url()
        os.environ["OCF_PV_DB_URL"] = url

        url = postgres.get_connection_url()
        engine = create_engine(url)
        Base.metadata.create_all(engine)

        yield engine

        engine.dispose()

        # Psql won't let us drop the tables if any sessions are still opened.
        # FIXME check if we still need it
        # sqlalchemy.orm.close_all_sessions()


@pytest.fixture(scope="session", autouse=True)
def Session(engine):
    return sessionmaker(bind=engine)


@pytest.fixture()
def db_session(Session):
    """Creates a new database session for a test.

    We automatically roll back whatever happens when the test completes.
    """

    with Session() as session:
        with session.begin():
            yield session
            session.rollback()


@pytest.fixture(scope="session", autouse=True)
def db_data(Session):
    """Fill in the database with some initial data."""

    # Those fixtures are inspired from:
    # https://github.com/openclimatefix/pvsite-datamodel/blob/ab7478b0a8c6f0bc06d998817622f7a80e6f6ca3/sdk/python/tests/conftest.py
    with Session() as session:

        n_clients = 2
        n_sites = 4
        n_generations = 100

        # Clients
        clients = []
        for i in range(n_clients):
            client = ClientSQL(
                client_uuid=uuid.uuid4(),
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
                site_uuid=uuid.uuid4(),
                client_uuid=clients[i % n_clients].client_uuid,
                client_site_id=i,
                latitude=51,
                longitude=3,
                capacity_kw=4,
                created_utc=datetime.now(timezone.utc),
                updated_utc=datetime.now(timezone.utc),
                ml_id=i,
            )
            session.add(site)
            sites.append(site)

        session.commit()

        # Generation
        start_times = [
            datetime(2022, 1, 1, 11, 58) + timedelta(minutes=x)
            for x in range(n_generations)
        ]

        for site in sites:
            for i in range(n_generations):
                datetime_interval, _ = get_or_else_create_datetime_interval(
                    session=session, start_time=start_times[i]
                )

                generation = GenerationSQL(
                    generation_uuid=uuid.uuid4(),
                    site_uuid=site.site_uuid,
                    power_kw=i,
                    datetime_interval_uuid=datetime_interval.datetime_interval_uuid,
                )
                session.add(generation)

        session.commit()

        # Forecast
        forecast_version: str = "0.0.0"

        for site in sites:
            forecast = ForecastSQL(
                forecast_uuid=uuid.uuid4(),
                site_uuid=site.site_uuid,
                forecast_version=forecast_version,
            )
            session.add(forecast)

            for i in range(n_generations):
                datetime_interval, _ = get_or_else_create_datetime_interval(
                    session=session, start_time=start_times[i]
                )

                latest_forecast_value: LatestForecastValueSQL = LatestForecastValueSQL(
                    latest_forecast_value_uuid=uuid.uuid4(),
                    datetime_interval_uuid=datetime_interval.datetime_interval_uuid,
                    forecast_generation_kw=i,
                    forecast_uuid=forecast.forecast_uuid,
                    site_uuid=site.site_uuid,
                    forecast_version=forecast_version,
                )

                session.add(latest_forecast_value)

        session.commit()

        # for time in start_times:
        #     get_or_else_create_datetime_interval(session=session, start_time=time)

        for i in range(4):
            status = StatusSQL(
                status_uuid=uuid.uuid4(),
                status="OK",
                message=f"Status {i}",
            )
            session.add(status)

        session.commit()
