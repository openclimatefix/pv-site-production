""" Pytest fixtures for tests """
import uuid
from datetime import datetime, timezone

import pytest
from pvsite_datamodel import ClientSQL, GenerationSQL, SiteSQL
from pvsite_datamodel.sqlmodels import Base
from pvsite_datamodel.write.datetime_intervals import get_or_else_create_datetime_interval
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def engine():
    """Create postgres db engine."""
    with PostgresContainer("postgres:14.5") as postgres:
        # TODO need to setup postgres database with docker
        url = postgres.get_connection_url()
        engine = create_engine(url)
        Base.metadata.create_all(engine)

        yield engine


@pytest.fixture(scope="function", autouse=True)
def db_session(engine):
    """Returns an sqlalchemy session, and after the test tears down everything properly."""
    connection = engine.connect()
    # begin the nested transaction
    transaction = connection.begin()
    # use the connection with the already started transaction

    with Session(bind=connection) as session:
        yield session

        session.close()
        # roll back the broader transaction
        transaction.rollback()
        # put back the connection to the connection pool
        connection.close()
        session.flush()

    engine.dispose()


@pytest.fixture()
def sites(db_session):
    """Create two fake sites under two fake clients."""

    sites = []
    client_names = ["solar_sheffield_passiv", "pv_output"]
    for i, name in enumerate(client_names):
        client = ClientSQL(
            client_uuid=uuid.uuid4(),
            client_name=name,
            created_utc=datetime.now(timezone.utc),
        )
        site = SiteSQL(
            site_uuid=uuid.uuid4(),
            client_uuid=client.client_uuid,
            client_site_id=1,
            latitude=51,
            longitude=3,
            capacity_kw=4,
            created_utc=datetime.now(timezone.utc),
            updated_utc=datetime.now(timezone.utc),
            ml_id=i,
        )
        db_session.add(client)
        db_session.add(site)
        db_session.commit()

        sites.append(site)

    return sites


@pytest.fixture()
def generations(db_session, sites):
    """Create some fake generations.

    For system 1, pv yields from 4 to 10 at 5 minutes. Last one at 09.55
    For system 2: 1 pv yield at 04.00
    """

    all_generations = []

    # Site 1 has a generation every 5 minutes between 4 and 10
    for hour in range(4, 10):
        for minute in range(0, 60, 5):
            datetime_interval, _ = get_or_else_create_datetime_interval(
                session=db_session,
                start_time=datetime(2022, 1, 1, hour, minute, tzinfo=timezone.utc),
            )
            generation = GenerationSQL(
                generation_uuid=uuid.uuid4(),
                site_uuid=sites[0].site_uuid,
                power_kw=hour + minute / 100,
                datetime_interval_uuid=datetime_interval.datetime_interval_uuid,
            )
            all_generations.append(generation)

    # Site 2 has one generation at 4
    datetime_interval, _ = get_or_else_create_datetime_interval(
        session=db_session, start_time=datetime(2022, 1, 1, hour, minute, tzinfo=timezone.utc),
    )

    generation = GenerationSQL(
        generation_uuid=uuid.uuid4(),
        site_uuid=sites[1].site_uuid,
        power_kw=4,
        datetime_interval_uuid=datetime_interval.datetime_interval_uuid,
    )
    all_generations.append(generation)

    db_session.add_all(all_generations)
    db_session.commit()
