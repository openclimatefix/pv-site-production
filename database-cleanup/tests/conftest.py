import os

import pytest
import sqlalchemy as sa
from pvsite_datamodel.sqlmodels import Base, LocationSQL
from sqlalchemy.orm import Session
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session", autouse=True)
def engine():
    with PostgresContainer("postgres:15.2") as postgres:
        url = postgres.get_connection_url()
        os.environ["DB_URL"] = url

        engine = sa.create_engine(url)

        Base.metadata.create_all(engine)

        yield engine

        engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def fill_db(engine):
    # Fill in some clients and sites
    num_clients = 2
    num_sites = 3
    with Session(engine) as session:
        for i in range(num_clients):
            session.commit()
            for j in range(num_sites):
                session.add(LocationSQL(ml_id=j + num_sites * i))
                session.commit()


@pytest.fixture
def session(engine):
    with Session(engine) as session:
        yield session
