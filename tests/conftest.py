"""
Fixtures for testing
"""
import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models import (
    Base_PV,
    PVSystem,
    PVSystemSQL,
    PVYield,
    PVYieldSQL,
    pv_output,
    solar_sheffield_passiv,
)


@pytest.fixture
def db_connection():
    """Create data connection"""

    with tempfile.NamedTemporaryFile(suffix=".db") as temp:
        url = f"sqlite:///{temp.name}"
        os.environ["DB_URL_PV"] = url

        connection = DatabaseConnection(url=url, base=Base_PV, echo=False)

        for table in [PVYieldSQL, PVSystemSQL]:
            table.__table__.create(connection.engine)

        yield connection

        for table in [PVYieldSQL, PVSystemSQL]:
            table.__table__.drop(connection.engine)


@pytest.fixture(scope="function", autouse=True)
def db_session(db_connection):
    """Creates a new database session for a test."""

    connection = db_connection.engine.connect()
    t = connection.begin()

    with db_connection.get_session() as s:
        s.begin()
        yield s
        s.rollback()

    t.rollback()
    connection.close()


@pytest.fixture()
def pv_yields_and_systems(db_session):
    """Create pv yields and systems

    Pv systems: Two systems
    PV yields:
        For system 1, pv yields from 4 to 10 at 5 minutes. Last one at 09.55
        For system 2: 1 pv yield at 04.00
    """

    pv_system_sql_1: PVSystemSQL = PVSystem(
        pv_system_id=1,
        provider="pvoutput.org",
        status_interval_minutes=5,
        longitude=0,
        latitude=55,
        ml_capacity_kw=123,
    ).to_orm()
    pv_system_sql_1_ss: PVSystemSQL = PVSystem(
        pv_system_id=1,
        provider=solar_sheffield_passiv,
        status_interval_minutes=5,
        longitude=0,
        latitude=57,
        ml_capacity_kw=124,
    ).to_orm()
    pv_system_sql_2: PVSystemSQL = PVSystem(
        pv_system_id=2,
        provider="pvoutput.org",
        status_interval_minutes=5,
        longitude=0,
        latitude=56,
        ml_capacity_kw=124,
    ).to_orm()
    pv_system_sql_3: PVSystemSQL = PVSystem(
        pv_system_id=3,
        provider=pv_output,
        status_interval_minutes=5,
        longitude=0,
        latitude=57,
        ml_capacity_kw=124,
    ).to_orm()

    pv_yield_sqls = []
    for hour in range(4, 10):
        for minute in range(0, 60, 5):
            pv_yield_1 = PVYield(
                datetime_utc=datetime(2022, 1, 1, hour, minute, tzinfo=timezone.utc),
                solar_generation_kw=hour + minute / 100,
            ).to_orm()
            pv_yield_1.pv_system = pv_system_sql_1
            pv_yield_sqls.append(pv_yield_1)

            pv_yield_1_ss = PVYield(
                datetime_utc=datetime(2022, 1, 1, hour, minute),
                solar_generation_kw=hour + minute / 100,
            ).to_orm()
            pv_yield_1_ss.pv_system = pv_system_sql_1_ss
            pv_yield_sqls.append(pv_yield_1_ss)

    # pv system with gaps every 5 mins
    for minutes in [0, 10, 20, 30]:

        pv_yield_4 = PVYield(
            datetime_utc=datetime(2022, 1, 1, 4, tzinfo=timezone.utc) + timedelta(minutes=minutes),
            solar_generation_kw=4,
        ).to_orm()
        pv_yield_4.pv_system = pv_system_sql_2
        pv_yield_sqls.append(pv_yield_4)

    # add a system with only on pv yield
    pv_yield_5 = PVYield(
        datetime_utc=datetime(2022, 1, 1, 4, tzinfo=timezone.utc) + timedelta(minutes=minutes),
        solar_generation_kw=4,
    ).to_orm()
    pv_yield_5.pv_system = pv_system_sql_3
    pv_yield_sqls.append(pv_yield_5)

    # add to database
    db_session.add_all(pv_yield_sqls)
    db_session.add(pv_system_sql_1)
    db_session.add(pv_system_sql_2)

    db_session.commit()

    return {
        "pv_yields": pv_yield_sqls,
        "pv_systems": [pv_system_sql_1, pv_system_sql_2],
    }


# TODO add fake NWP data so tests get nwp data.