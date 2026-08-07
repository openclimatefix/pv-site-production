"""Integration-style tests for the Data Platform read path.

Structure mirrors tests/integration/test_dataplatform_save.py. Spins up a real DP
Docker container via the ``dp_address`` fixture, seeds a location + observations,
then verifies DbPvDataSource.get() (with READ_FROM_DATA_PLATFORM=true) returns the
right generation values and location metadata end-to-end.

Unit tests (mocked DP client, no Docker required) live in
tests/unit/data/test_pv_data_sources.py.
"""

import asyncio
import datetime as dt
import logging

import numpy as np
import pytest
from dp_sdk.ocf import dp
from freezegun.api import real_datetime
from pvsite_datamodel.sqlmodels import LocationSQL

from forecast_inference.data.pv_data_sources import DbPvDataSource
from forecast_inference.save.data_platform import (
    DataPlatformClient,
    _sanitize,
    create_new_location,
    get_dataplatform_client,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers (same pattern as test_dataplatform_save.py)
# ---------------------------------------------------------------------------


async def _setup_test_location_in_dp(
    client: DataPlatformClient,
    site_name: str,
    capacity_kw: float,
    latitude: float,
    longitude: float,
) -> str:
    """Get or create a SITE location in the DP, returning its UUID.

    Checks for an existing location first so repeated test runs against the
    same container do not fail on duplicate name errors.
    """
    list_resp = await client.list_locations(dp.ListLocationsRequest())
    sanitized = _sanitize(site_name)
    for loc in list_resp.locations:
        if loc.location_name == sanitized:
            return loc.location_uuid

    return await create_new_location(
        client,
        site_name,
        capacity_kw,
        latitude=latitude,
        longitude=longitude,
        init_time_utc=dt.datetime(2020, 1, 1, tzinfo=dt.UTC),
        location_type=dp.LocationType.SITE,
    )


async def _seed_dp_location_and_observations(
    host: str,
    port: int,
    site_name: str,
    capacity_kw: float,
    latitude: float,
    longitude: float,
    observer_name: str,
    values: list[tuple[dt.datetime, int]],
) -> str:
    """Create (or reuse) a DP location and push observation values for it."""
    async with get_dataplatform_client() as client:
        location_uuid = await _setup_test_location_in_dp(
            client,
            site_name,
            capacity_kw=capacity_kw,
            latitude=latitude,
            longitude=longitude,
        )

        try:
            await client.create_observer(dp.CreateObserverRequest(name=observer_name))
        except Exception as e:  # noqa: BLE001 - observer may already exist from a prior run
            log.debug(f"create_observer({observer_name!r}) failed, assuming it exists: {e}")

        await client.create_observations(
            dp.CreateObservationsRequest(
                location_uuid=location_uuid,
                energy_source=dp.EnergySource.SOLAR,
                observer_name=observer_name,
                values=[
                    dp.CreateObservationsRequestValue(timestamp_utc=ts, value_watts=watts)
                    for ts, watts in values
                ],
            )
        )

    return location_uuid


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def observation_window():
    """A 2-hour window of 15-minute observations, from a fixed UTC time.

    Uses freezegun.api.real_datetime to produce genuine datetime objects even
    when the session-scoped freeze_time autouse fixture is active.
    """
    start = real_datetime(2024, 6, 1, 10, 0, 0, tzinfo=dt.UTC)
    end = start + dt.timedelta(hours=2)
    # 2kW capacity, ramping from 0.1 to 0.8 fraction across the window.
    capacity_watts = 2000
    values = [
        (start + dt.timedelta(minutes=15 * i), int(capacity_watts * (0.1 + 0.1 * i)))
        for i in range(8)
    ]
    return start, end, capacity_watts, values


@pytest.fixture()
def db_site(database_connection):
    """A site in our database with a client_location_name, committed directly."""
    with database_connection.get_session() as session:
        site = LocationSQL(
            client_location_id=88888,
            client_location_name="dp_read_integration_site",
            latitude=51.0,
            longitude=-1.0,
            capacity_kw=4.0,
            tilt=30.0,
            orientation=180.0,
            ml_id=888,
            country="uk",
            active=True,
        )
        session.add(site)
        session.commit()
        session.refresh(site)
        yield site


# ---------------------------------------------------------------------------
# Integration tests (require Docker)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_read_generation_and_location_from_dataplatform_integration(
    monkeypatch,
    dp_address,
    database_connection,
    db_site,
    observation_window,
):
    """End-to-end: create a DP location, seed observations, and read them back
    through DbPvDataSource.get() with READ_FROM_DATA_PLATFORM=true.
    """
    host, port = dp_address
    monkeypatch.setenv("READ_FROM_DATA_PLATFORM", "true")
    monkeypatch.setenv("DATA_PLATFORM_HOST", host)
    monkeypatch.setenv("DATA_PLATFORM_PORT", str(port))
    monkeypatch.setenv("OBSERVER_NAME", "nednl")

    start, end, capacity_watts, values = observation_window
    # Deliberately different from db_site's DB-side lat/lon/capacity, so we can tell
    # the values really came from the Data Platform and not the database.
    dp_latitude, dp_longitude = 52.5, -2.5

    asyncio.run(
        _seed_dp_location_and_observations(
            host,
            port,
            db_site.client_location_name,
            capacity_kw=capacity_watts / 1000.0,
            latitude=dp_latitude,
            longitude=dp_longitude,
            observer_name="nednl",
            values=values,
        )
    )

    pv_data_source = DbPvDataSource(database_connection)
    result = pv_data_source.get(
        [str(db_site.location_uuid)],
        start_ts=start,
        end_ts=end,
    )

    site_uuid = str(db_site.location_uuid)

    # Generation values were read from the DP, not the (empty) GenerationSQL table.
    powers = result["power"].sel(id=site_uuid).values
    powers = powers[~np.isnan(powers)]
    expected_powers_kw = sorted(watts / 1000.0 for _, watts in values)
    assert sorted(powers) == pytest.approx(expected_powers_kw)

    # Location metadata came from the DP (capacity/lat/lon), not the database.
    assert result["capacity"].sel(id=site_uuid).item() == pytest.approx(capacity_watts / 1000.0)
    assert result["latitude"].sel(id=site_uuid).item() == pytest.approx(dp_latitude)
    assert result["longitude"].sel(id=site_uuid).item() == pytest.approx(dp_longitude)

    # tilt/orientation have no DP equivalent, so they still come from the database.
    assert result["tilt"].sel(id=site_uuid).item() == pytest.approx(30.0)
    assert result["orientation"].sel(id=site_uuid).item() == pytest.approx(180.0)
