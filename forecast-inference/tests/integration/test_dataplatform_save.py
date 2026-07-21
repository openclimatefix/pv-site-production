"""Integration-style tests for the Data Platform save path.

Structure mirrors site-forecast-app/tests/integration/test_dataplatform_save.py.
Spins up a real DP Docker container via the ``dp_address`` fixture and verifies
the full save flow end-to-end.

Unit tests for sanitization and prepare_forecast_values live in
tests/unit/save/test_data_platform.py (no Docker required).
"""

import asyncio
import datetime as dt
import os
import re

import pytest
from dp_sdk.ocf import dp
from freezegun.api import real_datetime

from forecast_inference.save.data_platform import (
    create_new_location,
    get_dataplatform_client,
    save_to_dataplatform,
)


# ---------------------------------------------------------------------------
# Helpers (same pattern as site-forecast-app)
# ---------------------------------------------------------------------------


def _sanitize(name: str) -> str:
    """Replicate the sanitization logic from data_platform.py."""
    return re.sub(r"[^a-z0-9_|]", "_", name.lower())


async def _setup_test_location_in_dp(
    host: str,
    port: int,
    site_name: str,
    capacity_kw: float,
    latitude: float,
    longitude: float,
) -> str:
    """Get or create a SITE location in the DP, returning its UUID.

    Checks for an existing location first so repeated test runs against the
    same container do not fail on duplicate name errors.
    """
    os.environ["DATA_PLATFORM_HOST"] = host
    os.environ["DATA_PLATFORM_PORT"] = str(port)

    async with get_dataplatform_client() as client:
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


async def _verify_forecast_in_dp(
    host: str,
    port: int,
    location_uuid: str,
    init_time: dt.datetime,
    forecast_end_time: dt.datetime,
    forecaster_name: str = "pv_site_production",
) -> dp.GetForecastAsTimeseriesResponse:
    """Query the DP and return the stored forecast timeseries."""
    os.environ["DATA_PLATFORM_HOST"] = host
    os.environ["DATA_PLATFORM_PORT"] = str(port)

    async with get_dataplatform_client() as client:
        list_resp = await client.list_forecasters(
            dp.ListForecastersRequest(forecaster_names_filter=[forecaster_name])
        )
        assert len(list_resp.forecasters) > 0, (
            f"No forecaster '{forecaster_name}' found in DP"
        )
        forecaster = list_resp.forecasters[0]

        return await client.get_forecast_as_timeseries(
            dp.GetForecastAsTimeseriesRequest(
                location_uuid=location_uuid,
                energy_source=dp.EnergySource.SOLAR,
                time_window=dp.TimeWindow(
                    start_timestamp_utc=init_time - dt.timedelta(minutes=1),
                    end_timestamp_utc=forecast_end_time + dt.timedelta(minutes=1),
                ),
                forecaster=forecaster,
            )
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def forecast_rows():
    """10 forecast rows at 15-minute intervals from a fixed UTC time.

    Uses freezegun.api.real_datetime to produce genuine datetime objects even
    when the session-scoped freeze_time autouse fixture is active (freeze_time
    monkeypatches dt.datetime, but real_datetime bypasses it).
    """
    n = 10
    step = 15
    init_utc = real_datetime(2020, 1, 1, 12, 0, 0, tzinfo=dt.UTC)
    return [
        {
            "start_utc": init_utc + dt.timedelta(minutes=i * step),
            "end_utc": init_utc + dt.timedelta(minutes=(i + 1) * step),
            "forecast_power_kw": float(i),
            "horizon_minutes": i * step,
        }
        for i in range(n)
    ]


@pytest.fixture()
def site_meta_clean():
    """Site metadata with a plain alphanumeric name (no sanitization needed)."""
    return {
        "client_location_name": "test_uk_site",
        "capacity_kw": 4.0,
        "latitude": 51.5,
        "longitude": -1.8,
    }


@pytest.fixture()
def site_meta_special_chars():
    """Site metadata whose name contains chars that require sanitization."""
    return {
        "client_location_name": "pvoutput.org-12345 UK",
        "capacity_kw": 3.0,
        "latitude": 50.3,
        "longitude": -4.8,
    }


# ---------------------------------------------------------------------------
# Integration tests (require Docker)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_save_forecast_to_dataplatform_integration(
    monkeypatch,
    dp_address,
    forecast_rows,
    site_meta_clean,
):
    """End-to-end: create a DP location, save a forecast, verify it is stored.

    Mirrors test_save_forecast_integration in site-forecast-app.
    """
    host, port = dp_address
    monkeypatch.setenv("SAVE_TO_DATA_PLATFORM", "true")
    monkeypatch.setenv("DATA_PLATFORM_HOST", host)
    monkeypatch.setenv("DATA_PLATFORM_PORT", str(port))

    # 1. Create location in DP
    dp_location_uuid = asyncio.run(
        _setup_test_location_in_dp(
            host, port,
            site_meta_clean["client_location_name"],
            site_meta_clean["capacity_kw"],
            site_meta_clean["latitude"],
            site_meta_clean["longitude"],
        )
    )

    # 2. Save forecast via our function
    init_time = forecast_rows[0]["start_utc"]
    asyncio.run(
        save_to_dataplatform(
            rows=forecast_rows,
            client_location_name=site_meta_clean["client_location_name"],
            model_tag="pv-site-production",
            init_time_utc=init_time,
            capacity_kw=site_meta_clean["capacity_kw"],
            latitude=site_meta_clean["latitude"],
            longitude=site_meta_clean["longitude"],
        )
    )

    # 3. Verify in DP
    forecast_end = forecast_rows[-1]["end_utc"]
    resp = asyncio.run(
        _verify_forecast_in_dp(host, port, dp_location_uuid, init_time, forecast_end)
    )
    assert len(resp.values) == len(forecast_rows)


@pytest.mark.integration
def test_save_forecast_special_chars_location_name(
    monkeypatch,
    dp_address,
    forecast_rows,
    site_meta_special_chars,
):
    """Location names with dots, dashes, spaces are sanitized before DP create.

    Verifies the re.sub sanitization works end-to-end against a real DP container.
    """
    host, port = dp_address
    monkeypatch.setenv("DATA_PLATFORM_HOST", host)
    monkeypatch.setenv("DATA_PLATFORM_PORT", str(port))

    init_time = forecast_rows[0]["start_utc"]

    # Should not raise — the sanitizer handles all special chars
    asyncio.run(
        save_to_dataplatform(
            rows=forecast_rows,
            client_location_name=site_meta_special_chars["client_location_name"],
            model_tag="pv-site-production",
            init_time_utc=init_time,
            capacity_kw=site_meta_special_chars["capacity_kw"],
            latitude=site_meta_special_chars["latitude"],
            longitude=site_meta_special_chars["longitude"],
        )
    )

    # Confirm the sanitized name exists in DP
    async def _check():
        os.environ["DATA_PLATFORM_HOST"] = host
        os.environ["DATA_PLATFORM_PORT"] = str(port)
        async with get_dataplatform_client() as client:
            resp = await client.list_locations(dp.ListLocationsRequest())
            names = [loc.location_name for loc in resp.locations]
            sanitized = _sanitize(site_meta_special_chars["client_location_name"])
            assert sanitized in names, f"Expected '{sanitized}' in DP locations: {names}"

    asyncio.run(_check())


@pytest.mark.integration
def test_location_reused_on_second_save(
    monkeypatch,
    dp_address,
    forecast_rows,
    site_meta_clean,
):
    """Saving twice for the same site reuses the existing DP location (no duplicate create)."""
    host, port = dp_address
    monkeypatch.setenv("DATA_PLATFORM_HOST", host)
    monkeypatch.setenv("DATA_PLATFORM_PORT", str(port))

    init_time = forecast_rows[0]["start_utc"]

    # Pre-create location
    asyncio.run(
        _setup_test_location_in_dp(
            host, port,
            site_meta_clean["client_location_name"],
            site_meta_clean["capacity_kw"],
            site_meta_clean["latitude"],
            site_meta_clean["longitude"],
        )
    )

    # Save twice — second call must not raise even though location already exists
    for _ in range(2):
        asyncio.run(
            save_to_dataplatform(
                rows=forecast_rows,
                client_location_name=site_meta_clean["client_location_name"],
                model_tag="pv-site-production",
                init_time_utc=init_time,
                capacity_kw=site_meta_clean["capacity_kw"],
                latitude=site_meta_clean["latitude"],
                longitude=site_meta_clean["longitude"],
            )
        )

    # Confirm only one location with this name in DP
    async def _check():
        os.environ["DATA_PLATFORM_HOST"] = host
        os.environ["DATA_PLATFORM_PORT"] = str(port)
        async with get_dataplatform_client() as client:
            resp = await client.list_locations(dp.ListLocationsRequest())
            sanitized = _sanitize(site_meta_clean["client_location_name"])
            matching = [loc for loc in resp.locations if loc.location_name == sanitized]
            assert len(matching) == 1, f"Expected 1 location, found {len(matching)}"

    asyncio.run(_check())
