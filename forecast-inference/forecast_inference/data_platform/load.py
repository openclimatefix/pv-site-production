"""Reading generation and location data from the Data Platform."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import UTC, timedelta

import pandas as pd
from dp_sdk.ocf import dp
from psp.typings import Timestamp
from pvsite_datamodel.sqlmodels import LocationSQL

from forecast_inference.data_platform.client import (
    DataPlatformClient,
    LocationSummary,
    _sanitize,
    fetch_dp_location_map,
    get_dataplatform_client,
)

log = logging.getLogger(__name__)


def _ensure_timezone_aware(ts: Timestamp) -> Timestamp:
    """Ensure a datetime is timezone-aware UTC, as required by the Data Platform API."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


async def fetch_generation_from_dp(
    client: DataPlatformClient,
    loc_map: dict[str, LocationSummary],
    site: LocationSQL,
    start: Timestamp,
    end: Timestamp,
    observer_name: str | None = None,
) -> list[tuple[Timestamp, float]]:
    """Fetch generation (observation) data from the Data Platform for a single site."""
    if not site.client_location_name:
        log.warning(f"Site {site.location_uuid} has no client_location_name, skipping DP")
        return []

    name = _sanitize(site.client_location_name)
    summary = loc_map.get(name)
    if not summary:
        log.warning(f"Site {name!r} not found in Data Platform")
        return []

    actual_observer_name = observer_name or os.getenv("OBSERVER_NAME", "pv_site_api")

    start_dt = _ensure_timezone_aware(start)
    end_dt = _ensure_timezone_aware(end)
    max_window = timedelta(days=7)

    chunks = []
    curr_start = start_dt
    while curr_start < end_dt:
        curr_end = min(curr_start + max_window, end_dt)
        chunks.append((curr_start, curr_end))
        curr_start = curr_end

    data = []
    for c_start, c_end in chunks:
        req = dp.GetObservationsAsTimeseriesRequest(
            location_uuid=summary.location_uuid,
            energy_source=dp.EnergySource.SOLAR,
            observer_name=actual_observer_name,
            time_window=dp.TimeWindow(
                start_timestamp_utc=c_start,
                end_timestamp_utc=c_end,
            ),
        )
        res = await client.get_observations_as_timeseries(req)

        if not res.values:
            continue

        cap_w = res.values[0].effective_capacity_watts
        for val in res.values:
            t = val.timestamp_utc
            power_kw = (val.value_fraction * cap_w) / 1000.0
            data.append((t, power_kw))

    return data


def fetch_location_from_dp(
    loc_map: dict[str, LocationSummary],
    site: LocationSQL,
) -> dict | None:
    """Look up a site's location metadata (latitude, longitude, capacity_kw) in the DP."""
    if not site.client_location_name:
        return None

    name = _sanitize(site.client_location_name)
    summary = loc_map.get(name)
    if not summary:
        return None

    return {
        "latitude": summary.latlng.latitude,
        "longitude": summary.latlng.longitude,
        "capacity_kw": summary.effective_capacity_watts / 1000.0,
    }


async def get_generation_from_dp(
    client: DataPlatformClient,
    loc_map: dict[str, LocationSummary],
    sites: list[LocationSQL],
    start_ts: Timestamp,
    end_ts: Timestamp,
) -> pd.DataFrame:
    """Fetch generation values for all sites from the Data Platform, as a dataframe.

    Returns a dataframe of (id, ts, power) records, one row per generation value found.
    """
    generations = await asyncio.gather(
        *[fetch_generation_from_dp(client, loc_map, site, start_ts, end_ts) for site in sites]
    )

    records = [
        {"id": str(site.location_uuid), "ts": t.replace(tzinfo=None), "power": power_kw}
        for site, site_generation in zip(sites, generations)
        for t, power_kw in site_generation
    ]
    return pd.DataFrame.from_records(records, columns=["id", "ts", "power"])


def get_locations_from_dp(
    loc_map: dict[str, LocationSummary],
    sites: list[LocationSQL],
) -> dict[str, dict]:
    """Look up location metadata (latitude, longitude, capacity_kw) for all sites from the DP.

    Purely local dict lookups against an already-fetched `loc_map` — no network calls.

    Returns a dict of location_uuid (as in our database) -> {latitude, longitude, capacity_kw},
    for sites that were found in the Data Platform.
    """
    return {
        str(site.location_uuid): location
        for site in sites
        if (location := fetch_location_from_dp(loc_map, site)) is not None
    }


async def fetch_generation_and_locations_from_dp(
    sites: list[LocationSQL],
    start_ts: Timestamp | None,
    end_ts: Timestamp | None,
    loc_map_cache: dict[str, LocationSummary] | None,
) -> tuple[pd.DataFrame, dict[str, dict], dict[str, LocationSummary]]:
    """Fetch generation and location metadata for all sites from the Data Platform.

    If `loc_map_cache` is provided, it is reused instead of re-listing every location in
    the Data Platform (callers typically invoke this once per site, so re-fetching the
    full location list every time would multiply DP round-trips by the number of sites).

    Returns a tuple of (generation dataframe, location metadata dict, the location map
    used) so the caller can cache the location map for subsequent calls.
    """
    if start_ts is None or end_ts is None:
        raise ValueError("Reading from the Data Platform requires both `start_ts` and `end_ts`")

    if loc_map_cache is None:
        log.info("Reading generation and locations from the Data Platform")
    else:
        log.debug("Reading generation and locations from the Data Platform (cached)")

    async with get_dataplatform_client() as client:
        loc_map = loc_map_cache if loc_map_cache is not None else await fetch_dp_location_map(
            client
        )
        df = await get_generation_from_dp(client, loc_map, sites, start_ts, end_ts)

    locations = get_locations_from_dp(loc_map, sites)

    return df, locations, loc_map
