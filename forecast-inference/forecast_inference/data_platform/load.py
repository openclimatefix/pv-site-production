"""Reading generation and location data from the Data Platform.

Fully Data Platform-native: no database is involved anywhere in this module. Sites are
identified by their Data Platform `location_uuid`.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import UTC, timedelta

import pandas as pd
from ocf import dp
from psp.typings import Timestamp

from forecast_inference.data_platform.client import (
    DataPlatformClient,
    LocationSummary,
    fetch_dp_location_map,
    get_dataplatform_client,
)

log = logging.getLogger(__name__)


def _ensure_timezone_aware(ts: Timestamp) -> Timestamp:
    """Ensure a datetime is timezone-aware UTC, as required by the Data Platform API."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


async def fetch_generation_for_one_location_from_dp(
    client: DataPlatformClient,
    location_uuid: str,
    start: Timestamp,
    end: Timestamp,
    observer_name: str | None = None,
) -> list[tuple[Timestamp, float]]:
    """Fetch generation (observation) data from the Data Platform for a single location."""
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
            location_uuid=location_uuid,
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


def _get_metadata_float(summary: LocationSummary, key: str) -> float | None:
    """Read a numeric value out of a DP location's generic `metadata` Struct, if present."""
    value = summary.metadata.fields.get(key)
    if value is None:
        return None
    return value.number_value


def _get_metadata_string(summary: LocationSummary, key: str) -> str | None:
    """Read a string value out of a DP location's generic `metadata` Struct, if present."""
    value = summary.metadata.fields.get(key)
    if value is None:
        return None
    return value.string_value


def build_location_metadata(summary: LocationSummary) -> dict:
    """Build a location metadata dict (latitude, longitude, capacity_kw, tilt, orientation).

    `tilt`/`orientation` have no native DP field, so they're read from the location's
    `metadata` Struct when present (see `pv-site-api`'s `create_location`/`update_location`,
    which write them there). When absent (not yet backfilled), the key is omitted.
    """
    location = {
        "latitude": summary.latlng.latitude,
        "longitude": summary.latlng.longitude,
        "capacity_kw": summary.effective_capacity_watts / 1000.0,
    }

    tilt = _get_metadata_float(summary, "tilt")
    if tilt is not None:
        location["tilt"] = tilt

    orientation = _get_metadata_float(summary, "orientation")
    if orientation is not None:
        location["orientation"] = orientation

    return location


def get_client_location_name(summary: LocationSummary) -> str:
    """Get a site's original (unsanitized) client-facing name.

    Falls back to the DP `location_name` (the sanitized version) when `client_location_name`
    hasn't been written into `metadata` for this location.
    """
    return _get_metadata_string(summary, "client_location_name") or summary.location_name


async def get_generation_from_dp(
    client: DataPlatformClient,
    summaries: list[LocationSummary],
    start_ts: Timestamp,
    end_ts: Timestamp,
) -> pd.DataFrame:
    """Fetch generation values for all given DP locations, as a dataframe.

    Returns a dataframe of (id, ts, power) records, one row per generation value found.
    `id` is the Data Platform's own `location_uuid`.
    """
    generations = await asyncio.gather(
        *[
            fetch_generation_for_one_location_from_dp(
                client, summary.location_uuid, start_ts, end_ts
            )
            for summary in summaries
        ]
    )

    records = [
        {"id": summary.location_uuid, "ts": t.replace(tzinfo=None), "power": power_kw}
        for summary, summary_generation in zip(summaries, generations)
        for t, power_kw in summary_generation
    ]
    return pd.DataFrame.from_records(records, columns=["id", "ts", "power"])


async def fetch_generation_and_locations_from_dp(
    location_uuids: list[str],
    start_ts: Timestamp | None,
    end_ts: Timestamp | None,
    loc_map_cache: dict[str, LocationSummary] | None,
) -> tuple[pd.DataFrame, dict[str, dict], dict[str, LocationSummary]]:
    """Fetch generation and location metadata for the given sites from the Data Platform.

    `loc_map_cache`, if provided, must be keyed by `location_uuid` and is reused instead of
    re-listing every DP location on every call (callers typically invoke this once per site,
    so re-fetching the full location list every time would multiply DP round-trips).

    Returns a tuple of (generation dataframe, location metadata dict keyed by `location_uuid`,
    the location map used) so the caller can cache the location map for subsequent calls.
    """
    if start_ts is None or end_ts is None:
        raise ValueError("Reading from the Data Platform requires both `start_ts` and `end_ts`")

    if loc_map_cache is None:
        log.info("Reading generation and locations from the Data Platform")
    else:
        log.debug("Reading generation and locations from the Data Platform (cached)")

    async with get_dataplatform_client() as client:
        if loc_map_cache is None:
            name_map = await fetch_dp_location_map(client)
            loc_map_cache = {summary.location_uuid: summary for summary in name_map.values()}

        summaries = [loc_map_cache[uuid] for uuid in location_uuids if uuid in loc_map_cache]

        missing = set(location_uuids) - {s.location_uuid for s in summaries}
        if missing:
            log.warning(f"{len(missing)} location(s) not found in the Data Platform: {missing}")

        df = await get_generation_from_dp(client, summaries, start_ts, end_ts)

    locations = {
        summary.location_uuid: build_location_metadata(summary) for summary in summaries
    }

    return df, locations, loc_map_cache
