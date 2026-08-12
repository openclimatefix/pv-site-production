"""Data Platform operations: location management, forecaster lifecycle, and forecast saving."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pandas as pd
from betterproto.lib.google.protobuf import Struct, Value
from dp_sdk.ocf import dp
from grpclib.client import Channel
from psp.typings import Timestamp
from pvsite_datamodel.sqlmodels import LocationSQL

log = logging.getLogger(__name__)


def _sanitize(name: str) -> str:
    """Sanitize location name to contain only DP-supported characters."""
    return re.sub(r"[^a-z0-9_|]", "_", name.lower())


# Keep this static so the adjuster and API keep working even if the app version changes.
dp_forecaster_version = "1.4.0"

# Type alias for the Data Platform client stub
DataPlatformClient = dp.DataPlatformDataServiceStub


@contextlib.asynccontextmanager
async def get_dataplatform_client() -> AsyncIterator[DataPlatformClient]:
    """Async context manager that opens a gRPC channel and yields a ready-to-use client.

    Host and port are read from DATA_PLATFORM_HOST / DATA_PLATFORM_PORT env vars
    (defaulting to localhost:50051).
    """
    channel = Channel(
        host=os.getenv("DATA_PLATFORM_HOST", "localhost"),
        port=int(os.getenv("DATA_PLATFORM_PORT", "50051")),
    )
    try:
        yield dp.DataPlatformDataServiceStub(channel)
    finally:
        channel.close()


# Type returned per location from list_locations — includes uuid and capacity.
LocationSummary = dp.ListLocationsResponseLocationSummary


async def fetch_dp_location_map(
    client: DataPlatformClient,
    location_type: dp.LocationType = dp.LocationType.SITE,
) -> dict[str, LocationSummary]:
    """Fetch locations from the Data Platform filtered by location type.

    Returns a name → LocationSummary map. The summary includes both the UUID and
    effective_capacity_watts, so callers avoid a second get_location gRPC call.
    Pre-fetching once avoids separate list_locations calls for every forecast save.
    """
    resp = await client.list_locations(
        dp.ListLocationsRequest(location_type_filter=[location_type])
    )
    return {loc.location_name: loc for loc in resp.locations}


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


async def resolve_target_uuid(
    client: DataPlatformClient,
    client_location_name: str,
    location_map: dict[str, LocationSummary] | None = None,
) -> str | None:
    """Look up the DP location UUID by name.

    If a pre-fetched *location_map* (name → LocationSummary) is supplied it is used
    directly, avoiding an extra list_locations gRPC call. When None, the map is fetched
    on-demand.

    Returns the UUID string if found, or None if the location does not exist yet.
    """
    client_location_name = _sanitize(client_location_name)
    if location_map is None:
        location_map = await fetch_dp_location_map(client)

    summary = location_map.get(client_location_name)
    if summary:
        log.info(
            f"Mapped client location '{client_location_name}' to DP UUID {summary.location_uuid}"
        )
        return summary.location_uuid

    log.warning(f"DP location '{client_location_name}' not found — will create it.")
    return None


async def create_new_location(
    client: DataPlatformClient,
    client_location_name: str,
    capacity_kw: float,
    latitude: float,
    longitude: float,
    init_time_utc: datetime,
    location_type: dp.LocationType = dp.LocationType.SITE,
    energy_source: dp.EnergySource = dp.EnergySource.SOLAR,
) -> str:
    """Create a new location in the Data Platform and return its UUID."""
    log.warning(
        f"Location {client_location_name} (type={location_type.name}) not found. "
        "Attempting to create it...",
    )

    wkt = f"POINT ({longitude} {latitude})"
    capacity_watts = int(capacity_kw * 1000)
    client_location_name = _sanitize(client_location_name)

    try:
        create_req = dp.CreateLocationRequest(
            location_name=client_location_name,
            energy_source=energy_source,
            geometry_wkt=wkt,
            effective_capacity_watts=capacity_watts,
            location_type=location_type,
            valid_from_utc=init_time_utc - timedelta(days=7),
        )
        create_resp = await client.create_location(create_req)
        log.info(f"Created new location {create_resp.location_uuid} for '{client_location_name}'")
        return create_resp.location_uuid
    except Exception as create_error:
        log.error(f"Failed to create location: {create_error}")
        raise RuntimeError(
            f"Failed to create location for '{client_location_name}': {create_error}"
        ) from create_error


async def create_forecaster_if_not_exists(
    client: DataPlatformClient,
    model_tag: str,
) -> dp.Forecaster:
    """Create the current forecaster if it does not exist."""
    forecaster_name = model_tag.replace("-", "_").lower()

    list_forecasters_request = dp.ListForecastersRequest(
        forecaster_names_filter=[forecaster_name],
    )
    try:
        list_forecasters_response = await client.list_forecasters(list_forecasters_request)
        existing_forecasters = list_forecasters_response.forecasters
    except Exception as e:
        if "NOT_FOUND" in str(e) or "No forecasters found" in str(e):
            existing_forecasters = []
        else:
            raise

    if len(existing_forecasters) > 0:
        filtered_forecasters = [
            f
            for f in existing_forecasters
            if f.forecaster_version == dp_forecaster_version
        ]
        if len(filtered_forecasters) == 1:
            return filtered_forecasters[0]
        else:
            update_forecaster_request = dp.UpdateForecasterRequest(
                name=forecaster_name,
                new_version=dp_forecaster_version,
            )
            update_forecaster_response = await client.update_forecaster(update_forecaster_request)
            return update_forecaster_response.forecaster
    else:
        create_forecaster_request = dp.CreateForecasterRequest(
            name=forecaster_name,
            version=dp_forecaster_version,
        )
        create_forecaster_response = await client.create_forecaster(create_forecaster_request)
        return create_forecaster_response.forecaster


def prepare_forecast_values(
    rows: list[dict],
    init_time_utc: datetime,
    capacity_watts: int,
) -> list[dp.CreateForecastRequestForecastValue]:
    """Convert forecast rows to a list of DP forecast value objects.

    Args:
        rows: List of dicts with keys start_utc, end_utc, forecast_power_kw, horizon_minutes.
        init_time_utc: Forecast initialisation time.
        capacity_watts: Site capacity in watts used to calculate the power fraction.
    """
    init_ts = pd.Timestamp(init_time_utc)
    if init_ts.tz is None:
        init_ts = init_ts.tz_localize("UTC")
    init_ts = init_ts.floor("15min")

    forecast_values: list[dp.CreateForecastRequestForecastValue] = []
    for row in rows:
        if "horizon_minutes" in row and row["horizon_minutes"] is not None:
            horizon_mins = int(row["horizon_minutes"])
        else:
            start_ts = pd.Timestamp(row["start_utc"])
            if start_ts.tz is None:
                start_ts = start_ts.tz_localize("UTC")
            horizon_mins = int((start_ts - init_ts).total_seconds() / 60)

        p50_fraction = max(0.0, min(1.0, (row["forecast_power_kw"] * 1000) / capacity_watts))

        forecast_values.append(
            dp.CreateForecastRequestForecastValue(
                horizon_mins=horizon_mins,
                p50_fraction=p50_fraction,
            ),
        )

    return forecast_values


async def save_forecast_to_dataplatform(
    rows: list[dict],
    client_location_name: str,
    model_tag: str,
    init_time_utc: datetime,
    client: DataPlatformClient,
    capacity_kw: float,
    latitude: float,
    longitude: float,
    location_map: dict[str, LocationSummary] | None = None,
) -> None:
    """Save forecast to the Data Platform."""
    if not rows:
        log.warning("forecast rows list is empty")
        return

    if not client_location_name:
        log.error("client_location_name is None/empty — cannot save")
        raise ValueError("client_location_name is required to save to the Data Platform")

    client_location_name = _sanitize(client_location_name)
    energy_source = dp.EnergySource.SOLAR  # UK PV only

    init_ts = pd.Timestamp(init_time_utc)
    if init_ts.tz is None:
        init_ts = init_ts.tz_localize("UTC")
    else:
        init_ts = init_ts.tz_convert("UTC")
    init_time_utc_dt: datetime = init_ts.floor("15min").to_pydatetime()


    log.info(
        "Starting DP save | "
        f"location={client_location_name!r}  model={model_tag!r}  "
        f"init_time={init_time_utc_dt}  capacity_kw={capacity_kw}  "
        f"rows={len(rows)}  "
        f"location_map_size={len(location_map) if location_map else None}",
    )

    if location_map is None:
        location_map = await fetch_dp_location_map(client)

    target_uuid_str, forecaster = await asyncio.gather(
        resolve_target_uuid(client, client_location_name, location_map),
        create_forecaster_if_not_exists(client=client, model_tag=model_tag),
    )
    log.info(
        f"uuid={target_uuid_str}  "
        f"forecaster={forecaster.forecaster_name!r} v{forecaster.forecaster_version}",
    )

    if target_uuid_str is None:
        log.warning(
            f"location {client_location_name!r} not in DP — creating it  "
            f"(capacity_kw={capacity_kw}, lat={latitude}, lon={longitude})",
        )
        target_uuid_str = await create_new_location(
            client,
            client_location_name,
            capacity_kw,
            latitude,
            longitude,
            init_time_utc_dt,
            location_type=dp.LocationType.SITE,
            energy_source=energy_source,
        )
        log.info(f"created location uuid={target_uuid_str}")
        capacity_watts = int(capacity_kw * 1000)
    else:
        log.info(f"location already exists uuid={target_uuid_str}")
        capacity_watts = location_map[client_location_name].effective_capacity_watts

    log.info(f"capacity_watts={capacity_watts:,}  ({capacity_watts / 1000:,.1f} kW)")

    if capacity_watts == 0:
        log.error(
            f"location {target_uuid_str} has 0 W capacity — "
            "no forecast values can be expressed as fractions; skipping save",
        )
        return

    forecast_values = prepare_forecast_values(rows, init_time_utc_dt, capacity_watts)
    log.info(f"prepared {len(forecast_values)} forecast value(s)")

    if not forecast_values:
        log.warning("no forecast values after preparation")
        return

    base_request = dp.CreateForecastRequest(
        forecaster=forecaster,
        location_uuid=target_uuid_str,
        energy_source=energy_source,
        init_time_utc=init_time_utc_dt,
        values=forecast_values,
        metadata=Struct(fields={"app_version": Value(string_value=dp_forecaster_version)}),
    )
    log.info(
        f"submitting forecast  "
        f"forecaster={forecaster.forecaster_name!r}  "
        f"location={target_uuid_str}  values={len(forecast_values)}",
    )

    await client.create_forecast(base_request)
    log.info(f"Save complete for location={client_location_name!r}")


