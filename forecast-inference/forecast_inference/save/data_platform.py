"""Data Platform operations: location management, forecaster lifecycle, and forecast saving."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pandas as pd
from betterproto.lib.google.protobuf import Struct, Value
from dp_sdk.ocf import dp
from grpclib.client import Channel

log = logging.getLogger(__name__)

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


async def fetch_dp_location_map(client: DataPlatformClient) -> dict[str, str]:
    """Fetch all locations from the Data Platform.

    Returns a name → UUID map. Pre-fetching avoids separate list_locations calls
    for every forecast save.
    """
    resp = await client.list_locations(dp.ListLocationsRequest())
    return {loc.location_name: loc.location_uuid for loc in resp.locations}


async def build_dp_location_map() -> dict[str, str]:
    """Async wrapper: open a channel, fetch the location map, close."""
    async with get_dataplatform_client() as client:
        return await fetch_dp_location_map(client)


async def resolve_target_uuid(
    client: DataPlatformClient,
    client_location_name: str,
    location_map: dict[str, str] | None = None,
) -> str | None:
    """Look up the DP location UUID by name.

    If a pre-fetched *location_map* (name → UUID) is supplied it is used directly,
    avoiding an extra list_locations gRPC call. When None, the map is fetched on-demand.

    Returns the UUID string if found, or None if the location does not exist yet.
    """
    client_location_name = client_location_name.replace("-", "_").lower()
    if location_map is None:
        resp = await client.list_locations(dp.ListLocationsRequest())
        location_map = {loc.location_name: loc.location_uuid for loc in resp.locations}

    if client_location_name in location_map:
        target_uuid = location_map[client_location_name]
        log.info(f"Mapped client location '{client_location_name}' to DP UUID {target_uuid}")
        return target_uuid

    log.warning(f"DP location '{client_location_name}' not found — will create it.")
    return None


async def get_location_capacity(
    client: DataPlatformClient,
    target_uuid_str: str,
    energy_source: dp.EnergySource = dp.EnergySource.SOLAR,
) -> int:
    """Fetch effective capacity (watts) for an existing DP location."""
    location = await client.get_location(
        dp.GetLocationRequest(
            location_uuid=target_uuid_str,
            energy_source=energy_source,
            include_geometry=False,
        ),
    )
    return location.effective_capacity_watts


async def create_new_location(
    client: DataPlatformClient,
    client_location_name: str,
    capacity_kw: float,
    latitude: float | None,
    longitude: float | None,
    init_time_utc: datetime,
    location_type: dp.LocationType = dp.LocationType.SITE,
    energy_source: dp.EnergySource = dp.EnergySource.SOLAR,
) -> str:
    """Create a new location in the Data Platform and return its UUID."""
    log.warning(
        f"Location {client_location_name} (type={location_type.name}) not found. "
        "Attempting to create it...",
    )

    lon, lat = longitude or 0.0, latitude or 0.0
    wkt = f"POINT ({lon} {lat})"
    capacity_watts = int(capacity_kw * 1000)
    client_location_name = client_location_name.lower().replace("-", "_")

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
        raise


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

    forecast_values: list[dp.CreateForecastRequestForecastValue] = []
    for row in rows:
        horizon_mins = int(row.get("horizon_minutes", 0))
        if not horizon_mins:
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
    capacity_kw: float | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    location_map: dict[str, str] | None = None,
) -> None:
    """Save forecast to the Data Platform."""
    if not rows:
        log.warning("forecast rows list is empty")
        return

    if not client_location_name:
        log.error("client_location_name is None/empty — cannot save")
        raise ValueError("client_location_name is required to save to the Data Platform")

    client_location_name = client_location_name.lower()
    energy_source = dp.EnergySource.SOLAR  # UK PV only

    if isinstance(init_time_utc, pd.Timestamp):
        init_time_utc_dt: datetime = (
            init_time_utc.tz_localize("UTC") if init_time_utc.tz is None
            else init_time_utc.tz_convert("UTC")
        ).to_pydatetime()
    else:
        init_time_utc_dt = (
            init_time_utc.replace(tzinfo=UTC) if init_time_utc.tzinfo is None
            else init_time_utc.astimezone(UTC)
        )

    log.info(
        "Starting DP save | "
        f"location={client_location_name!r}  model={model_tag!r}  "
        f"init_time={init_time_utc_dt}  capacity_kw={capacity_kw}  "
        f"rows={len(rows)}  "
        f"location_map_size={len(location_map) if location_map else None}",
    )

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
            capacity_kw or 0.0,
            latitude,
            longitude,
            init_time_utc_dt,
            location_type=dp.LocationType.SITE,
            energy_source=energy_source,
        )
        log.info(f"created location uuid={target_uuid_str}")
    else:
        log.info(f"location already exists uuid={target_uuid_str}")

    capacity_watts = await get_location_capacity(
        client=client,
        target_uuid_str=target_uuid_str,
        energy_source=energy_source,
    )
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


async def save_to_dataplatform(
    rows: list[dict],
    client_location_name: str,
    model_tag: str,
    init_time_utc: datetime,
    capacity_kw: float | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    location_map: dict[str, str] | None = None,
) -> None:
    """Save forecast to the Data Platform (opens its own gRPC channel)."""
    async with get_dataplatform_client() as client:
        await save_forecast_to_dataplatform(
            rows=rows,
            client_location_name=client_location_name,
            model_tag=model_tag,
            init_time_utc=init_time_utc,
            client=client,
            capacity_kw=capacity_kw,
            latitude=latitude,
            longitude=longitude,
            location_map=location_map,
        )
