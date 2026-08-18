"""Shared Data Platform client and location-listing infrastructure.

Used by both `data_platform.load` (reading generation/location data) and
`data_platform.save` (saving forecasts, managing locations/forecasters).
"""

from __future__ import annotations

import contextlib
import os
import re
from collections.abc import AsyncIterator

from grpclib.client import Channel
from ocf import dp

# Type alias for the Data Platform client stub
DataPlatformClient = dp.DataPlatformDataServiceStub

# Type returned per location from list_locations — includes uuid and capacity.
LocationSummary = dp.ListLocationsResponseLocationSummary


def _sanitize(name: str) -> str:
    """Sanitize location name to contain only DP-supported characters."""
    return re.sub(r"[^a-z0-9_|]", "_", name.lower())


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


async def fetch_dp_location_map(
    client: DataPlatformClient,
    location_type: dp.LocationType = dp.LocationType.SITE,
) -> dict[str, LocationSummary]:
    """Fetch locations from the Data Platform filtered by location type.

    Returns a name → LocationSummary map. The summary includes both the UUID and
    effective_capacity_watts, so callers avoid a second get_location gRPC call.
    Pre-fetching once avoids separate list_locations calls for every forecast save.
    """
    resp = await client.list_locations(dp.ListLocationsRequest(location_type_filter=location_type))
    return {loc.location_name: loc for loc in resp.locations}


async def fetch_dp_location_map_by_uuid(
    client: DataPlatformClient,
    location_type: dp.LocationType = dp.LocationType.SITE,
) -> dict[str, LocationSummary]:
    """Same as `fetch_dp_location_map`, but keyed by `location_uuid` instead of name.

    Used by callers that look sites up by id (e.g. `pv_id`) rather than by client name.
    """
    name_map = await fetch_dp_location_map(client, location_type)
    return {summary.location_uuid: summary for summary in name_map.values()}
