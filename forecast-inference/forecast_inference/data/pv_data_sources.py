"""
PV Data Source
"""

import asyncio
import concurrent.futures
import copy
import logging

import xarray as xr
from psp.data_sources.pv import PvDataSource, min_timestamp
from psp.typings import PvId, Timestamp

from forecast_inference.data_platform.client import (
    LocationSummary,
    fetch_dp_location_map,
    get_dataplatform_client,
)
from forecast_inference.data_platform.load import (
    fetch_generation_and_locations_from_dp,
    get_client_location_name,
)

META_KEYS = [
    "longitude",
    "latitude",
    "tilt",
    "orientation",
    "capacity_kw",
]

_log = logging.getLogger(__name__)


def _run_async(coro):
    """Run an async coroutine safely, even if an event loop is already running in the current thread."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            return executor.submit(asyncio.run, coro).result()
    else:
        return asyncio.run(coro)


class DataPlatformPvDataSource(PvDataSource):
    """PV Data Source that reads exclusively from the Data Platform — no database involved.

    `pv_id` is the Data Platform's own `location_uuid`, not a site database uuid. `tilt` and
    `orientation` have no native Data Platform field; they're read from a location's `metadata`
    Struct (see `pv-site-api`'s `create_location`/`update_location`, which write them there).
    A site missing tilt/orientation (or any other required metadata) in the Data Platform
    causes `.get()` to raise, rather than silently predicting on `NaN` inputs.
    """

    def __init__(self):
        """Constructor"""
        self._max_ts: Timestamp | None = None
        # Cached across calls, keyed by DP location_uuid, so a run over many sites doesn't
        # re-list every DP location on every single `.get()` call (which happens once per site).
        self._dp_location_map: dict[str, LocationSummary] | None = None

    def _get_location_map(self) -> dict[str, LocationSummary]:
        """Fetch (and cache) all Data Platform SITE locations, keyed by `location_uuid`."""
        if self._dp_location_map is None:

            async def _fetch():
                async with get_dataplatform_client() as client:
                    return await fetch_dp_location_map(client)

            name_map = _run_async(_fetch())
            self._dp_location_map = {
                summary.location_uuid: summary for summary in name_map.values()
            }

        return self._dp_location_map

    def get(
        self,
        pv_ids: list[PvId] | PvId,
        start_ts: Timestamp | None = None,
        end_ts: Timestamp | None = None,
    ) -> xr.Dataset:
        """Get a slice of data"""

        if self._max_ts is not None:
            end_ts = min_timestamp(self._max_ts, end_ts)

        if isinstance(pv_ids, PvId):
            # Note that this was a scalar.
            was_scalar = True
            pv_ids = [pv_ids]
        else:
            was_scalar = False

        location_uuids = pv_ids

        _log.debug(
            f"Getting data from {start_ts} to {end_ts} for {len(location_uuids)} PVs "
            "from the Data Platform"
        )

        df, dp_locations, self._dp_location_map = _run_async(
            fetch_generation_and_locations_from_dp(
                location_uuids, start_ts, end_ts, self._dp_location_map
            )
        )

        df = df.set_index(["id", "ts"])

        # Remove duplicate rows.
        duplicates = df.index.duplicated(keep="first")
        df = df[~duplicates]

        da = xr.Dataset.from_dataframe(df)

        # Make sure the ids are in the index. They won't be in the case where there is no data.
        da = da.reindex(id=pv_ids)

        # Add the metadata associated with the PV systems, all sourced from the Data Platform.
        # A `NaN` tilt/orientation/lat/long/capacity makes pvlib's irradiance calculation
        # return `NaN` for the whole plane-of-array irradiance, which then silently propagates
        # into `NaN` for every normalized power value and therefore every prediction — so a
        # site missing any of these in its DP metadata (e.g. not yet backfilled with
        # tilt/orientation) fails hard here instead of producing a garbage forecast.
        meta: dict[str, dict] = {}
        for location_uuid in location_uuids:
            location_data = dp_locations.get(location_uuid, {})
            missing = [key for key in META_KEYS if key not in location_data]
            if missing:
                raise RuntimeError(
                    f"Data Platform location {location_uuid!r} is missing {missing} in its "
                    "metadata — cannot compute a forecast without them."
                )
            meta[location_uuid] = {key: location_data[key] for key in META_KEYS}

        # Add the metadata as coordinates to the PVs in the xr.Dataset.
        da = da.assign_coords(
            {
                key: (
                    "id",
                    [meta[location_uuid][key] for location_uuid in location_uuids],
                )
                for key in META_KEYS
            }
        )

        # "capacity" is the only coord that doesn't have the name we expect.
        da = da.rename({"capacity_kw": "capacity"})

        # If the input was a scalar, we make sure the output is consistent, by slicing on the
        # (unique) PV.
        if was_scalar:
            da = da.isel(id=0)

        return da

    def get_site_metadata(self) -> dict[str, dict]:
        """Fetch client_location_name, capacity_kw, latitude, longitude for all SITE locations.

        Returns a mapping of pv_id (Data Platform location_uuid) to a metadata dict.
        """
        loc_map = self._get_location_map()
        return {
            location_uuid: {
                "client_location_name": get_client_location_name(summary),
                "capacity_kw": summary.effective_capacity_watts / 1000.0,
                "latitude": summary.latlng.latitude,
                "longitude": summary.latlng.longitude,
            }
            for location_uuid, summary in loc_map.items()
        }

    def list_pv_ids(self) -> list[PvId]:
        """List all the PV ids (Data Platform location_uuids)."""
        location_uuids = list(self._get_location_map().keys())
        _log.debug("%i location_uuids from the Data Platform", len(location_uuids))
        return location_uuids

    def min_ts(self) -> Timestamp:
        """Return the earliest timestamp of the data."""
        raise NotImplementedError

    def max_ts(self) -> Timestamp:
        """Return the latest timestamp of the data."""
        raise NotImplementedError

    def as_available_at(self, ts: Timestamp):
        """Return a copy that ignores everything after `ts - blackout`."""
        self_copy = copy.copy(self)
        self_copy._max_ts = min_timestamp(ts, self._max_ts)
        return self_copy
