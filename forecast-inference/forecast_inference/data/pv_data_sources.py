"""
PV Data Source
"""

import asyncio
import concurrent.futures
import copy
import logging
import os
from datetime import UTC, timedelta
from uuid import UUID

import numpy as np
import pandas as pd
import xarray as xr
from dp_sdk.ocf import dp
from psp.data_sources.pv import PvDataSource, min_timestamp
from psp.typings import PvId, Timestamp
from pvsite_datamodel.connection import DatabaseConnection
from pvsite_datamodel.sqlmodels import GenerationSQL, LocationSQL
from sqlalchemy.orm import Session

from forecast_inference.save.data_platform import (
    DataPlatformClient,
    LocationSummary,
    _sanitize,
    fetch_dp_location_map,
    get_dataplatform_client,
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


def _to_float(x: float | None) -> float:
    """Return `np.nan` when `None."""
    if x is None:
        return np.nan
    return x


def _ensure_timezone_aware(ts: Timestamp) -> Timestamp:
    """Ensure a datetime is timezone-aware UTC, as required by the Data Platform API."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def _get_site_client_id_to_uuid_mapping(
    session: Session,
) -> dict[str, str]:
    """Construct a mapping from site_client_id to site_uuid.

    This is needed because our meta data is still by client_site_id.
    """
    query = session.query(LocationSQL)
    mapping = {str(row.client_location_id): str(row.location_uuid) for row in query}
    return mapping


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
        _log.warning(f"Site {site.location_uuid} has no client_location_name, skipping DP")
        return []

    name = _sanitize(site.client_location_name)
    summary = loc_map.get(name)
    if not summary:
        _log.warning(f"Site {name!r} not found in Data Platform")
        return []

    actual_observer_name = observer_name or os.getenv("OBSERVER_NAME", "nednl")

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
        try:
            res = await client.get_observations_as_timeseries(req)
        except Exception as e:  # noqa: BLE001 - one site's DP failure shouldn't abort the run
            _log.error(f"Failed to fetch observations for {name!r} [{c_start} to {c_end}]: {e}")
            continue

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
        "latitude": summary.latlng.latitude if summary.latlng else np.nan,
        "longitude": summary.latlng.longitude if summary.latlng else np.nan,
        "capacity_kw": summary.effective_capacity_watts / 1000.0,
    }


async def _get_generation_and_locations_from_dp(
    sites: list[LocationSQL],
    start_ts: Timestamp,
    end_ts: Timestamp,
) -> tuple[pd.DataFrame, dict[str, dict]]:
    """Fetch generation values and location metadata for all sites from the Data Platform.

    Returns a tuple of:
    - A dataframe of (id, ts, power) records, one row per generation value found.
    - A dict of location_uuid (as in our database) -> {latitude, longitude, capacity_kw},
      for sites that were found in the Data Platform.
    """
    async with get_dataplatform_client() as client:
        loc_map = await fetch_dp_location_map(client)

        generations = await asyncio.gather(
            *[
                fetch_generation_from_dp(client, loc_map, site, start_ts, end_ts)
                for site in sites
            ]
        )

    records = [
        {"id": str(site.location_uuid), "ts": t.replace(tzinfo=None), "power": power_kw}
        for site, site_generation in zip(sites, generations)
        for t, power_kw in site_generation
    ]
    df = pd.DataFrame.from_records(records, columns=["id", "ts", "power"])

    locations = {
        str(site.location_uuid): location
        for site in sites
        if (location := fetch_location_from_dp(loc_map, site)) is not None
    }

    return df, locations


class DbPvDataSource(PvDataSource):
    """PV Data Source that reads from our database.

    When the `READ_FROM_DATA_PLATFORM` environment variable is set to "true", generation
    power values and location metadata (latitude, longitude, capacity_kw) are instead
    read from the Data Platform, matched to each site via its `client_location_name`.
    `tilt` and `orientation` have no Data Platform equivalent, so they always come from
    the database.
    """

    def __init__(
        self,
        database_connection: DatabaseConnection,
    ):
        """Constructor"""
        self._database_connection = database_connection
        self._max_ts: Timestamp | None = None

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

        site_uuids = pv_ids

        read_from_dp = os.getenv("READ_FROM_DATA_PLATFORM", "false").lower() == "true"

        _log.debug(f"Getting data from {start_ts} to {end_ts} for {len(site_uuids)} PVs")
        with self._database_connection.get_session() as session:
            # Get the site info in a separate query. We don't JOIN on `generation` in case there
            # is no generation data.
            site_query = session.query(LocationSQL).filter(
                LocationSQL.location_uuid.in_([UUID(x) for x in site_uuids])
            )
            sites = site_query.all()

            if len(sites) != len(site_uuids):
                raise RuntimeError(f"We found only {len(sites)} of {len(site_uuids)}, aborting!")

            if read_from_dp:
                _log.info("Reading generation and locations from the Data Platform")
                if start_ts is None or end_ts is None:
                    raise ValueError(
                        "Reading from the Data Platform requires both `start_ts` and `end_ts`"
                    )
                df, dp_locations = _run_async(
                    _get_generation_and_locations_from_dp(sites, start_ts, end_ts)
                )
            else:
                query = session.query(GenerationSQL).filter(
                    GenerationSQL.location_uuid.in_([UUID(x) for x in site_uuids])
                )

                if start_ts is not None:
                    query = query.filter(GenerationSQL.start_utc >= start_ts)

                if end_ts is not None:
                    # Note that we still filter on the `start_utc` field. This is because we
                    # assume that the "generation" power is punctual.
                    query = query.filter(GenerationSQL.start_utc < end_ts)

                generations = query.all()
                dp_locations = {}

                _log.debug(f"Found {len(generations)} generation data for {len(site_uuids)} PVs")

                # Build a pandas dataframe of id, timestamp and power.
                # This makes it easy to convert to an xarray.
                df = pd.DataFrame.from_records(
                    [
                        {
                            "id": str(g.location_uuid),
                            # We remove the timezone information since otherwise the timestamp
                            # index gets converted to an "object" index later. In any case we
                            # should have everything in UTC.
                            "ts": g.start_utc.replace(tzinfo=None)
                            if g.start_utc is not None
                            else None,
                            "power": g.generation_power_kw,
                        }
                        for g in generations
                    ],
                    columns=["id", "ts", "power"],
                )

        df = df.set_index(["id", "ts"])

        # Remove duplicate rows.
        # TODO This should not be necessary: we should be able to remove it once we insure the
        # database can not have duplicates.
        # See https://github.com/openclimatefix/pvsite-datamodel/issues/34
        duplicates = df.index.duplicated(keep="first")
        df = df[~duplicates]

        da = xr.Dataset.from_dataframe(df)

        # Make sure the ids are in the index. They won't be in the case where the is no data.
        da = da.reindex(id=pv_ids)

        # Add the metadata associated with the PV systems. Prefer values fetched from the Data
        # Platform (when reading from there), falling back to the database for sites the DP
        # doesn't know about, and for the fields (tilt, orientation) it doesn't hold at all.
        meta = {
            str(site.location_uuid): {
                key: dp_locations.get(str(site.location_uuid), {}).get(
                    key, _to_float(getattr(site, key))
                )
                for key in META_KEYS
            }
            for site in sites
        }

        # Add the metadata as coordinates to the PVs in the xr.Dataset.
        da = da.assign_coords(
            {
                key: (
                    "id",
                    [meta[site_uuid][key] for site_uuid in site_uuids],
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
        """Fetch client_location_name, capacity_kw, latitude, longitude for all active UK sites.

        Returns a mapping of pv_id (location_uuid str) to a metadata dict.
        """
        with self._database_connection.get_session() as session:
            query = (
                session.query(LocationSQL)
                .where(LocationSQL.country == "uk")
                .where(LocationSQL.active)
                .all()
            )
            return {
                str(site.location_uuid): {
                    "client_location_name": site.client_location_name,
                    "capacity_kw": site.capacity_kw,
                    "latitude": site.latitude,
                    "longitude": site.longitude,
                }
                for site in query
            }

    def list_pv_ids(self) -> list[PvId]:
        """List all the PV ids"""
        site_uuids = list(self.get_site_metadata().keys())
        _log.debug("%i site_uuids from DB", len(site_uuids))
        return site_uuids

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
