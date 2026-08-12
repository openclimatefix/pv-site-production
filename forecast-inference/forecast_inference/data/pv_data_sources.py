"""
PV Data Source
"""

import asyncio
import concurrent.futures
import copy
import logging
import os
from uuid import UUID

import numpy as np
import pandas as pd
import xarray as xr
from psp.data_sources.pv import PvDataSource, min_timestamp
from psp.typings import PvId, Timestamp
from pvsite_datamodel.connection import DatabaseConnection
from pvsite_datamodel.sqlmodels import GenerationSQL, LocationSQL
from sqlalchemy.orm import Session

from forecast_inference.save.data_platform import (
    LocationSummary,
    get_generation_and_locations_from_dp,
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
        # Cached across calls so a run over many sites doesn't re-list every DP location
        # on every single `.get()` call (which happens once per site).
        self._dp_location_map: dict[str, LocationSummary] | None = None

    def _load_sites(self, session: Session, site_uuids: list[str]) -> list[LocationSQL]:
        """Fetch and validate the LocationSQL rows for the given site uuids."""
        site_query = session.query(LocationSQL).filter(
            LocationSQL.location_uuid.in_([UUID(x) for x in site_uuids])
        )
        sites = site_query.all()

        if len(sites) != len(site_uuids):
            raise RuntimeError(f"We found only {len(sites)} of {len(site_uuids)}, aborting!")

        return sites

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

        if read_from_dp:
            # Get the site info first and let the DB session close before making the
            # (potentially slow) Data Platform network calls below, rather than holding
            # a pooled DB connection idle for the duration of the DP round-trip.
            with self._database_connection.get_session() as session:
                sites = self._load_sites(session, site_uuids)

            if start_ts is None or end_ts is None:
                raise ValueError(
                    "Reading from the Data Platform requires both `start_ts` and `end_ts`"
                )
            if self._dp_location_map is None:
                _log.info("Reading generation and locations from the Data Platform")
            else:
                _log.debug("Reading generation and locations from the Data Platform (cached)")
            df, dp_locations, self._dp_location_map = _run_async(
                get_generation_and_locations_from_dp(
                    sites, start_ts, end_ts, self._dp_location_map
                )
            )
        else:
            with self._database_connection.get_session() as session:
                sites = self._load_sites(session, site_uuids)

                query = session.query(GenerationSQL).filter(
                    GenerationSQL.location_uuid.in_([UUID(x) for x in site_uuids])
                )

                if start_ts is not None:
                    query = query.filter(GenerationSQL.start_utc >= start_ts)

                if end_ts is not None:
                    query = query.filter(GenerationSQL.start_utc < end_ts)

                generations = query.all()
                dp_locations = {}

                _log.debug(f"Found {len(generations)} generation data for {len(site_uuids)} PVs")

                # Build a pandas dataframe of id, timestamp and power.
                df = pd.DataFrame.from_records(
                    [
                        {
                            "id": str(g.location_uuid),
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
