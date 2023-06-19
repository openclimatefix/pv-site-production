"""
PV Data Source
"""

import copy
import logging
from uuid import UUID

import numpy as np
import pandas as pd
import xarray as xr
from psp.data_sources.pv import PvDataSource, min_timestamp
from psp.typings import PvId, Timestamp
from pvsite_datamodel.connection import DatabaseConnection
from pvsite_datamodel.sqlmodels import GenerationSQL, SiteSQL
from sqlalchemy.orm import Session, joinedload

META_KEYS = [
    "longitude",
    "latitude",
    "tilt",
    "orientation",
    "capacity_kw",
]

_log = logging.getLogger(__name__)


def _to_float(x: float | None) -> float:
    """Return `np.nan` when `None."""
    if x is None:
        return np.nan
    return x


def _get_site_client_id_to_uuid_mapping(
    session: Session,
) -> dict[str, str]:
    """Construct a mapping from site_client_id to site_uuid.

    This is needed because our meta data is still by client_site_id.
    """
    query = session.query(SiteSQL)
    mapping = {str(row.client_site_id): str(row.site_uuid) for row in query}
    return mapping


class DbPvDataSource(PvDataSource):
    """PV Data Source that reads from our database."""

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

        _log.debug(f"Getting data from {start_ts} to {end_ts} for {len(site_uuids)} PVs")
        with self._database_connection.get_session() as session:
            query = session.query(GenerationSQL).filter(
                GenerationSQL.site_uuid.in_([UUID(x) for x in site_uuids])
            )

            if start_ts is not None:
                query = query.filter(GenerationSQL.start_utc >= start_ts)

            if end_ts is not None:
                # Note that we still filter on the `start_utc` field. This is because we assume that
                # the "generation" power is punctual.
                query = query.filter(GenerationSQL.start_utc < end_ts)

            # Eagerly join the sites: we need its metadata.
            query = query.options(joinedload(GenerationSQL.site))
            generations = query.all()

        _log.debug(f"Found {len(generations)} generation data for {len(site_uuids)} PVs")

        # Build a pandas dataframe of id, timestamp and power.
        # This makes it easy to convert to an xarray.
        df = pd.DataFrame.from_records(
            {
                "id": str(g.site_uuid),
                # We remove the timezone information since otherwise the timestamp index gets
                # converted to an "object" index later. In any case we should have everything in
                # UTC.
                "ts": g.start_utc.replace(tzinfo=None),
                "power": g.generation_power_kw,
            }
            for g in generations
        )

        df = df.set_index(["id", "ts"])

        # Remove duplicate rows.
        # TODO This should not be necessary: we should be able to remove it once we insure the
        # database can not have duplicates.
        # See https://github.com/openclimatefix/pvsite-datamodel/issues/34
        duplicates = df.index.duplicated(keep="first")
        df = df[~duplicates]

        da = xr.Dataset.from_dataframe(df)

        # Add the metadata associated with the PV systems.
        # Some come from the database, and some from a separate metadata file.
        meta = {
            str(g.site_uuid): {key: _to_float(getattr(g.site, key)) for key in META_KEYS}
            for g in generations
        }

        # Add the metadata as coordinates to the PVs in the xr.Dataset.
        da = da.assign_coords(
            {key: ("id", [meta[site_uuid][key] for site_uuid in site_uuids]) for key in META_KEYS}
        )

        # "capacity" is the only coord that doesn't have the name we expect.
        da = da.rename({"capacity_kw": "capacity"})

        # If the input was a scalar, we make sure the output is consistent, by slicing on the
        # (unique) PV.
        if was_scalar:
            da = da.isel(id=0)

        return da

    def list_pv_ids(self) -> list[PvId]:
        """List all the PV ids"""
        with self._database_connection.get_session() as session:
            query = session.query(SiteSQL.site_uuid)
            site_uuids = [str(row.site_uuid) for row in query]
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
