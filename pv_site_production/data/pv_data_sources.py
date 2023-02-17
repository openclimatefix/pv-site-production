"""
PV Data Source
"""

import copy
import logging
import pathlib
from collections import defaultdict
from datetime import timedelta
from typing import Any
from uuid import UUID

import pandas as pd
import xarray as xr
from psp.data.data_sources.pv import PvDataSource, min_timestamp
from psp.typings import PvId, Timestamp
from pvsite_datamodel.connection import DatabaseConnection
from pvsite_datamodel.read.generation import get_pv_generation_by_sites
from pvsite_datamodel.sqlmodels import SiteSQL
from sqlalchemy.orm import Session

# Meta keys that are still taken from our inferred metadata file.
META_FILE_KEYS = ["tilt", "orientation", "factor"]
META_DB_KEYS = ["longitude", "latitude"]

_log = logging.getLogger(__name__)


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
        metadata_path: pathlib.Path | str,
    ):
        """Constructor"""
        self._database_connection = database_connection

        # The info in the metadata file uses the client's ids, we'll need to map those to
        # site_uuids.
        with database_connection.get_session() as session:  # type: ignore
            site_id_to_uuid = _get_site_client_id_to_uuid_mapping(session)

        # Fill in the metadata from the file.
        self._meta: dict[PvId, dict[str, float]] = {}

        # Make sure we load the `ss_id`s as `str` (if we cast it after, we get '1234.0' instead of
        # '1234'). Everything else can be loaded as `float`.
        meta_dtype: dict[str, Any] = defaultdict(lambda: float)
        meta_dtype["ss_id"] = str

        for _, row in pd.read_csv(metadata_path, dtype=meta_dtype).iterrows():
            client_site_id = str(row["ss_id"])
            site_uuid = site_id_to_uuid.get(client_site_id)

            if site_uuid is None:
                _log.info('Unknown client_site_id "%s"', client_site_id)
                continue

            self._meta[site_uuid] = {key: float(row[key]) for key in META_FILE_KEYS}

        # We'll ignore anything after that date. This is set in the `without_future` method.
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
        with self._database_connection.get_session() as session:  # type: ignore
            generations = get_pv_generation_by_sites(
                session=session,
                start_utc=start_ts,
                end_utc=end_ts,
                # Convert to proper `UUID`s when we interact with the database.
                site_uuids=[UUID(x) for x in site_uuids],
            )

        _log.debug(f"Found {len(generations)} generation data for {len(site_uuids)} PVs")

        # Build a pandas dataframe of id, timestamp and power.
        # This makes it easy to convert to an xarray.
        df = pd.DataFrame.from_records(
            {
                "id": str(g.site_uuid),
                # We remove the timezone information since otherwise the timestamp index gets
                # converted to an "object" index later. In any case we should have everything in
                # UTC.
                "ts": g.datetime_interval.start_utc.replace(tzinfo=None),
                "power": g.power_kw,
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
        meta_from_db = {
            str(g.site_uuid): {key: getattr(g.site, key) for key in META_DB_KEYS}
            for g in generations
        }

        # Merge both the meta data from the DB and from the file.
        meta = {
            site_uuid: self._meta[site_uuid] | meta_from_db[site_uuid] for site_uuid in site_uuids
        }

        # Add the metadata as coordinates to the PVs in the xr.Dataset.
        da = da.assign_coords(
            {
                key: ("id", [meta[site_uuid][key] for site_uuid in site_uuids])
                for key in META_FILE_KEYS + META_DB_KEYS
            }
        )

        # If the input was a scalar, we make sure the output is consistent, by slicing on the
        # (unique) PV.
        if was_scalar:
            da = da.isel(id=0)

        return da

    def list_pv_ids(self) -> list[PvId]:
        """List all the PV ids"""
        with self._database_connection.get_session() as session:  # type: ignore
            query = session.query(SiteSQL.site_uuid)
            site_uuids = [str(row.site_uuid) for row in query]
        _log.debug("%i site_uuids from DB", len(site_uuids))
        _log.debug("%i site_uuids from meta", len(self._meta))
        # Only keep the site_uuids for which we have metadata.
        site_uuids = [site_uuid for site_uuid in site_uuids if site_uuid in self._meta]
        _log.debug("%i site_uuids in common", len(site_uuids))
        return site_uuids

    def min_ts(self) -> Timestamp:
        """Return the earliest timestamp of the data."""
        raise NotImplementedError

    def max_ts(self) -> Timestamp:
        """Return the latest timestamp of the data."""
        raise NotImplementedError

    def without_future(self, ts: Timestamp, *, blackout: int = 0):
        """Return a copy that ignores everything after `ts - blackout`."""
        now = ts - timedelta(minutes=blackout) - timedelta(seconds=1)
        self_copy = copy.copy(self)
        self_copy._max_ts = min_timestamp(now, self._max_ts)
        return self_copy
