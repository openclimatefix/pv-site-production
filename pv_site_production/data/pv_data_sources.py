"""
PV Data Source
"""

import copy
import logging
import pathlib
from datetime import timedelta
from uuid import UUID

import numpy as np
import pandas as pd
import xarray as xr
from psp.data.data_sources.pv import PvDataSource, min_timestamp
from psp.ml.typings import PvId, Timestamp

# from nowcasting_datamodel.models.pv import solar_sheffield_passiv as SHEFFIELD
# from nowcasting_datamodel.read.read_pv import get_pv_systems, get_pv_yield
from pvsite_datamodel.read.generation import get_pv_generation_by_sites
from pvsite_datamodel.sqlmodels import SiteSQL
from sqlalchemy.orm import Session, sessionmaker

# Meta keys that are still taken from our inferred metadata file.
META_FILE_KEYS = ["tilt", "orientation", "factor"]
META_DB_KEYS = ["longitude", "latitude"]

_log = logging.getLogger(__name__)


# def _list_site_ids_by_client(session: Session, client_uuid: str) -> list[int]:
#     query = session.query(SiteSQL).filter(SiteSQL.client_uuid == client_uuid)
#     return [row.site_uuid for row in query]


def _get_site_client_id_to_uuid_mapping(
    session: Session,  # , client_uuid: str
) -> dict[int, str]:
    query = session.query(SiteSQL)  # .filter(SiteSQL.client_uuid == client_uuid)
    # FIXME do we need the cast to str here - should we use the UUID type everywhere?
    mapping = {row.client_site_id: str(row.site_uuid) for row in query}
    return mapping


class DbPvDataSource(PvDataSource):
    """PV Data Source that reads from our database."""

    def __init__(
        self,
        session_factory: sessionmaker,
        metadata_path: pathlib.Path | str,
    ):
        """Constructor"""
        self._session_factory = session_factory

        # The info in the metadata file uses the client's ids, we'll need to map those to
        # site_uuids.
        with session_factory() as session:
            self.id_map = _get_site_client_id_to_uuid_mapping(session)

            print(self.id_map)

        # Fill in the metadata from the file.
        self._meta: dict[str, dict[str, float]] = {}

        for _, row in pd.read_csv(metadata_path).iterrows():
            client_site_id = int(row["ss_id"])
            site_uuid = self.id_map.get(client_site_id)

            if site_uuid is None:
                _log.warning('Unknown client_site_id "%i"', client_site_id)
                continue

            self._meta[site_uuid] = {key: row[key] for key in META_FILE_KEYS}

        print(self._meta)

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

        # TODO The fact that we have to check for two types tells me something does not get checked
        # properly somewhere!
        if isinstance(pv_ids, (PvId, np.integer)):
            # Note that this was a scalar.
            was_scalar = True
            pv_ids = [pv_ids]
        else:
            was_scalar = False

        with self._session_factory() as session:
            # FIXME change variable names to reflresh the database objects.

            print(f'Getting data from {start_ts} to {end_ts} for {pv_ids}')

            site_uuids = [UUID(self.id_map[pv_id]) for pv_id in pv_ids]

            generations = get_pv_generation_by_sites(
                session=session,
                start_utc=start_ts,
                end_utc=end_ts,
                site_uuids=site_uuids,
            )

            if len(generations) > 0:
                # FIXME it should get here when running the test_common test
                # Until then something is odd
                assert False

        # Build a pandas dataframe of pv_id, timestamp and power. This makes it easy to convert to
        # an xarray.
        df = pd.DataFrame.from_records(
            {
                "pv_id": str(g.site_uuid),
                # We remove the timezone information since otherwise the timestamp index gets
                # converted to an "object" index later. In any case we should have everything in
                # UTC.
                # FIXME this will probably not be an eager join? We need to make sure we don't hit
                # the database again.
                "ts": g.datetime_interval.start_utc.replace(tzinfo=None),
                "power": g.power_kw,
            }
            for g in generations
        )
        # Convert it to an xarray.
        print('Select "pv_id", "ts"')
        print(df)
        df = df.set_index(["pv_id", "ts"])

        # Remove duplicate rows.
        # TODO This should not be necessary: we should be able to remove it once we insure the
        # database can not have duplicates.
        df = df[~df.index.duplicated(keep="first")]

        da = xr.Dataset.from_dataframe(df)

        # Add the metadata associated with the PV systems.
        # Some come from the database, and some from a separate metadata file.
        meta_from_db = {
            # FIXME there might be a missing relationship here and we need to make sure it is
            # not lazy loaded.
            str(g.site_uuid): {key: getattr(g.site, key) for key in META_DB_KEYS}
            for g in generations
        }

        pv_ids = [str(x) for x in da.coords["pv_id"].values]

        # Merge both the meta data from the DB and from the file.
        # FIXME no UUID cast should be required here
        meta = {pv_id: self._meta[pv_id] | meta_from_db[pv_id] for pv_id in pv_ids}

        # Add the metadata as coordinates to the pv_ids in the xr.Dataset.
        da = da.assign_coords(
            {
                key: ("pv_id", [meta[pv_id][key] for pv_id in pv_ids])
                for key in META_FILE_KEYS + META_DB_KEYS
            }
        )

        # If the input was a scalar, we make sure the output is consistent, by slicing on the
        # (unique) PV.
        if was_scalar:
            da = da.isel(pv_id=0)

        return da

    def list_pv_ids(self) -> list[PvId]:
        """List all the PV ids"""
        with self._session_factory() as session:
            query = session.query(SiteSQL.site_uuid)
            pv_ids = [str(row.site_uuid) for row in query]
        print("site_uuids from DB")
        print(pv_ids)
        print("site_uuids from meta")
        print(list(self._meta.keys()))
        #     mapping = {row.client_site_id: row.site_uuid for row in query}
        # pv_ids = _list_site_ids_by_client(session, self._client_uuid)
        # Only keep the pv_ids for which we have metadata.
        pv_ids = [pv_id for pv_id in pv_ids if pv_id in self._meta]
        print("pv_ids left")
        print(pv_ids)
        return pv_ids

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
