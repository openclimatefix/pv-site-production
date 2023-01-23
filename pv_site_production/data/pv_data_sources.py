import copy
import pathlib
from datetime import timedelta

import numpy as np
import pandas as pd
import xarray as xr
from nowcasting_datamodel.models.pv import solar_sheffield_passiv as SHEFFIELD
from nowcasting_datamodel.read.read_pv import get_pv_systems, get_pv_yield
from psp.data.data_sources.pv import PvDataSource, min_timestamp
from psp.ml.typings import PvId, Timestamp

# Meta keys that are stil taken from our metadata file.
# TODO Those should move to the database.
META_FILE_KEYS = ["tilt", "orientation", "factor"]
META_DB_KEYS = ["longitude", "latitude"]


class DbPvDataSource(PvDataSource):
    def __init__(self, db_connection, metadata_path: pathlib.Path):
        self._db_connection = db_connection
        self._meta = {
            # TODO Standardize the column names.
            # Making sure the pv_id (ss_id) is parsed as an integer.
            int(row["ss_id"]): {key: row[key] for key in META_FILE_KEYS}
            for _, row in pd.read_csv(metadata_path).iterrows()
        }

        # We'll ignore anything after that date. This is set in the `without_future` method.
        self._max_ts: Timestamp | None = None

    def get(
        self,
        pv_ids: list[PvId] | PvId,
        start_ts: Timestamp | None = None,
        end_ts: Timestamp | None = None,
    ) -> xr.Dataset:

        if self._max_ts is not None:
            end_ts = min_timestamp(self._max_ts, end_ts)

        # TODO The fact that we have to check for two types tells me something does not get checked
        # properly somewhere!
        if isinstance(pv_ids, (PvId, np.integer)):
            was_1d = True
            pv_ids = [pv_ids]
        else:
            was_1d = False

        with self._db_connection.get_session() as session:
            pv_yields = get_pv_yield(
                session=session,
                pv_systems_ids=pv_ids,
                start_utc=start_ts,
                end_utc=end_ts,
                # TODO What does this option mean?
                correct_data=None,
                providers=[SHEFFIELD],
            )

        # Build a pandas dataframe of pv_id, timestamp and power. This makes it easy to convert to
        # an xarray.
        df = pd.DataFrame.from_records(
            {
                "pv_id": y.pv_system.pv_system_id,
                # We remove the timezone information since otherwise the timestamp index gets
                # converted to an "object" index later. In any case we should have everything in
                # UTC.
                "ts": y.datetime_utc.replace(tzinfo=None),
                "power": y.solar_generation_kw,
            }
            for y in pv_yields
        )
        # Convert it to an xarray.
        df = df.set_index(["pv_id", "ts"])

        # Remove duplicate rows.
        # TODO This should not be necessary: we should be able to remove it once we insure the
        # database can not have duplicates.
        df = df[~df.index.duplicated(keep="first")]

        da = xr.Dataset.from_dataframe(df)

        # Add the metadata associated with the PV systems.
        # Some come from the database, and some from a separate metadata file.
        # TODO The info from the metadata file (tilt, orientation and "factor")
        # probably belong to the database!
        meta_from_db = {
            y.pv_system.pv_system_id: {
                key: getattr(y.pv_system, key) for key in META_DB_KEYS
            }
            for y in pv_yields
        }

        pv_ids = [int(x) for x in da.coords["pv_id"].values]

        # Merge both the meta data from the DB and from the file.
        meta = {pv_id: self._meta[pv_id] | meta_from_db[pv_id] for pv_id in pv_ids}

        # Add the metadata as coordinates to the pv_ids in the xr.Dataset.
        da = da.assign_coords(
            {
                key: ("pv_id", [meta[pv_id][key] for pv_id in pv_ids])
                for key in META_FILE_KEYS + META_DB_KEYS
            }
        )

        if was_1d:
            da = da.isel(pv_id=0)

        return da

    def list_pv_ids(self) -> list[PvId]:
        with self._db_connection.get_session() as session:
            pv_systems = get_pv_systems(session=session, provider=SHEFFIELD)
        pv_ids = [p.pv_system_id for p in pv_systems]
        # Only keep the pv_ids for which we have metadata.
        pv_ids = [pv_id for pv_id in pv_ids if pv_id in self._meta]
        return pv_ids

    def min_ts(self) -> Timestamp:
        raise NotImplementedError

    def max_ts(self) -> Timestamp:
        raise NotImplementedError

    def without_future(self, ts: Timestamp, *, blackout: int = 0):
        now = ts - timedelta(minutes=blackout) - timedelta(seconds=1)
        self_copy = copy.copy(self)
        self_copy._max_ts = min_timestamp(now, self._max_ts)
        return self_copy
