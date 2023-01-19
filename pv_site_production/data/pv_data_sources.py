import pathlib

import numpy as np
import pandas as pd
import xarray as xr
from nowcasting_datamodel.models.pv import PVYield
from nowcasting_datamodel.models.pv import solar_sheffield_passiv as SHEFFIELD
from nowcasting_datamodel.read.read_pv import get_pv_systems, get_pv_yield
from psp.data.data_sources.pv import PvDataSource
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

    def get(
        self,
        pv_ids: list[PvId] | PvId,
        start_ts: Timestamp | None = None,
        end_ts: Timestamp | None = None,
    ) -> xr.Dataset:

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

        records = [(PVYield.from_orm(pv_yield)).__dict__ for pv_yield in pv_yields]

        # Build a pandas dataframe of pv_id, timestamp and power. This makes it easy to convert to
        # an xarray.
        df = pd.DataFrame.from_records(
            {
                "pv_id": r["pv_system"].pv_system_id,
                # We remove the timezone information since otherwise the timestamp index gets
                # converted to an "object" index later. In any case we should have everything in
                # UTC.
                "ts": r["datetime_utc"].replace(tzinfo=None),
                "power": r["solar_generation_kw"],
            }
            for r in records
        )
        # Convert it to an xarray.
        df = df.set_index(["pv_id", "ts"])
        da = xr.Dataset.from_dataframe(df)

        # Add the metadata associated with the PV systems.
        # Some come from the database, and some from a separate metadata file.
        # TODO The info from the metadata file (tilt, orientation and "factor")
        # probably belong to the database!
        meta_from_db = {
            r["pv_system"].pv_system_id: {
                key: getattr(r["pv_system"], key) for key in META_DB_KEYS
            }
            for r in records
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
        return pv_ids

    def min_ts(self) -> Timestamp:
        raise NotImplementedError

    def max_ts(self) -> Timestamp:
        raise NotImplementedError

    def without_future(self, ts: Timestamp, *, blackout: int = 0):
        # TODO Do we need to cut anything for prod?
        return self
