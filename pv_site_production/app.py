""" Main app """
import importlib
import pathlib
from datetime import datetime, timedelta
from typing import Any

import click
import numpy as np
import pandas as pd
import xarray as xr
import yaml
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_PV
from nowcasting_datamodel.models.pv import PVYield
from nowcasting_datamodel.models.pv import solar_sheffield_passiv as SHEFFIELD
from nowcasting_datamodel.read.read_pv import get_pv_systems, get_pv_yield
from psp.data.data_sources.pv import PvDataSource
from psp.ml.models.base import PvSiteModel
from psp.ml.typings import PvId, Timestamp, X

# Meta keys that are stil taken from our metadata file.
# TODO Those should move to the database.
META_FILE_KEYS = ["tilt", "orientation", "factor"]
META_DB_KEYS = ["longitude", "latitude"]


# TODO This probably goes to another file
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
        session = self._db_connection.get_session()

        # TODO The fact that we have to check for two types tells me something does not get checked
        # properly somewhere!
        if isinstance(pv_ids, (PvId, np.integer)):
            was_1d = True
            pv_ids = [pv_ids]
        else:
            was_1d = False

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
        raise NotImplementedError

    def min_ts(self) -> Timestamp:
        raise NotImplementedError

    def max_ts(self) -> Timestamp:
        raise NotImplementedError

    def without_future(self, ts: Timestamp, *, blackout: int = 0):
        # TODO Do we need to cut anything for prod?
        return self


def apply_model(model: PvSiteModel, pv_ids: list[PvId], ts: Timestamp) -> pd.DataFrame:
    """Run a model on PVs at a given timestamp.

    This returns a dataframe with the following columns
        "target_datetime_utc"
        "forecast_kw"
        "pv_uuid"
    """
    records: list[dict] = []

    for pv_id in pv_ids:
        pred = model.predict(X(pv_id=pv_id, ts=ts))
        for horizon, power in zip(model.config.future_intervals, pred.powers):
            # TODO Make sure we have consistent units for the forecast.
            # We might want something like "energy" for the given time interval.
            records.append(
                {
                    # TODO Make sure we use the same units everywhere.
                    "forecast_kw": power / 1000 * 12,
                    "pv_uuid": pv_id,
                    # TODO Does it make sense to use the middle of the horizon interval?
                    "target_datetime_utc": ts
                    + timedelta(minutes=horizon[1] - horizon[0]),
                }
            )

    return pd.DataFrame.from_records(records)


def import_from_module(module_path: str) -> Any:
    """
    `func = import_from_module('some.module.func')`

    is equivalent to

    `from some.module import func`
    -------
    """
    module, func_name = module_path.rsplit(".", maxsplit=1)
    return getattr(importlib.import_module(module), func_name)


@click.command()
@click.option(
    "--config",
    "-c",
    "config_path",
    type=click.Path(path_type=pathlib.Path),
    help="Config defining the model to use and its parameters.",
)
@click.option(
    "--date",
    "-d",
    'timestamp',
    type=click.DateTime(formats=["%Y-%m-%d-%H-%M"]),
    default=None,
    help='Date-time (UTC) at which to make the prediction. Defaults to "now".',
)
def run(
    config_path: pathlib.Path,
    timestamp: datetime | None ,
):
    """Make app method"""
    with open(config_path) as f:
        config = yaml.safe_load(f)

    if timestamp is None:
        timestamp = datetime.utcnow()

    get_model = import_from_module(config["run_model_func"])

    url = config["pv_db_url"]
    pv_db_connection = DatabaseConnection(url=url, base=Base_PV)

    # Wrap into a PV data source for the models.
    pv_data_source = DbPvDataSource(pv_db_connection, config["pv_metadata_path"])

    model: PvSiteModel = get_model(config, pv_data_source)

    session = pv_db_connection.get_session()
    pv_systems = get_pv_systems(session=session, provider=SHEFFIELD)

    pv_ids = [p.pv_system_id for p in pv_systems]

    # TODO make sure `run_model` accept a config object and not a path.
    results_df = apply_model(model, pv_ids=pv_ids, ts=timestamp)

    # TODO
    print(results_df)


if __name__ == "__main__":
    run()
