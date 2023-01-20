""" Main app """
import logging
import pathlib
from datetime import datetime, timedelta

import click
import pandas as pd
import yaml
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_PV
from psp.ml.models.base import PvSiteModel
from psp.ml.typings import PvId, Timestamp, X

from pv_site_production.data.pv_data_sources import DbPvDataSource
from pv_site_production.utils.imports import import_from_module

_log = logging.getLogger(__name__)


# TODO This probably goes to another file
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
                    + timedelta(minutes=(horizon[1] + horizon[0]) / 2),
                }
            )

    return pd.DataFrame.from_records(records)


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
    "timestamp",
    type=click.DateTime(formats=["%Y-%m-%d-%H-%M"]),
    default=None,
    help='Date-time (UTC) at which to make the prediction. Defaults to "now".',
)
def run(
    config_path: pathlib.Path,
    timestamp: datetime | None,
):
    # logging.basicConfig(level=logging.INFO)
    """Make app method"""
    with open(config_path) as f:
        config = yaml.safe_load(f)

    if timestamp is None:
        timestamp = datetime.utcnow()

    get_model = import_from_module(config["run_model_func"])

    _log.debug("Connecting to pv database")
    url = config["pv_db_url"]
    pv_db_connection = DatabaseConnection(url=url, base=Base_PV, echo=False)

    # Wrap into a PV data source for the models.
    _log.debug("Creating PV data source")
    pv_data_source = DbPvDataSource(pv_db_connection, config["pv_metadata_path"])

    _log.debug("Loading model")
    model: PvSiteModel = get_model(config, pv_data_source)

    pv_ids = pv_data_source.list_pv_ids()
    pv_ids = pv_ids[:10]

    _log.info("Applying model")
    # TODO Calling/printing like this to debug but we could call `apply_model` once with all the pv
    # ids.
    for pv_id in pv_ids:
        results_df = apply_model(model, pv_ids=[pv_id], ts=timestamp)
        print(pv_id)
        for _, row in results_df.iterrows():
            print(f"{row['target_datetime_utc']}: {row['forecast_kw']}")


if __name__ == "__main__":
    run()
