""" Main app """
import importlib
import pathlib
from datetime import datetime, timedelta

import click
import pandas as pd
import yaml
from psp.data.data_sources.pv import NetcdfPvDataSource
from psp.ml.models.base import PvSiteModel
from psp.ml.typings import PvId, Timestamp, X


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


@click.command()
@click.option(
    "--config",
    "-c",
    "config_path",
    type=click.Path(path_type=pathlib.Path),
    help="Config defining the model to use and its parameters.",
)
def run(
    config_path: pathlib.Path,
):
    """Make app method"""
    with open(config_path) as f:
        config = yaml.safe_load(f)

    run_model_func = config["run_model_func"]
    module, func_name = run_model_func.rsplit(".", maxsplit=1)
    get_model = getattr(importlib.import_module(module), func_name)

    model: PvSiteModel = get_model(config)

    # TODO This should point to our database instead.
    pv_data = NetcdfPvDataSource(config["pv_path"])

    pv_ids = pv_data.list_pv_ids()

    # TODO support passing a different date.
    now = datetime.now()

    # TODO make sure `run_model` accept a config object and not a path.
    results_df = apply_model(model, pv_ids=pv_ids, ts=now)

    # TODO
    print(results_df)


if __name__ == "__main__":
    run()
