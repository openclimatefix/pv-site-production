"""
Apply the model to the PVs in the database and note the results.
"""

import datetime as dt
import numpy as np
import logging
import os
import pathlib
from uuid import UUID

import click
import dotenv
from psp.models.base import PvSiteModel
from psp.typings import PvId, Timestamp, X
from pvsite_datamodel.connection import DatabaseConnection
from pvsite_datamodel.sqlmodels import ForecastSQL, ForecastValueSQL

from forecast_inference.data.pv_data_sources import DbPvDataSource
from forecast_inference.utils.config import load_config
from forecast_inference.utils.imports import import_from_module
from forecast_inference.utils.profiling import profile

_log = logging.getLogger(__name__)


def _run_model_and_save_for_one_pv(
    database_connection: DatabaseConnection,
    model: PvSiteModel,
    pv_id: PvId,
    timestamp: Timestamp,
    write_to_db: bool,
    print_to_stdout: bool,
) -> bool:
    """
    Run model and save to database for one PV.

    Return:
    ------
        True on success and False if there was an error.
    """
    with profile(f'Applying model on pv "{pv_id}"'):
        try:
            pred = model.predict(X(pv_id=pv_id, ts=timestamp))
        except Exception:
            _log.error(
                'There was an exception calling `model.predict` for pv_id="{pv_id}". Skipping.',
                # Add the exception traceback.
                exc_info=True,
            )
            return False

    site_uuid = UUID(pv_id)

    # Assemble the data in ForecastValuesSQL rows for the database.
    rows = [
        dict(
            start_utc=timestamp + dt.timedelta(minutes=start),
            end_utc=timestamp + dt.timedelta(minutes=end),
            forecast_power_kw=np.round(value,2),
            horizon_minutes=start,
        )
        for (start, end), value in zip(model.config.horizons, pred.powers)
    ]

    if write_to_db:
        with profile(f'Writing {len(rows)} forecast values to db for pv "{pv_id}"'):
            with database_connection.get_session() as session:
                forecast = ForecastSQL(
                    site_uuid=site_uuid, forecast_version="0.0.0", timestamp_utc=timestamp
                )
                session.add(forecast)
                # Flush to get the Forecast's primary key.
                session.flush()

                # Insert all the forecast value objects in one efficient call.
                session.bulk_save_objects(
                    [
                        ForecastValueSQL(
                            **row,
                            forecast_uuid=forecast.forecast_uuid,
                        )
                        for row in rows
                    ]
                )
                session.commit()
    elif print_to_stdout:
        # Write to stdout when we don't want to write in the database.
        print(f'PV Site = "{pv_id}"')
        for row in rows:
            print(f" | {row['start_utc']}" f" | {row['end_utc']}" f" | {row['forecast_power_kw']}")

    return True


@click.command()
@click.option(
    "--config",
    "-c",
    "config_path",
    type=click.Path(path_type=pathlib.Path),
    help="Config defining the model to use and its parameters.",
    required=True,
)
@click.option(
    "--date",
    "-d",
    "timestamp",
    type=click.DateTime(formats=["%Y-%m-%d-%H-%M"]),
    default=None,
    help='Date-time (UTC) at which we make the prediction. Defaults to "now".',
)
@click.option(
    "--round-date-to-minutes",
    type=int,
    help="Round the time at which we make the prediction to nearest (in the past) N minutes."
    " For instance, if it's 15:18 but we use `--round-date-to-minutes 10`, we'll do the predictions"
    " as if it was 15:10."
    " Should not be used if `--date` is used."
    " Default: no rounding.",
)
@click.option(
    "--max-pvs",
    type=int,
    default=None,
    help="Maximum number of PVs to treat. This is useful for testing.",
)
@click.option(
    "--write-to-db",
    is_flag=True,
    default=False,
    help="Set this flag to actually write the results to the database."
    "By default we only print to stdout",
)
@click.option(
    "--no-print-to-stdout",
    is_flag=True,
    default=False,
    help="Do not print the forecasts to stdout, even if --write-to-db is not set."
    " This is useful when debugging",
)
@click.option(
    "--log-level",
    default="warning",
    help="Set the python logging log level",
    show_default=True,
)
def main(
    config_path: pathlib.Path,
    timestamp: dt.datetime | None,
    max_pvs: int | None,
    write_to_db: bool,
    round_date_to_minutes: int | None,
    no_print_to_stdout: bool,
    log_level: str,
):
    """Main function"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
    )

    if timestamp is not None and round_date_to_minutes is not None:
        raise RuntimeError("You can not use both --date and --round-date-to-minutes")

    _log.debug("Load the configuration file")
    # Typically the configuration will contain many placeholders pointing to environment variables.
    # We allow specifying them in a .env file. See the .env.dist for a list of expected variables.
    # Environment variables still have precedence.

    # We remove the `None` values because that's how we typed `load_config`.
    dotenv_variables = {k: v for k, v in dotenv.dotenv_values().items() if v is not None}
    config = load_config(config_path, dotenv_variables | os.environ)

    if timestamp is None:
        timestamp = dt.datetime.utcnow()
        if round_date_to_minutes:
            timestamp = timestamp.replace(
                minute=int(timestamp.minute / round_date_to_minutes) * round_date_to_minutes,
                second=0,
                microsecond=0,
            )

    _log.info(f"Making predictions with now={timestamp}.")

    get_model = import_from_module(config["run_model_func"])

    _log.debug("Connecting to pv database")
    url = config["pv_db_url"]

    database_connection = DatabaseConnection(url, echo=False)

    # Wrap into a PV data source for the models.
    _log.info("Creating PV data source")
    pv_data_source = DbPvDataSource(database_connection)

    with profile("Loading model"):
        model: PvSiteModel = get_model(config, pv_data_source)

    pv_ids = pv_data_source.list_pv_ids()
    _log.info(f"Found {len(pv_ids)} sites")

    if max_pvs is not None:
        pv_ids = pv_ids[:max_pvs]
        _log.info(f"Keeping only {len(pv_ids)} sites")

    num_successes = 0
    for pv_id in pv_ids:
        success = _run_model_and_save_for_one_pv(
            database_connection=database_connection,
            model=model,
            pv_id=pv_id,
            timestamp=timestamp,
            write_to_db=write_to_db,
            print_to_stdout=not write_to_db and not no_print_to_stdout,
        )
        if success:
            num_successes += 1

    num_errors = len(pv_ids) - num_successes

    _log.info(
        f"Ran successfully on {num_successes} PV sites ({num_successes / len(pv_ids) * 100:.1f} %) "
    )
    _log.info(f"Errored on {num_errors} PV sites ({num_errors / len(pv_ids) * 100:.1f} %)")


if __name__ == "__main__":
    main()
