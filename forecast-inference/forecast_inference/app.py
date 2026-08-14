"""
Apply the model to the PVs in the database and note the results.
"""

import asyncio
import datetime as dt
import importlib.metadata
import logging
import os
import pathlib
from uuid import UUID

import click
import dotenv
import numpy as np
import sentry_sdk
from psp.models.base import PvSiteModel
from psp.typings import PvId, Timestamp, X
from pvsite_datamodel.connection import DatabaseConnection
from pvsite_datamodel.sqlmodels import ForecastSQL, ForecastValueSQL

from forecast_inference.data.nwp_data_sources import (
    download_and_add_osgb_to_nwp_data_source,
)
from forecast_inference.data.pv_data_sources import DbPvDataSource
from forecast_inference.data_platform import (
    DataPlatformClient,
    LocationSummary,
    fetch_dp_location_map,
    get_dataplatform_client,
    save_forecast_to_dataplatform,
)
from forecast_inference.utils.config import load_config
from forecast_inference.utils.imports import import_from_module
from forecast_inference.utils.profiling import profile

logging.basicConfig(
    level=getattr(logging, os.getenv("LOGLEVEL", "INFO")),
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)

# Get rid of the verbose logs
logging.getLogger("sqlalchemy").setLevel(logging.ERROR)
logging.getLogger("aiobotocore").setLevel(logging.ERROR)
logging.getLogger("aiobotocore").setLevel(logging.ERROR)


version = importlib.metadata.version("forecast_inference")

sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"), environment=os.getenv("ENVIRONMENT", "local"), traces_sample_rate=1
)

sentry_sdk.set_tag("app_name", "pv-site-production_forecast_inferance")
sentry_sdk.set_tag("version", version)




async def _run_model_and_save_for_one_pv(
    database_connection: DatabaseConnection,
    model: PvSiteModel,
    pv_id: PvId,
    timestamp: Timestamp,
    write_to_db: bool,
    print_to_stdout: bool,
    save_to_data_platform: bool = False,
    site_meta: dict | None = None,
    client: DataPlatformClient | None = None,
    dp_location_map: dict[str, LocationSummary] | None = None,
) -> bool:
    """
    Run model and save to database for one PV.

    When save_to_data_platform is True, also pushes the forecast to the Data Platform
    after a successful DB write. site_meta must contain client_location_name, capacity_kw,
    latitude, and longitude for the given pv_id.

    Return:
    ------
        True on success and False if there was an error.
    """
    with profile(f'Applying model on pv "{pv_id}"'):
        try:
            pred = model.predict(X(pv_id=pv_id, ts=timestamp))
        except Exception:
            log.exception(
                'There was an exception calling `model.predict` for pv_id="{pv_id}". Skipping.',
            )
            return False

    site_uuid = UUID(pv_id)

    # Assemble the data in ForecastValuesSQL rows for the database.
    rows = [
        {
            "start_utc": timestamp + dt.timedelta(minutes=start),
            "end_utc": timestamp + dt.timedelta(minutes=end),
            "forecast_power_kw": np.round(value, 3),
            "horizon_minutes": start,
        }
        for (start, end), value in zip(model.config.horizons, pred.powers)
    ]

    if write_to_db:
        with (
            profile(f'Writing {len(rows)} forecast values to db for pv "{pv_id}"'),
            database_connection.get_session() as session,
        ):
            forecast = ForecastSQL(
                location_uuid=site_uuid,  # type: ignore
                forecast_version="0.0.0",  # TODO get version
                timestamp_utc=timestamp,
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

    # Optionally push to the Data Platform
    if save_to_data_platform and site_meta is not None and client is not None:
        log.info(f"Saving to Data Platform for pv_id={pv_id}...")
        await save_forecast_to_dataplatform(
            rows=rows,
            client_location_name=site_meta["client_location_name"],
            model_tag="pv-site-production",
            init_time_utc=timestamp,
            client=client,
            capacity_kw=site_meta["capacity_kw"],
            latitude=site_meta["latitude"],
            longitude=site_meta["longitude"],
            location_map=dp_location_map,
        )
        log.info(f"Saving to Data Platform completed for pv_id={pv_id}")

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
@click.option(
    "--raise-on-failure",
    is_flag=True,
    default=False,
    help="Exit with an error if any PV site processing fails.",
)
def main(
    config_path: pathlib.Path,
    timestamp: dt.datetime | None,
    max_pvs: int | None,
    write_to_db: bool,
    round_date_to_minutes: int | None,
    no_print_to_stdout: bool,
    raise_on_failure: bool,
    log_level: str,
):
    """Main function"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
    )

    if timestamp is not None and round_date_to_minutes is not None:
        raise RuntimeError("You can not use both --date and --round-date-to-minutes")

    log.debug("Load the configuration file")
    # Typically the configuration will contain many placeholders pointing to environment variables.
    # We allow specifying them in a .env file. See the .env.dist for a list of expected variables.
    # Environment variables still have precedence.

    # We remove the `None` values because that's how we typed `load_config`.
    dotenv_variables = {k: v for k, v in dotenv.dotenv_values().items() if v is not None}
    config = load_config(config_path, dotenv_variables | os.environ)

    if timestamp is None:
        # Naive UTC by convention, to match GenerationSQL/ForecastSQL's naive DateTime columns.
        timestamp = dt.datetime.utcnow()  # noqa: DTZ003
        if round_date_to_minutes:
            timestamp = timestamp.replace(
                minute=int(timestamp.minute / round_date_to_minutes) * round_date_to_minutes,
                second=0,
                microsecond=0,
            )

    log.info(f"Making predictions with now={timestamp}.")

    get_model = import_from_module(config["run_model_func"])

    log.debug("Connecting to pv database")
    url = config["pv_db_url"]

    database_connection = DatabaseConnection(url, echo=False)

    # download and add osbg to nwp datasource
    nwp_zarr_path = os.getenv("NWP_ZARR_PATH")
    if nwp_zarr_path is not None:
        download_and_add_osgb_to_nwp_data_source(
            nwp_zarr_path, "nwp.zarr", variables_to_keep=config["nwp"]["kwargs"]["variables"]
        )

    # Wrap into a PV data source for the models.
    log.info("Creating PV data source")
    pv_data_source = DbPvDataSource(database_connection)

    with profile("Loading model"):
        model: PvSiteModel = get_model(config, pv_data_source)

    pv_ids = pv_data_source.list_pv_ids()
    log.info(f"Found {len(pv_ids)} sites")

    if max_pvs is not None:
        pv_ids = pv_ids[:max_pvs]
        log.info(f"Keeping only {len(pv_ids)} sites")

    # Read Data Platform flag
    save_to_dp = os.getenv("SAVE_TO_DATA_PLATFORM", "false").lower() == "true"

    # Pre-fetch site metadata (single DB query via pv_data_source — avoids per-PV round-trips)
    site_metadata = pv_data_source.get_site_metadata()
    log.info(f"Pre-fetched metadata for {len(site_metadata)} sites")

    async def _run_app():
        num_successes = 0
        if save_to_dp:
            async with get_dataplatform_client() as client:
                dp_location_map = await fetch_dp_location_map(client)
                log.info(f"Pre-fetched {len(dp_location_map)} DP site locations.")
                for pv_id in pv_ids:
                    success = await _run_model_and_save_for_one_pv(
                        database_connection=database_connection,
                        model=model,
                        pv_id=pv_id,
                        timestamp=timestamp,
                        write_to_db=write_to_db,
                        print_to_stdout=not write_to_db and not no_print_to_stdout,
                        save_to_data_platform=True,
                        site_meta=site_metadata.get(pv_id),
                        client=client,
                        dp_location_map=dp_location_map,
                    )
                    if success:
                        num_successes += 1
        else:
            for pv_id in pv_ids:
                success = await _run_model_and_save_for_one_pv(
                    database_connection=database_connection,
                    model=model,
                    pv_id=pv_id,
                    timestamp=timestamp,
                    write_to_db=write_to_db,
                    print_to_stdout=not write_to_db and not no_print_to_stdout,
                    save_to_data_platform=False,
                    site_meta=site_metadata.get(pv_id),
                )
                if success:
                    num_successes += 1
        return num_successes

    num_successes = asyncio.run(_run_app())

    num_errors = len(pv_ids) - num_successes

    log.info(
        f"Ran successfully on {num_successes} PV sites ({num_successes / len(pv_ids) * 100:.1f}%)"
    )
    log.info(f"Errored on {num_errors} PV sites ({num_errors / len(pv_ids) * 100:.1f}%)")

    # If requested, raise if any site failed
    if raise_on_failure and num_errors > 0:
        raise RuntimeError(f"{num_errors} PV site(s) failed out of {len(pv_ids)}")

    # Raise an error if all forecasts fail
    if num_successes == 0:
        raise RuntimeError("All forecasts failed")


if __name__ == "__main__":
    main()
