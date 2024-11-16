"""Delete the forecasts made before a certain date.

This process can be batched and optionally we can wait between each batch to not overload the
database with work.
"""

import contextlib
import datetime as dt
import logging
import os
import time
import uuid
import fsspec
from typing import Optional

import click
import importlib.metadata
import sentry_sdk
import sqlalchemy as sa
from pvsite_datamodel.sqlmodels import ForecastSQL, ForecastValueSQL
from sqlalchemy.orm import Session, sessionmaker
import pandas as pd


logging.basicConfig(
    level=getattr(logging, os.getenv("LOGLEVEL", "INFO")),
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
)
_log = logging.getLogger(__name__)
# Get rid of the verbose logs
logging.getLogger("sqlalchemy").setLevel(logging.ERROR)
logging.getLogger("aiobotocore").setLevel(logging.ERROR)


version = importlib.metadata.version("database-cleanup")

sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"), environment=os.getenv("ENVIRONMENT", "local"), traces_sample_rate=1
)

sentry_sdk.set_tag("app_name", "pv-site-production_database_cleanup")
sentry_sdk.set_tag("version", version)


@contextlib.contextmanager
def _profile(msg: str):
    _log.debug(msg)
    t0 = time.time()
    yield
    t1 = time.time()
    _log.debug(f"Done in {t1 - t0:.3f}s")


def _get_forecasts(session: Session, max_date: dt.datetime, limit: int) -> list[uuid.UUID]:
    """Get the `limit` older forecasts that are before `max_date`."""
    stmt = (
        sa.select(ForecastSQL.forecast_uuid)
        .where(ForecastSQL.timestamp_utc < max_date)
        .order_by(ForecastSQL.timestamp_utc)
        .limit(limit)
    )

    return session.scalars(stmt).all()


def _delete_forecasts_and_values(session: Session, forecast_uuids: list[uuid.UUID]) -> None:
    """Delete the forecasts and their corresponding values."""
    with _profile(f"Deleting forecast values for {len(forecast_uuids)} forecasts."):
        stmt = sa.delete(ForecastValueSQL).where(ForecastValueSQL.forecast_uuid.in_(forecast_uuids))
        session.execute(stmt)

    with _profile(f"Deleting {len(forecast_uuids)} forecasts"):
        stmt = sa.delete(ForecastSQL).where(ForecastSQL.forecast_uuid.in_(forecast_uuids))
        session.execute(stmt)


def save_forecast_and_values(
    session: Session, forecast_uuids: list[uuid.UUID], directory: str, index: int = 0
):
    """
    Save forecast and forecast values to csv
    :param session: database session
    :param forecast_uuids: list of forecast uuids
    :param directory: the directory where they should be saved
    :param index: the index of the file, we delete the forecasts in batches,
        so there will be several files to save
    """
    _log.info(f"Saving data to {directory}")

    fs = fsspec.open(directory).fs
    # check folder exists, if it doesnt, add it
    if not fs.exists(directory):
        fs.mkdir(directory)

    # loop over both forecast and forecast_values tables
    for table in ["forecast", "forecast_value"]:
        model = ForecastSQL if table == "forecast" else ForecastValueSQL

        # get data
        query = session.query(model).where(model.forecast_uuid.in_(forecast_uuids))
        forecasts_sql = query.all()
        forecasts_df = pd.DataFrame([f.__dict__ for f in forecasts_sql])

        # save to csv
        _log.info(f"saving to {directory}, Saving {len(forecasts_df)} rows to {table}.csv")
        forecasts_df.to_csv(f"{directory}/{table}_{index}.csv", index=False)


@click.command()
@click.option(
    "--date",
    required=False,
    help="Datetime (UTC) before which to delete, format: YYYY-MM-DD HH:mm."
    ' Defaults to "00:00, 3 days ago".',
)
@click.option(
    "--batch-size",
    default=100,
    help="Number of forecasts to delete in one batch."
    " (Note that this means orders of magnitude more Forecast *Values*).",
    show_default=True,
)
@click.option(
    "--save-dir",
    default=None,
    envvar="SAVE_DIR",
    help="The directory where we save the delete forecasts and values.",
    show_default=True,
)
@click.option(
    "--sleep",
    type=float,
    default=0,
    help="How much time to wait between batches in seconds",
    show_default=True,
)
@click.option(
    "--log-level",
    default="info",
    show_default=True,
)
@click.option(
    "--do-delete", is_flag=True, help="Actually delete the rows. By default we only do a dry run."
)
def main(
    date: dt.datetime,
    batch_size: int,
    save_dir: Optional[str],
    sleep: int,
    do_delete: bool,
    log_level: str,
):
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
    )

    date = format_date(date)

    db_url = os.environ["DB_URL"]
    engine = sa.create_engine(db_url)
    Session = sessionmaker(engine, future=True)

    if save_dir is not None:
        save_dir = f"{save_dir}/{date.isoformat()}"
    _log.info(f"Saving data to {save_dir}")

    if do_delete:
        _log.info(f"Deleting forecasts made before {date} (UTC).")
    else:
        _log.info(f"Would delete forecasts made before {date} (UTC).")

    num_forecast_deleted = 0

    i = -1
    while True:
        i += 1
        with Session.begin() as session:
            forecast_uuids = _get_forecasts(
                session,
                max_date=date,
                limit=batch_size,
            )

            if len(forecast_uuids) == 0:
                _log.info(f"Done deleting forecasts made before {date}")
                _log.info(
                    f"A total of {num_forecast_deleted} (and corresponding values) "
                    f"were deleted from the database."
                )
                _log.info("Exiting.")
                return

            if (save_dir is not None) and do_delete:
                save_forecast_and_values(
                    session=session,
                    forecast_uuids=forecast_uuids,
                    directory=save_dir,
                    index=i,
                )

        if do_delete:
            # Not that it is important to run this in a transaction for atomicity.
            with Session.begin() as session:
                _delete_forecasts_and_values(session, forecast_uuids)
        else:
            print(f"Would delete data from {len(forecast_uuids)} forecasts in a first batch.")
            # Stop here because otherwise we would loop infinitely.
            return

        num_forecast_deleted += len(forecast_uuids)

        if sleep:
            _log.debug(f"Sleeping for {sleep} seconds")
            time.sleep(sleep)


def format_date(date) -> dt.datetime:
    """
    Format the date to a datetime object
    :param date: None, or string in the format "YYYY-MM-DD HH:mm"
    :return:
    """
    if date is None:
        date = (dt.date.today() - dt.timedelta(days=3)).strftime("%Y-%m-%d 01:00")

    date = dt.datetime.strptime(date, "%Y-%m-%d %H:%M")

    return date


if __name__ == "__main__":
    main()
