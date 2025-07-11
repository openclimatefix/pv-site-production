import datetime as dt
import uuid
import os
import tempfile
import pandas as pd

import pytest
import sqlalchemy as sa
from click.testing import CliRunner
from database_cleanup.app import main, format_date
from freezegun import freeze_time
from pvsite_datamodel.sqlmodels import ForecastSQL, ForecastValueSQL, LocationSQL, LocationGroupSQL
from sqlalchemy.orm import Session


def _add_foreasts(
    session: Session,
    *,
    site_uuid: str,
    timestamps: list[dt.datetime],
    num_values: int,
    frequency: int,
):
    for timestamp in timestamps:
        forecast = ForecastSQL(
            location_uuid=site_uuid, timestamp_utc=timestamp, forecast_version="0"
        )
        session.add(forecast)
        session.commit()

        for i in range(num_values):
            # N forecasts every minute.
            fv = ForecastValueSQL(
                forecast_uuid=forecast.forecast_uuid,
                forecast_power_kw=i,
                horizon_minutes=i,
                start_utc=timestamp + dt.timedelta(minutes=i * frequency),
                end_utc=timestamp + dt.timedelta(minutes=(i + 1) * frequency),
            )
            session.add(fv)
        session.commit()


def _run_cli(func, args: list[str]):
    runner = CliRunner()
    result = runner.invoke(func, args, catch_exceptions=True)

    # Without this the output to stdout/stderr is grabbed by click's test runner.
    if result.output:
        print(result.output)

    # In case of an exception, raise it so that the test fails with the exception.
    if result.exception:
        raise result.exception

    assert result.exit_code == 0


@pytest.fixture
def site(session):
    # create SiteGroupSQL
    now = pd.Timestamp.now().isoformat()
    site_group = LocationGroupSQL(location_group_name=f"test_group_name_{now}", service_level=1)
    session.add(site_group)
    session.commit()

    # Create a new site (this way we know it won't have any forecasts yet).
    site = LocationSQL(ml_id=hash(uuid.uuid4()) % 2147483647)
    session.add(site)
    session.commit()

    site_group.locations.append(site)
    session.commit()

    return site


@freeze_time("2020-01-11 00:01")
@pytest.mark.parametrize("batch_size", [None, 5, 20])
@pytest.mark.parametrize(
    "date_str,expected",
    [
        # `None` == use default, which means '2020-01-08'
        [None, 3],
        ["2019-12-31 23:59", 10],
        ["2020-01-01 00:00", 10],
        ["2020-01-02 00:00", 9],
        ["2020-01-09 00:00", 2],
        ["2020-01-10 00:00", 1],
        ["2020-01-30 00:00", 0],
    ],
)
def test_app(session: Session, site, batch_size: int, date_str: str | None, expected: int):
    # We'll only consider this site.
    site_uuid = site.location_uuid

    # Write some forecasts to the database for our site.
    num_forecasts = 10
    num_values = 9

    # make temp directory
    with tempfile.TemporaryDirectory() as tmpdirname:
        save_dir = tmpdirname

        timestamps = [dt.datetime(2020, 1, d + 1) for d in range(num_forecasts)]

        # Add forecasts for those.
        _add_foreasts(
            session,
            site_uuid=site_uuid,
            timestamps=timestamps,
            num_values=num_values,
            frequency=1,
        )

        # Run the script.
        args = ["--do-delete"]

        if date_str is not None:
            args.extend(["--date", date_str])

        if batch_size is not None:
            args.extend(["--batch-size", str(batch_size)])

        args.extend(["--save-dir", save_dir])

        _run_cli(main, args)

        # Check that we have the right number of rows left.
        # Only check for the site_uuid that we considered.
        num_forecasts_left = session.scalars(
            sa.select(sa.func.count())
            .select_from(ForecastSQL)
            .where(ForecastSQL.location_uuid == site_uuid)
        ).one()
        assert num_forecasts_left == expected

        num_values_left = session.scalars(
            sa.select(sa.func.count())
            .select_from(ForecastValueSQL)
            .join(ForecastSQL)
            .where(ForecastSQL.location_uuid == site_uuid)
        ).one()
        assert num_values_left == expected * num_values

        if num_forecasts_left < num_forecasts:
            # check that forecast.csv and forecast_values.csv are saved
            date = format_date(date_str).isoformat()
            assert os.path.exists(f"{tmpdirname}")
            assert os.path.exists(f"{tmpdirname}/{date}")
            assert os.path.exists(f"{tmpdirname}/{date}/forecast_0.csv")
            assert os.path.exists(f"{tmpdirname}/{date}/forecast_value_0.csv")

            forecast_df = pd.read_csv(f"{tmpdirname}/{date}/forecast_0.csv")
            forecast_value_df = pd.read_csv(f"{tmpdirname}/{date}/forecast_value_0.csv")
            for data in [forecast_df, forecast_value_df]:
                assert len(data) > 0


@freeze_time("2020-01-11 00:01")
@pytest.mark.parametrize("do_delete", [True, False])
def test_app_dry_run(session: Session, site, do_delete: bool):
    # We'll only consider this site.
    site_uuid = site.location_uuid

    # Write some forecasts to the database for our site.
    num_forecasts = 10
    num_values = 9

    timestamps = [dt.datetime(2020, 1, d + 1) for d in range(num_forecasts)]

    _add_foreasts(
        session,
        site_uuid=site_uuid,
        timestamps=timestamps,
        num_values=num_values,
        frequency=1,
    )

    args = []
    if do_delete:
        args.append("--do-delete")

    # Run the script.
    _run_cli(main, args)

    expected = 3 if do_delete else 10

    # Check that we have the right number of rows left.
    # Only check for the site_uuid that we considered.
    num_forecasts_left = session.scalars(
        sa.select(sa.func.count())
        .select_from(ForecastSQL)
        .where(ForecastSQL.location_uuid == site_uuid)
    ).one()
    assert num_forecasts_left == expected

    num_values_left = session.scalars(
        sa.select(sa.func.count())
        .select_from(ForecastValueSQL)
        .join(ForecastSQL)
        .where(ForecastSQL.location_uuid == site_uuid)
    ).one()
    assert num_values_left == expected * num_values
