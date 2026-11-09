import asyncio
import logging
import pathlib
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from betterproto.lib.google.protobuf import Struct, Value
from freezegun import freeze_time
from ocf import dp
from pvsite_datamodel.sqlmodels import ForecastSQL, ForecastValueSQL, LocationSQL

from forecast_inference.app import _run_one_pv_and_continue_on_error, main
from forecast_inference.data.pv_data_sources import DbPvDataSource
from forecast_inference.utils.testing import run_click_script

CONFIG_FIXTURES = [
    x for x in pathlib.Path("tests/fixtures/model_configs").iterdir() if x.suffix == ".yaml"
]


@pytest.mark.parametrize("config_file", CONFIG_FIXTURES)
@pytest.mark.parametrize("write_to_db", [True, False])
def test_app(config_file: pathlib.Path, write_to_db: bool, db_session, now):

    # The script creates its own Database Connection object so it's not possible to use the
    # `db_session` defined in `conftest.py` that automatically removes the rows.
    # Because of this we compare rows before and rows after.
    def get_num_rows() -> dict[str, int]:
        return {
            table.__table__.name: db_session.query(table).count()  # type: ignore
            for table in [ForecastSQL, ForecastValueSQL]
            # TODO Currently nothing is written in LatestForecastValueSQL
            # , LatestForecastValueSQL]
        }

    num_rows_before = get_num_rows()

    cmd_args = ["--config", str(config_file), "--date", now.strftime("%Y-%m-%d-%H-%M")]
    cmd_args.append("--write-to-db" if write_to_db else "--no-write-to-db")

    result = run_click_script(main, cmd_args)
    assert result.exit_code == 0

    num_rows_after = get_num_rows()

    # Make sure forecast are written in the DB when we passe the --write-to-db option, and that
    # none are written otherwise.
    for table_name, num_rows in num_rows_after.items():
        if write_to_db:
            assert num_rows > num_rows_before[table_name]
        else:
            assert num_rows == num_rows_before[table_name]


def test_app_can_not_use_both_date_and_round_to_minutes(now):
    cmd_args = [
        "--config",
        "tests/fixtures/model_configs/cos.yaml",
        "--date",
        "2023-01-01-00-01",
        "--round-date-to-minutes",
        "10",
    ]

    result = run_click_script(main, cmd_args, catch_exceptions=True)
    assert result.exit_code != 0
    assert "can not use both" in str(result.exception)


def test_app_runs_every_site_then_fails_when_a_site_fails(monkeypatch):
    monkeypatch.delenv("SAVE_TO_DATA_PLATFORM", raising=False)
    cmd_args = ["--config", "tests/fixtures/model_configs/cos.yaml", "--raise-on-failure"]

    with (
        patch("forecast_inference.app.DbPvDataSource.list_pv_ids", return_value=["bad", "good"]),
        patch("forecast_inference.app.DbPvDataSource.get_site_metadata", return_value={}),
        patch(
            "forecast_inference.app._run_model_and_save_for_one_pv",
            new=AsyncMock(side_effect=[RuntimeError("boom"), True]),
        ) as mock_run,
    ):
        result = run_click_script(main, cmd_args, catch_exceptions=True)

    assert mock_run.call_count == 2
    assert result.exit_code != 0
    assert "1 PV site(s) failed" in str(result.exception)


@pytest.mark.parametrize("has_site_database_uuid", [True, False])
def test_dp_site_forecast_is_written_to_its_database_site(
    monkeypatch, database_connection, has_site_database_uuid
):
    """Load sites from the DP and write to the DB: only works with `site_database_uuid`."""
    monkeypatch.setenv("LOAD_SITES_FROM_DATA_PLATFORM", "true")

    with database_connection.get_session() as session:
        site_uuid = str(session.query(LocationSQL).first().location_uuid)

    dp_uuid = "019f8397-c1b4-7f54-b09a-b62142a06688"
    metadata = {"site_database_uuid": Value(string_value=site_uuid)} if has_site_database_uuid else {}
    location = dp.ListLocationsResponseLocationSummary(
        location_uuid=dp_uuid,
        location_name="dp_site",
        energy_source=dp.EnergySource.SOLAR,
        effective_capacity_watts=4000,
        latlng=dp.LatLng(latitude=51.0, longitude=3.0),
        metadata=Struct(fields=metadata),
    )
    site_id = site_uuid if has_site_database_uuid else dp_uuid

    model = MagicMock()
    model.config.horizons = [(0, 15)]
    model.predict.return_value = MagicMock(powers=[1.0])

    with (
        patch("forecast_inference.data_platform.load.get_dataplatform_client"),
        patch(
            "forecast_inference.data_platform.load.fetch_dp_location_map",
            new=AsyncMock(return_value={site_id: location}),
        ),
    ):
        pv_ids = DbPvDataSource(database_connection).list_pv_ids()

    assert pv_ids == [site_id]

    def count_forecasts() -> int:
        with database_connection.get_session() as session:
            return session.query(ForecastSQL).filter(ForecastSQL.location_uuid == site_uuid).count()

    num_forecasts_before = count_forecasts()

    success = asyncio.run(
        _run_one_pv_and_continue_on_error(
            database_connection=database_connection,
            model=model,
            pv_id=pv_ids[0],
            timestamp=datetime(2021, 1, 1),
            write_to_db=True,
            print_to_stdout=False,
        )
    )

    # Without `site_database_uuid` the pv id is the DP uuid, which isn't in the database
    assert success is has_site_database_uuid
    assert count_forecasts() - num_forecasts_before == (1 if has_site_database_uuid else 0)


@pytest.mark.parametrize(
    # Naive UTC by convention: expected_timestamp is string-matched against app.py's log
    # output, which is built from the naive `datetime.datetime.utcnow()`.
    "round_to,timestamp,expected_timestamp",
    [
        # No rounding by default.
        [
            None,
            datetime(2000, 12, 30, 23, 59, 59, 123456),
            datetime(2000, 12, 30, 23, 59, 59, 123456),
        ],
        [None, datetime(2000, 12, 30), datetime(2000, 12, 30)],
        # "Floor" the minutes when we want to round.
        [
            5,
            datetime(2000, 12, 30, 23, 59, 59, 123456),
            datetime(2000, 12, 30, 23, 55),
        ],
        [
            10,
            datetime(2000, 12, 30, 23, 59, 59, 123456),
            datetime(2000, 12, 30, 23, 50),
        ],
    ],
)
def test_app_no_round_date(round_to, timestamp, expected_timestamp, caplog):
    """
    Test that if the app.py runs at time `timestamp`
    with --round-date-to-minutes=`round_to`
    then we actually run predictions for `expected_timestamp`.
    """
    # The date is logged with level INFO, that's where we'll check if it's right.
    caplog.set_level(logging.INFO)

    # Here we make sure that datetime.datetime.utcnow() == `timestamp`.
    with freeze_time(timestamp):
        cmd_args = [
            "--config",
            "tests/fixtures/model_configs/cos.yaml",
        ]
        if round_to is not None:
            cmd_args.extend(
                [
                    "--round-date-to-minutes",
                    str(round_to),
                ]
            )

        run_click_script(main, cmd_args)

        # Check that we logged the right "now" timestamp.
        assert f"Making predictions with now={expected_timestamp}" in caplog.text
