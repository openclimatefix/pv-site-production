import datetime as dt
import logging
import pathlib
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from freezegun import freeze_time

from forecast_inference.app import main
from forecast_inference.utils.testing import run_click_script

CONFIG_FIXTURES = [
    x for x in pathlib.Path("tests/fixtures/model_configs").iterdir() if x.suffix == ".yaml"
]


def _mock_dp_context(client):
    ctx = AsyncMock()
    ctx.__aenter__.return_value = client
    return ctx


@contextmanager
def _mock_dp_environment(now, n_generations=100):
    """Patch every Data Platform touchpoint app.py's full run goes through, with one mock site.

    Covers site listing/metadata (`pv_data_sources.py`) and generation reads (`load.py`).
    `SAVE_TO_DATA_PLATFORM` isn't set by these tests, so the save-side plumbing isn't exercised.
    """
    dp_location_uuid = "dp-uuid-app-test"
    dp_location_name = "test_site"

    tilt_value = MagicMock()
    tilt_value.number_value = 30.0
    orientation_value = MagicMock()
    orientation_value.number_value = 180.0

    summary = MagicMock()
    summary.location_uuid = dp_location_uuid
    summary.location_name = dp_location_name
    summary.latlng.latitude = 51.0
    summary.latlng.longitude = 3.0
    summary.effective_capacity_watts = 4000
    summary.metadata.fields = {"tilt": tilt_value, "orientation": orientation_value}

    obs_values = []
    for i in range(n_generations):
        val = MagicMock()
        val.timestamp_utc = now - dt.timedelta(minutes=i + 1)
        val.value_fraction = i / n_generations
        val.effective_capacity_watts = 4000
        obs_values.append(val)
    mock_obs_resp = MagicMock()
    mock_obs_resp.values = obs_values

    mock_client = AsyncMock()
    mock_client.get_observations_as_timeseries.return_value = mock_obs_resp

    loc_map = {dp_location_name: summary}

    with (
        patch(
            "forecast_inference.data.pv_data_sources.get_dataplatform_client",
            return_value=_mock_dp_context(mock_client),
        ),
        patch(
            "forecast_inference.data.pv_data_sources.fetch_dp_location_map",
            new=AsyncMock(return_value=loc_map),
        ),
        patch(
            "forecast_inference.data_platform.load.get_dataplatform_client",
            return_value=_mock_dp_context(mock_client),
        ),
        patch(
            "forecast_inference.data_platform.load.fetch_dp_location_map",
            new=AsyncMock(return_value=loc_map),
        ),
    ):
        yield dp_location_uuid


@pytest.mark.parametrize("config_file", CONFIG_FIXTURES)
def test_app(config_file: pathlib.Path, now):
    with _mock_dp_environment(now):
        cmd_args = ["--config", str(config_file), "--date", now.strftime("%Y-%m-%d-%H-%M")]
        result = run_click_script(main, cmd_args)

    assert result.exit_code == 0
    assert "PV Site" in result.output


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


@pytest.mark.parametrize(
    # Naive UTC by convention: expected_timestamp is string-matched against app.py's log
    # output, which is built from the naive `datetime.datetime.utcnow()`.
    "round_to,timestamp,expected_timestamp",
    [
        # No rounding by default.
        [
            None,
            datetime(2000, 12, 30, 23, 59, 59, 123456),  # noqa: DTZ001
            datetime(2000, 12, 30, 23, 59, 59, 123456),  # noqa: DTZ001
        ],
        [None, datetime(2000, 12, 30), datetime(2000, 12, 30)],  # noqa: DTZ001
        # "Floor" the minutes when we want to round.
        [
            5,
            datetime(2000, 12, 30, 23, 59, 59, 123456),  # noqa: DTZ001
            datetime(2000, 12, 30, 23, 55),  # noqa: DTZ001
        ],
        [
            10,
            datetime(2000, 12, 30, 23, 59, 59, 123456),  # noqa: DTZ001
            datetime(2000, 12, 30, 23, 50),  # noqa: DTZ001
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
    with freeze_time(timestamp), _mock_dp_environment(timestamp):
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
