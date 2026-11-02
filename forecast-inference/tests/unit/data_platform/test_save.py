"""Unit tests for the Data Platform save module.

These tests require no Docker or network access — they test pure logic only.
"""

import asyncio
import datetime as dt
from unittest.mock import AsyncMock, MagicMock

import pytest
from ocf import dp

from forecast_inference.app import _run_model_and_save_for_one_pv
from forecast_inference.data_platform.client import (
    _sanitize,
    fetch_dp_location_map,
    find_dp_location,
)
from forecast_inference.data_platform.save import (
    prepare_forecast_values,
    save_forecast_to_dataplatform,
)


class TestFindDpLocation:
    """A site's location is found by name: its id, or else its client name."""

    # the uuid that is stored in the database
    site_uuid = "006a6820-7470-483f-9c74-14e9c76ab47a"

    # this is what the location name looks like when we have renamed it in data-platform
    uuid_name = "006a6820_7470_483f_9c74_14e9c76ab47a"

    def test_prefers_uuid_name(self):
        loc_map = {self.uuid_name: "by_uuid", "bf_home": "by_name"}
        assert find_dp_location(loc_map, self.site_uuid, "BF Home") == "by_uuid"

    def test_falls_back_to_client_name(self):
        assert find_dp_location({"bf_home": "by_name"}, self.site_uuid, "BF Home") == "by_name"

    def test_not_found(self):
        assert find_dp_location({}, self.site_uuid, None) is None


@pytest.mark.parametrize("write_to_db", [True, False])
def test_location_name_is_only_a_uuid_when_writing_to_db(write_to_db):
    model = MagicMock()
    model.config.horizons = [(0, 15)]
    model.predict.return_value = MagicMock(powers=[1.0])
    pv_id = "006a6820_7470_483f_9c74_14e9c76ab47a" if write_to_db else "bf_home"
    database_connection = MagicMock()

    success = asyncio.run(
        _run_model_and_save_for_one_pv(
            database_connection=database_connection,
            model=model,
            pv_id=pv_id,
            timestamp=dt.datetime(2024, 1, 1),
            write_to_db=write_to_db,
            print_to_stdout=False,
        )
    )

    assert success is True
    assert database_connection.get_session.called is write_to_db
    if write_to_db:
        session = database_connection.get_session.return_value.__enter__.return_value
        forecast = session.add.call_args[0][0]
        assert str(forecast.location_uuid) == "006a6820-7470-483f-9c74-14e9c76ab47a"


class TestSanitization:
    """Verify name sanitization handles all DP-invalid characters."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("BF Home", "bf_home"),
            ("pvoutput.org_10968", "pvoutput_org_10968"),
            ("zaks-house-isnt-here", "zaks_house_isnt_here"),
            ("Rough Grounds Farm Campsite", "rough_grounds_farm_campsite"),
            ("trust-power-site-1", "trust_power_site_1"),
            ("16223", "16223"),
            ("normal_name", "normal_name"),
            ("mixed@chars#here!", "mixed_chars_here_"),
            # pipe is explicitly allowed by DP schema
            ("region|uk", "region|uk"),
        ],
    )
    def test_sanitize(self, raw: str, expected: str):
        assert _sanitize(raw) == expected


class TestPrepareForecastValues:
    """Verify kW → fraction conversion and horizon calculation."""

    @pytest.fixture()
    def rows(self):
        """10 forecast rows at 15-min intervals from a fixed UTC time."""
        n = 10
        step = 15
        init_utc = dt.datetime(2024, 6, 1, 6, 0, 0, tzinfo=dt.UTC)
        return [
            {
                "start_utc": init_utc + dt.timedelta(minutes=i * step),
                "end_utc": init_utc + dt.timedelta(minutes=(i + 1) * step),
                "forecast_power_kw": float(i),
                "horizon_minutes": i * step,
            }
            for i in range(n)
        ]

    @pytest.fixture()
    def init_time(self, rows):
        return rows[0]["start_utc"]

    def _prepare(self, rows, init_time, capacity_watts=4000):
        return prepare_forecast_values(rows, init_time, capacity_watts)

    def test_returns_correct_count(self, rows, init_time):
        values = self._prepare(rows, init_time)
        assert len(values) == len(rows)

    def test_power_fraction_is_clamped(self, init_time):
        """Fraction must not exceed 1.0 even if power >> capacity."""
        rows = [
            {
                "start_utc": init_time,
                "end_utc": init_time + dt.timedelta(minutes=15),
                "forecast_power_kw": 999.0,
                "horizon_minutes": 0,
            }
        ]
        values = self._prepare(rows, init_time, capacity_watts=1000)
        assert values[0].p50_fraction <= 1.0

    def test_power_fraction_is_non_negative(self, rows, init_time):
        values = self._prepare(rows, init_time)
        for v in values:
            assert v.p50_fraction >= 0.0

    def test_horizon_derived_from_start_utc_when_missing(self):
        """When horizon_minutes is absent, horizon is derived from timestamps."""
        init = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.UTC)
        rows = [
            {
                "start_utc": init + dt.timedelta(minutes=30),
                "end_utc": init + dt.timedelta(minutes=45),
                "forecast_power_kw": 1.0,
                # no horizon_minutes key
            }
        ]
        values = self._prepare(rows, init, capacity_watts=2000)
        assert values[0].horizon_mins == 30

    def test_fraction_correct_value(self):
        """2 kW against 4 kW capacity → p50_fraction == 0.5."""
        init = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
        rows = [
            {
                "start_utc": init,
                "end_utc": init + dt.timedelta(minutes=15),
                "forecast_power_kw": 2.0,
                "horizon_minutes": 0,
            }
        ]
        values = self._prepare(rows, init, capacity_watts=4000)
        assert abs(values[0].p50_fraction - 0.5) < 1e-6
