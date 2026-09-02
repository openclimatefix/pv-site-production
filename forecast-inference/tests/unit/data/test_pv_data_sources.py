"""Unit tests for `DataPlatformPvDataSource`, the fully Data Platform-native PV data source."""

import asyncio
import datetime as dt
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from forecast_inference.data.pv_data_sources import DataPlatformPvDataSource
from forecast_inference.data_platform.load import (
    build_location_metadata,
    get_client_location_name,
)
from tests.dp_test_helpers import mock_dp_context, tilt_orientation_metadata_fields

dp_location_uuid = "dp-uuid-123"
dp_location_name = "test_dp_site"


def _mock_summary(
    location_uuid=dp_location_uuid,
    location_name=dp_location_name,
    latitude=51.0,
    longitude=-1.0,
    capacity_watts=4000,
    metadata_fields=None,
):
    """Build a MagicMock DP `LocationSummary`, with a real (non-auto-mocking) metadata dict.

    Defaults to having tilt/orientation set in metadata (required by `.get()`, which now
    raises when they're missing) — pass `metadata_fields={}` explicitly to test that case.
    """
    mock_summary = MagicMock()
    mock_summary.location_uuid = location_uuid
    mock_summary.location_name = location_name
    mock_summary.latlng.latitude = latitude
    mock_summary.latlng.longitude = longitude
    mock_summary.effective_capacity_watts = capacity_watts
    mock_summary.metadata.fields = (
        metadata_fields if metadata_fields is not None else tilt_orientation_metadata_fields()
    )
    return mock_summary


class TestBuildLocationMetadata:
    """Test the module-level `build_location_metadata` helper."""

    def test_returns_latitude_longitude_and_capacity(self):
        summary = _mock_summary(
            latitude=52.5, longitude=-2.5, capacity_watts=5000, metadata_fields={}
        )

        location = build_location_metadata(summary)

        assert location == {
            "latitude": 52.5,
            "longitude": -2.5,
            "capacity_kw": 5.0,
        }

    def test_includes_tilt_and_orientation_when_present_in_metadata(self):
        tilt_value = MagicMock()
        tilt_value.number_value = 35.0
        orientation_value = MagicMock()
        orientation_value.number_value = 170.0
        summary = _mock_summary(
            metadata_fields={"tilt": tilt_value, "orientation": orientation_value}
        )

        location = build_location_metadata(summary)

        assert location["tilt"] == 35.0
        assert location["orientation"] == 170.0

    def test_omits_tilt_and_orientation_when_absent_from_metadata(self):
        location = build_location_metadata(_mock_summary(metadata_fields={}))

        assert "tilt" not in location
        assert "orientation" not in location


class TestGetClientLocationName:
    """Test the module-level `get_client_location_name` helper."""

    def test_reads_from_metadata_when_present(self):
        name_value = MagicMock()
        name_value.string_value = "Original Client Name"
        summary = _mock_summary(
            location_name="sanitized_name", metadata_fields={"client_location_name": name_value}
        )

        assert get_client_location_name(summary) == "Original Client Name"

    def test_falls_back_to_sanitized_location_name(self):
        summary = _mock_summary(location_name="sanitized_name", metadata_fields={})

        assert get_client_location_name(summary) == "sanitized_name"


class TestDataPlatformPvDataSource:
    """Test `DataPlatformPvDataSource`."""

    def test_get_reads_generation_and_location_metadata_from_dp(self):
        mock_client = AsyncMock()

        mock_val = MagicMock()
        mock_val.timestamp_utc = dt.datetime(2024, 6, 1, 10, 0, tzinfo=dt.UTC)
        mock_val.value_fraction = 0.5
        mock_val.effective_capacity_watts = 5000  # 5 kW

        mock_obs_resp = MagicMock()
        mock_obs_resp.values = [mock_val]
        mock_client.get_observations_as_timeseries.return_value = mock_obs_resp

        summary = _mock_summary(capacity_watts=5000, latitude=52.5, longitude=-2.5)

        with (
            patch(
                "forecast_inference.data_platform.load.get_dataplatform_client",
                return_value=mock_dp_context(mock_client),
            ),
            patch(
                "forecast_inference.data_platform.load.fetch_dp_location_map_by_uuid",
                new=AsyncMock(return_value={dp_location_uuid: summary}),
            ),
        ):
            pv_data_source = DataPlatformPvDataSource()
            result = pv_data_source.get(
                [dp_location_uuid],
                start_ts=dt.datetime(2024, 6, 1, tzinfo=dt.UTC),
                end_ts=dt.datetime(2024, 6, 2, tzinfo=dt.UTC),
            )

        # Generation value: 0.5 fraction * 5000W / 1000 = 2.5 kW
        assert result["power"].sel(id=dp_location_uuid).values[0] == pytest.approx(2.5)
        assert result["latitude"].sel(id=dp_location_uuid).item() == pytest.approx(52.5)
        assert result["longitude"].sel(id=dp_location_uuid).item() == pytest.approx(-2.5)
        assert result["capacity"].sel(id=dp_location_uuid).item() == pytest.approx(5.0)
        assert result["tilt"].sel(id=dp_location_uuid).item() == pytest.approx(30.0)
        assert result["orientation"].sel(id=dp_location_uuid).item() == pytest.approx(180.0)

    def test_get_reads_tilt_and_orientation_from_dp_metadata_when_present(self):
        mock_client = AsyncMock()
        mock_obs_resp = MagicMock()
        mock_obs_resp.values = []
        mock_client.get_observations_as_timeseries.return_value = mock_obs_resp

        tilt_value = MagicMock()
        tilt_value.number_value = 35.0
        orientation_value = MagicMock()
        orientation_value.number_value = 170.0
        summary = _mock_summary(
            metadata_fields={"tilt": tilt_value, "orientation": orientation_value}
        )

        with (
            patch(
                "forecast_inference.data_platform.load.get_dataplatform_client",
                return_value=mock_dp_context(mock_client),
            ),
            patch(
                "forecast_inference.data_platform.load.fetch_dp_location_map_by_uuid",
                new=AsyncMock(return_value={dp_location_uuid: summary}),
            ),
        ):
            pv_data_source = DataPlatformPvDataSource()
            result = pv_data_source.get(
                [dp_location_uuid],
                start_ts=dt.datetime(2024, 6, 1, tzinfo=dt.UTC),
                end_ts=dt.datetime(2024, 6, 2, tzinfo=dt.UTC),
            )

        assert result["tilt"].sel(id=dp_location_uuid).item() == pytest.approx(35.0)
        assert result["orientation"].sel(id=dp_location_uuid).item() == pytest.approx(170.0)

    def test_get_site_not_found_in_dp_raises(self):
        """A site missing from the Data Platform entirely has no metadata at all, so `.get()`
        must fail hard rather than silently predicting on `NaN` inputs.
        """
        mock_client = AsyncMock()

        with (
            patch(
                "forecast_inference.data_platform.load.get_dataplatform_client",
                return_value=mock_dp_context(mock_client),
            ),
            patch(
                "forecast_inference.data_platform.load.fetch_dp_location_map_by_uuid",
                new=AsyncMock(return_value={}),
            ),
            pytest.raises(RuntimeError, match=dp_location_uuid),
        ):
            pv_data_source = DataPlatformPvDataSource()
            pv_data_source.get(
                [dp_location_uuid],
                start_ts=dt.datetime(2024, 6, 1, tzinfo=dt.UTC),
                end_ts=dt.datetime(2024, 6, 2, tzinfo=dt.UTC),
            )

        mock_client.get_observations_as_timeseries.assert_not_called()

    def test_get_raises_when_tilt_and_orientation_missing_from_dp_metadata(self):
        summary = _mock_summary(metadata_fields={})
        mock_client = AsyncMock()
        mock_obs_resp = MagicMock()
        mock_obs_resp.values = []
        mock_client.get_observations_as_timeseries.return_value = mock_obs_resp

        with (
            patch(
                "forecast_inference.data_platform.load.get_dataplatform_client",
                return_value=mock_dp_context(mock_client),
            ),
            patch(
                "forecast_inference.data_platform.load.fetch_dp_location_map_by_uuid",
                new=AsyncMock(return_value={dp_location_uuid: summary}),
            ),
            pytest.raises(RuntimeError, match="tilt"),
        ):
            pv_data_source = DataPlatformPvDataSource()
            pv_data_source.get(
                [dp_location_uuid],
                start_ts=dt.datetime(2024, 6, 1, tzinfo=dt.UTC),
                end_ts=dt.datetime(2024, 6, 2, tzinfo=dt.UTC),
            )

    def test_get_works_when_called_from_within_a_running_event_loop(self):
        """Regression test: `app.py` calls `model.predict()` (and so `.get()`) synchronously
        from inside `asyncio.run(_run_app())`. `.get()` must not try to `asyncio.run()` again
        on top of that already-running loop.
        """
        mock_client = AsyncMock()
        mock_obs_resp = MagicMock()
        mock_obs_resp.values = []
        mock_client.get_observations_as_timeseries.return_value = mock_obs_resp

        summary = _mock_summary()

        async def _call_get_from_within_a_running_loop():
            pv_data_source = DataPlatformPvDataSource()
            return pv_data_source.get(
                [dp_location_uuid],
                start_ts=dt.datetime(2024, 6, 1, tzinfo=dt.UTC),
                end_ts=dt.datetime(2024, 6, 2, tzinfo=dt.UTC),
            )

        with (
            patch(
                "forecast_inference.data_platform.load.get_dataplatform_client",
                return_value=mock_dp_context(mock_client),
            ),
            patch(
                "forecast_inference.data_platform.load.fetch_dp_location_map_by_uuid",
                new=AsyncMock(return_value={dp_location_uuid: summary}),
            ),
        ):
            # asyncio.run() here mirrors app.py's `asyncio.run(_run_app())`, under which
            # `.get()` is invoked synchronously — this must not raise
            # "asyncio.run() cannot be called from a running event loop".
            result = asyncio.run(_call_get_from_within_a_running_loop())

        assert result["latitude"].sel(id=dp_location_uuid).item() == pytest.approx(51.0)

    def test_location_map_is_cached_across_calls(self):
        """A single DataPlatformPvDataSource instance should only list DP locations once,
        not once per `.get()` call (which happens once per site in the real app loop).
        """
        mock_client = AsyncMock()
        mock_obs_resp = MagicMock()
        mock_obs_resp.values = []
        mock_client.get_observations_as_timeseries.return_value = mock_obs_resp

        summary = _mock_summary()
        mock_fetch_loc_map = AsyncMock(return_value={dp_location_uuid: summary})

        with (
            patch(
                "forecast_inference.data_platform.load.get_dataplatform_client",
                return_value=mock_dp_context(mock_client),
            ),
            patch(
                "forecast_inference.data_platform.load.fetch_dp_location_map_by_uuid",
                new=mock_fetch_loc_map,
            ),
        ):
            pv_data_source = DataPlatformPvDataSource()
            for _ in range(3):
                pv_data_source.get(
                    [dp_location_uuid],
                    start_ts=dt.datetime(2024, 6, 1, tzinfo=dt.UTC),
                    end_ts=dt.datetime(2024, 6, 2, tzinfo=dt.UTC),
                )

        mock_fetch_loc_map.assert_called_once()

    def test_list_pv_ids_and_get_site_metadata(self):
        name_value = MagicMock()
        name_value.string_value = "Original Name"
        summary = _mock_summary(
            location_name="sanitized_name",
            latitude=51.0,
            longitude=-1.0,
            capacity_watts=4000,
            metadata_fields={"client_location_name": name_value},
        )
        mock_fetch_loc_map = AsyncMock(return_value={dp_location_uuid: summary})

        with (
            patch(
                "forecast_inference.data.pv_data_sources.get_dataplatform_client",
                return_value=mock_dp_context(AsyncMock()),
            ),
            patch(
                "forecast_inference.data.pv_data_sources.fetch_dp_location_map_by_uuid",
                new=mock_fetch_loc_map,
            ),
        ):
            pv_data_source = DataPlatformPvDataSource()
            pv_ids = pv_data_source.list_pv_ids()
            site_metadata = pv_data_source.get_site_metadata()

        assert pv_ids == [dp_location_uuid]
        assert site_metadata == {
            dp_location_uuid: {
                "client_location_name": "Original Name",
                "capacity_kw": 4.0,
                "latitude": 51.0,
                "longitude": -1.0,
            }
        }
        # Only listed once, shared cache between list_pv_ids() and get_site_metadata().
        mock_fetch_loc_map.assert_called_once()
