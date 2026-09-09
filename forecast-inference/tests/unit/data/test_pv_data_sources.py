"""Unit tests for reading generation and location data from the Data Platform.


"""

import asyncio
import datetime as dt
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pvsite_datamodel.sqlmodels import LocationSQL

from forecast_inference.data.pv_data_sources import DbPvDataSource
from forecast_inference.data_platform.client import _sanitize
from forecast_inference.data_platform.load import (
    fetch_generation_for_one_site_from_dp,
    fetch_location_for_one_site_from_dp,
)

dp_site_name = "test-dp-site"


def _mock_dp_context(client):
    ctx = AsyncMock()
    ctx.__aenter__.return_value = client
    return ctx


@pytest.fixture()
def dp_site(database_connection):
    """A site with a client_location_name, committed directly to the database."""
    with database_connection.get_session() as session:
        site = LocationSQL(
            client_location_id=99999,
            client_location_name=dp_site_name,
            latitude=51.0,
            longitude=-1.0,
            capacity_kw=4.0,
            tilt=30.0,
            orientation=180.0,
            ml_id=999,
            country="uk",
        )
        session.add(site)
        session.commit()
        session.refresh(site)
        yield site


class TestFetchGenerationFromDp:
    """Test the module-level `fetch_generation_for_one_site_from_dp` helper."""

    def test_fetch_generation_for_one_site_from_dp_success(self, dp_site):
        mock_client = AsyncMock()

        mock_val = MagicMock()
        mock_val.timestamp_utc = dt.datetime(2024, 6, 1, 10, 0, tzinfo=dt.UTC)
        mock_val.value_fraction = 0.5
        mock_val.effective_capacity_watts = 2000  # 2 kW

        mock_obs_resp = MagicMock()
        mock_obs_resp.values = [mock_val]
        mock_client.get_observations_as_timeseries.return_value = mock_obs_resp

        mock_summary = MagicMock()
        mock_summary.location_uuid = "dp-uuid-123"
        loc_map = {_sanitize(dp_site_name): mock_summary}

        data = asyncio.run(
            fetch_generation_for_one_site_from_dp(
                mock_client,
                loc_map,
                dp_site,
                dt.datetime(2024, 6, 1, tzinfo=dt.UTC),
                dt.datetime(2024, 6, 2, tzinfo=dt.UTC),
                observer_name="test-observer",
            )
        )

        assert len(data) == 1
        # 0.5 fraction of 2000W = 1000W = 1.0 kW
        assert data[0][1] == 1.0
        assert data[0][0] == dt.datetime(2024, 6, 1, 10, 0, tzinfo=dt.UTC)

        req = mock_client.get_observations_as_timeseries.call_args[0][0]
        assert req.location_uuid == "dp-uuid-123"
        assert req.observer_name == "test-observer"

    def test_fetch_generation_for_one_site_from_dp_site_not_in_dp(self, dp_site):
        mock_client = AsyncMock()

        data = asyncio.run(
            fetch_generation_for_one_site_from_dp(
                mock_client,
                {"some-other-site": MagicMock()},
                dp_site,
                dt.datetime(2024, 6, 1, tzinfo=dt.UTC),
                dt.datetime(2024, 6, 2, tzinfo=dt.UTC),
            )
        )

        assert data == []
        mock_client.get_observations_as_timeseries.assert_not_called()


def _mock_summary(latitude=52.5, longitude=-2.5, capacity_watts=5000, metadata_fields=None):
    """Build a MagicMock DP `LocationSummary`, with a real (non-auto-mocking) metadata dict."""
    mock_summary = MagicMock()
    mock_summary.latlng.latitude = latitude
    mock_summary.latlng.longitude = longitude
    mock_summary.effective_capacity_watts = capacity_watts
    mock_summary.metadata.fields = metadata_fields or {}
    return mock_summary


class TestFetchLocationFromDp:
    """Test the module-level `fetch_location_for_one_site_from_dp` helper."""

    def test_returns_latitude_longitude_and_capacity(self, dp_site):
        loc_map = {_sanitize(dp_site_name): _mock_summary()}

        location = fetch_location_for_one_site_from_dp(loc_map, dp_site)

        assert location == {
            "latitude": 52.5,
            "longitude": -2.5,
            "capacity_kw": 5.0,
        }

    def test_site_not_in_dp_returns_none(self, dp_site):
        location = fetch_location_for_one_site_from_dp(
            {"some-other-site": _mock_summary()}, dp_site
        )
        assert location is None

    def test_site_without_client_location_name_returns_none(self, dp_site):
        dp_site.client_location_name = None
        location = fetch_location_for_one_site_from_dp(
            {_sanitize(dp_site_name): _mock_summary()}, dp_site
        )
        assert location is None

    def test_includes_tilt_and_orientation_when_present_in_metadata(self, dp_site):
        tilt_value = MagicMock()
        tilt_value.number_value = 35.0
        orientation_value = MagicMock()
        orientation_value.number_value = 170.0
        loc_map = {
            _sanitize(dp_site_name): _mock_summary(
                metadata_fields={"tilt": tilt_value, "orientation": orientation_value}
            )
        }

        location = fetch_location_for_one_site_from_dp(loc_map, dp_site)

        assert location == {
            "latitude": 52.5,
            "longitude": -2.5,
            "capacity_kw": 5.0,
            "tilt": 35.0,
            "orientation": 170.0,
        }

    def test_omits_tilt_and_orientation_when_absent_from_metadata(self, dp_site):
        """No `tilt`/`orientation` keys in DP metadata → caller falls back to the database."""
        loc_map = {_sanitize(dp_site_name): _mock_summary(metadata_fields={})}

        location = fetch_location_for_one_site_from_dp(loc_map, dp_site)

        assert "tilt" not in location
        assert "orientation" not in location


class TestDbPvDataSourceReadsFromDataPlatform:
    """Test `DbPvDataSource.get()` with `READ_FROM_DATA_PLATFORM=true`."""

    def test_reads_generation_and_location_metadata_from_dp(
        self, monkeypatch, database_connection, dp_site
    ):
        monkeypatch.setenv("READ_FROM_DATA_PLATFORM", "true")
        site_uuid = str(dp_site.location_uuid)

        mock_client = AsyncMock()

        mock_summary = MagicMock()
        mock_summary.location_uuid = "dp-uuid-123"
        mock_summary.effective_capacity_watts = 5000  # 5 kW, different from DB's 4 kW
        mock_summary.latlng.latitude = 52.5  # different from DB's 51.0
        mock_summary.latlng.longitude = -2.5  # different from DB's -1.0
        mock_summary.metadata.fields = {}  # no tilt/orientation in DP metadata for this site

        mock_val = MagicMock()
        mock_val.timestamp_utc = dt.datetime(2024, 6, 1, 10, 0, tzinfo=dt.UTC)
        mock_val.value_fraction = 0.5
        mock_val.effective_capacity_watts = 5000

        mock_obs_resp = MagicMock()
        mock_obs_resp.values = [mock_val]
        mock_client.get_observations_as_timeseries.return_value = mock_obs_resp

        with (
            patch(
                "forecast_inference.data_platform.load.get_dataplatform_client",
                return_value=_mock_dp_context(mock_client),
            ),
            patch(
                "forecast_inference.data_platform.load.fetch_dp_location_map",
                new=AsyncMock(return_value={_sanitize(dp_site_name): mock_summary}),
            ),
        ):
            pv_data_source = DbPvDataSource(database_connection)
            result = pv_data_source.get(
                [site_uuid],
                start_ts=dt.datetime(2024, 6, 1, tzinfo=dt.UTC),
                end_ts=dt.datetime(2024, 6, 2, tzinfo=dt.UTC),
            )

        # Generation value: 0.5 fraction * 5000W / 1000 = 2.5 kW
        assert result["power"].sel(id=site_uuid).values[0] == pytest.approx(2.5)

        # Location metadata comes from the DP, not the DB.
        assert result["latitude"].sel(id=site_uuid).item() == pytest.approx(52.5)
        assert result["longitude"].sel(id=site_uuid).item() == pytest.approx(-2.5)
        assert result["capacity"].sel(id=site_uuid).item() == pytest.approx(5.0)

        # tilt/orientation have no DP equivalent and weren't present in metadata here,
        # so they still come from the DB.
        assert result["tilt"].sel(id=site_uuid).item() == pytest.approx(30.0)
        assert result["orientation"].sel(id=site_uuid).item() == pytest.approx(180.0)

    def test_reads_tilt_and_orientation_from_dp_metadata_when_present(
        self, monkeypatch, database_connection, dp_site
    ):
        """When DP location metadata carries tilt/orientation, prefer those over the DB."""
        monkeypatch.setenv("READ_FROM_DATA_PLATFORM", "true")
        site_uuid = str(dp_site.location_uuid)

        mock_client = AsyncMock()

        tilt_value = MagicMock()
        tilt_value.number_value = 35.0  # different from DB's 30.0
        orientation_value = MagicMock()
        orientation_value.number_value = 170.0  # different from DB's 180.0

        mock_summary = MagicMock()
        mock_summary.location_uuid = "dp-uuid-123"
        mock_summary.effective_capacity_watts = 4000
        mock_summary.latlng.latitude = 51.0
        mock_summary.latlng.longitude = -1.0
        mock_summary.metadata.fields = {"tilt": tilt_value, "orientation": orientation_value}

        mock_obs_resp = MagicMock()
        mock_obs_resp.values = []
        mock_client.get_observations_as_timeseries.return_value = mock_obs_resp

        with (
            patch(
                "forecast_inference.data_platform.load.get_dataplatform_client",
                return_value=_mock_dp_context(mock_client),
            ),
            patch(
                "forecast_inference.data_platform.load.fetch_dp_location_map",
                new=AsyncMock(return_value={_sanitize(dp_site_name): mock_summary}),
            ),
        ):
            pv_data_source = DbPvDataSource(database_connection)
            result = pv_data_source.get(
                [site_uuid],
                start_ts=dt.datetime(2024, 6, 1, tzinfo=dt.UTC),
                end_ts=dt.datetime(2024, 6, 2, tzinfo=dt.UTC),
            )

        assert result["tilt"].sel(id=site_uuid).item() == pytest.approx(35.0)
        assert result["orientation"].sel(id=site_uuid).item() == pytest.approx(170.0)

    def test_site_not_found_in_dp_falls_back_to_db_metadata(
        self, monkeypatch, database_connection, dp_site
    ):
        monkeypatch.setenv("READ_FROM_DATA_PLATFORM", "true")
        site_uuid = str(dp_site.location_uuid)

        mock_client = AsyncMock()

        with (
            patch(
                "forecast_inference.data_platform.load.get_dataplatform_client",
                return_value=_mock_dp_context(mock_client),
            ),
            patch(
                "forecast_inference.data_platform.load.fetch_dp_location_map",
                new=AsyncMock(return_value={}),
            ),
        ):
            pv_data_source = DbPvDataSource(database_connection)
            result = pv_data_source.get(
                [site_uuid],
                start_ts=dt.datetime(2024, 6, 1, tzinfo=dt.UTC),
                end_ts=dt.datetime(2024, 6, 2, tzinfo=dt.UTC),
            )

        mock_client.get_observations_as_timeseries.assert_not_called()
        assert result["latitude"].sel(id=site_uuid).item() == pytest.approx(51.0)
        assert result["longitude"].sel(id=site_uuid).item() == pytest.approx(-1.0)
        assert result["capacity"].sel(id=site_uuid).item() == pytest.approx(4.0)

    def test_default_flag_reads_from_database(self, monkeypatch, database_connection, dp_site):
        """When READ_FROM_DATA_PLATFORM is unset, no Data Platform calls are made."""
        monkeypatch.delenv("READ_FROM_DATA_PLATFORM", raising=False)
        site_uuid = str(dp_site.location_uuid)

        with patch(
            "forecast_inference.data_platform.load.get_dataplatform_client",
        ) as mock_get_client:
            pv_data_source = DbPvDataSource(database_connection)
            result = pv_data_source.get(
                [site_uuid],
                start_ts=dt.datetime(2024, 6, 1, tzinfo=dt.UTC),
                end_ts=dt.datetime(2024, 6, 2, tzinfo=dt.UTC),
            )

        mock_get_client.assert_not_called()
        assert result["latitude"].sel(id=site_uuid).item() == pytest.approx(51.0)

    def test_works_when_called_from_within_a_running_event_loop(
        self, monkeypatch, database_connection, dp_site
    ):
        """Regression test: `app.py` calls `model.predict()` (and so `.get()`) synchronously
        from inside `asyncio.run(_run_app())`. `.get()` must not try to `asyncio.run()` again
        on top of that already-running loop.
        """
        monkeypatch.setenv("READ_FROM_DATA_PLATFORM", "true")
        site_uuid = str(dp_site.location_uuid)

        mock_client = AsyncMock()
        mock_obs_resp = MagicMock()
        mock_obs_resp.values = []
        mock_client.get_observations_as_timeseries.return_value = mock_obs_resp

        mock_summary = MagicMock()
        mock_summary.location_uuid = "dp-uuid-123"
        mock_summary.effective_capacity_watts = 4000
        mock_summary.latlng.latitude = 51.0
        mock_summary.latlng.longitude = -1.0
        mock_summary.metadata.fields = {}

        async def _call_get_from_within_a_running_loop():
            pv_data_source = DbPvDataSource(database_connection)
            return pv_data_source.get(
                [site_uuid],
                start_ts=dt.datetime(2024, 6, 1, tzinfo=dt.UTC),
                end_ts=dt.datetime(2024, 6, 2, tzinfo=dt.UTC),
            )

        with (
            patch(
                "forecast_inference.data_platform.load.get_dataplatform_client",
                return_value=_mock_dp_context(mock_client),
            ),
            patch(
                "forecast_inference.data_platform.load.fetch_dp_location_map",
                new=AsyncMock(return_value={_sanitize(dp_site_name): mock_summary}),
            ),
        ):
            # asyncio.run() here mirrors app.py's `asyncio.run(_run_app())`, under which
            # `.get()` is invoked synchronously — this must not raise
            # "asyncio.run() cannot be called from a running event loop".
            result = asyncio.run(_call_get_from_within_a_running_loop())

        assert result["latitude"].sel(id=site_uuid).item() == pytest.approx(51.0)

    def test_location_map_is_cached_across_calls(self, monkeypatch, database_connection, dp_site):
        """A single DbPvDataSource instance should only list DP locations once, not
        once per `.get()` call (which happens once per site in the real app loop).
        """
        monkeypatch.setenv("READ_FROM_DATA_PLATFORM", "true")
        site_uuid = str(dp_site.location_uuid)

        mock_client = AsyncMock()
        mock_obs_resp = MagicMock()
        mock_obs_resp.values = []
        mock_client.get_observations_as_timeseries.return_value = mock_obs_resp

        mock_summary = MagicMock()
        mock_summary.location_uuid = "dp-uuid-123"
        mock_summary.effective_capacity_watts = 4000
        mock_summary.latlng.latitude = 51.0
        mock_summary.latlng.longitude = -1.0
        mock_summary.metadata.fields = {}
        mock_fetch_loc_map = AsyncMock(return_value={_sanitize(dp_site_name): mock_summary})

        with (
            patch(
                "forecast_inference.data_platform.load.get_dataplatform_client",
                return_value=_mock_dp_context(mock_client),
            ),
            patch(
                "forecast_inference.data_platform.load.fetch_dp_location_map",
                new=mock_fetch_loc_map,
            ),
        ):
            pv_data_source = DbPvDataSource(database_connection)
            for _ in range(3):
                pv_data_source.get(
                    [site_uuid],
                    start_ts=dt.datetime(2024, 6, 1, tzinfo=dt.UTC),
                    end_ts=dt.datetime(2024, 6, 2, tzinfo=dt.UTC),
                )

        mock_fetch_loc_map.assert_called_once()
