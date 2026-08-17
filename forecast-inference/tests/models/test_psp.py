import datetime as dt
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
from psp.typings import X

from forecast_inference.data.pv_data_sources import DataPlatformPvDataSource
from forecast_inference.models.psp import get_model


def _mock_dp_context(client):
    ctx = AsyncMock()
    ctx.__aenter__.return_value = client
    return ctx


def test_get_model(now):
    with open("tests/fixtures/model_configs/psp.yaml") as f:
        config = yaml.safe_load(f)

    dp_location_uuid = "dp-uuid-123"
    dp_location_name = "test_site"

    summary = MagicMock()
    summary.location_uuid = dp_location_uuid
    summary.location_name = dp_location_name
    summary.latlng.latitude = 51.0
    summary.latlng.longitude = 3.0
    summary.effective_capacity_watts = 4000
    summary.metadata.fields = {}

    n_generations = 100
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

    with (
        patch(
            "forecast_inference.data_platform.load.get_dataplatform_client",
            return_value=_mock_dp_context(mock_client),
        ),
        patch(
            "forecast_inference.data_platform.load.fetch_dp_location_map",
            new=AsyncMock(return_value={dp_location_name: summary}),
        ),
    ):
        pv_data_source = DataPlatformPvDataSource()
        model = get_model(config, pv_data_source)

        y = model.predict(X(pv_id=dp_location_uuid, ts=now))

    # The fixture model was trained with 48 * 4 horizons.
    assert y.powers.shape == (48 * 4,)
