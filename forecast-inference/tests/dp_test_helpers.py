"""Shared test helpers for mocking the Data Platform client."""

from unittest.mock import AsyncMock, MagicMock


def mock_dp_context(client):
    """Wrap a mock DP client in an async context manager, as `get_dataplatform_client()` yields."""
    ctx = AsyncMock()
    ctx.__aenter__.return_value = client
    return ctx


def tilt_orientation_metadata_fields(tilt=30.0, orientation=180.0):
    """Metadata fields dict with tilt/orientation set, as required by `DataPlatformPvDataSource.get()`."""
    tilt_value = MagicMock()
    tilt_value.number_value = tilt
    orientation_value = MagicMock()
    orientation_value.number_value = orientation
    return {"tilt": tilt_value, "orientation": orientation_value}
