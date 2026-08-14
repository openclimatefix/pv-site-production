"""Public API for the data_platform package.

- `data_platform.client` — shared gRPC client + location listing.
- `data_platform.load` — reading generation/location data from the Data Platform.
- `data_platform.save` — saving forecasts to the Data Platform.
"""

from forecast_inference.data_platform.client import (
    DataPlatformClient,
    LocationSummary,
    fetch_dp_location_map,
    get_dataplatform_client,
)
from forecast_inference.data_platform.save import save_forecast_to_dataplatform

__all__ = [
    "DataPlatformClient",
    "LocationSummary",
    "fetch_dp_location_map",
    "get_dataplatform_client",
    "save_forecast_to_dataplatform",
]
