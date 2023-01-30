""" Schema for what the ml models produce."""
from datetime import datetime

import pandera as pa

# define schema
schema = pa.DataFrameSchema(
    {
        "pv_uuid": pa.Column(str),
        "forecast_kw": pa.Column(
            float,
        ),
        "target_datetime_utc": pa.Column(datetime),
    }
)
