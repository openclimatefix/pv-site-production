import pandera as pa
from datetime import datetime

# define schema
schema = pa.DataFrameSchema({
    "pv_uuid": pa.Column(str),
    "forecast_kw": pa.Column(float, ),
    "target_datetime_utc": pa.Column(datetime),
})