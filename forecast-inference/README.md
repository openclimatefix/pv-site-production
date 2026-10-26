# forecast-inference service

## Dependencies

    # All OS
    curl -sSL https://install.python-poetry.org | python3 -

    # Ubuntu
    apt install \
        libgeos-dev

    # Mac
    brew install \
        geos

## Running the service

```bash
poetry install
poetry run python forecast_inference/app.py
```

Need to set
- OCF_ENVIRONMENT
- OCF_PV_DB_URL
- NWP_ZARR_PATH
- SAVE_TO_DATA_PLATFORM

Optional:
- `WRITE_TO_DB` (default `true`): save forecasts to the site database.
- `LOAD_SITES_FROM_DATA_PLATFORM` (default `false`): load sites and their generation
  from the Data Platform instead of the database. Needs `WRITE_TO_DB=false`.
- `READ_FROM_DATA_PLATFORM`
- `OBSERVER_NAME` (default `pv_site_api`)
  observations from. Only used when `READ_FROM_DATA_PLATFORM=true` — has no effect
  otherwise.


## Development

Format the code base *in place*

    make format

Lint the code

    make lint

Run the tests

    make test
