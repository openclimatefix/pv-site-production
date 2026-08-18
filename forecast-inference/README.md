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
- DATA_PLATFORM_HOST / DATA_PLATFORM_PORT
- NWP_ZARR_PATH
- SAVE_TO_DATA_PLATFORM

Optional:
- `OBSERVER_NAME` (default `pv_site_api`): the Data Platform observer to read generation
  observations from.

This service reads sites, generation, and location metadata (latitude, longitude, capacity)
exclusively from the Data Platform — there is no database dependency for the live run. `tilt`
and `orientation` have no native Data Platform field; they're read from each location's
`metadata` (see `pv-site-api`'s `create_location`/`update_location`, which write them there —
specifically into `sources_history`, since they're a property of the energy source, not the
underlying geometry). A site missing `tilt`/`orientation`/`latitude`/`longitude`/`capacity_kw`
in its Data Platform metadata fails hard for that site rather than silently forecasting on
incomplete data — see `scripts/backfill_dp_tilt_orientation.py` in `pv-site-api` for backfilling
existing sites.


## Development

Format the code base *in place*

    make format

Lint the code

    make lint

Run the tests

    make test
