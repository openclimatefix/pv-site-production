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

## Development

Format the code base *in place*

    make format

Lint the code

    make lint

Run the tests

    make test
