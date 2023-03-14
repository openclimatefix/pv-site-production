# PV Site Production
Site specific forecasting

There are various different repos for ML research and development but this repo runs the forecasts.

The main entry point is

```
poetry run python pv_site_production/app.py
```

## Installation

### Dependencies

    # All OS
    curl -sSL https://install.python-poetry.org | python3 -

    # Ubuntu
    apt install \
        libgeos-dev

    # Mac
    brew install \
        geos

    # Python dependencies
    poetry install


## Files

- infrastructure: Docker files
- pv_site_production: Main code folder
    - models: Various models will be in this folder
        - cos: Simple model using time of day
        - TODO more
- tests

## Development

### Format/lint the code

    make format
    make lint


### Run the tests

    make test

### Build the service dockers

    make build

### CI

To make sure that everything builds, passes the lint and tests, you can run

    make all

Optionally run everything in parallel:

    make all -j 8
