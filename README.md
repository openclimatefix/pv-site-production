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
        libgeos-dev \
        docker-compose

    # Mac
    brew install \
        geos \
        docker-compose

    # Python dependencies
    poetry install


## Files

- infrastructure: Docker and docker compose files
- pv_site_production: Main code folder
    - models: Various models will be in this folder
        - cos: Simple model using time of day
        - TODO more
- tests


## docker tests
```
docker stop $(docker ps -a -q)
docker-compose -f infrastructure/test-docker-compose.yml build
docker-compose -f infrastructure/test-docker-compose.yml run tests
```
