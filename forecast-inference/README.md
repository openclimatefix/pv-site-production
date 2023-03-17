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

## Entry point

```bash
poetry run python forecast_inference/app.py
```

## Development

Format the code base *in place*

    make format

Lint the code

    make lint

Run the tests

    make test
