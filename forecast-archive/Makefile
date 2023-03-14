SRC=forecast_archive tests

lint:
	poetry run ruff $(SRC)
	poetry run black --check $(SRC)

format:
	poetry run ruff --fix $(SRC)
	poetry run black $(SRC)

test:
	poetry run pytest --cov=forecast_archive tests --cov-report xml

build:
	docker build .

all: lint test build

.PHONY: lint format test build all
