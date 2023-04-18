SRC=forecast_inference tests

.venv: pyproject.toml poetry.lock
	poetry install
	# Make sure the date of `.venv` is after `pyproject.toml`
	touch .venv

deps: .venv

lint: deps
	poetry run flake8 $(SRC)
	poetry run pydocstyle $(SRC)
	poetry run mypy $(SRC)

format: deps
	poetry run isort $(SRC)
	poetry run black $(SRC)

test: deps
	poetry run pytest --cov=forecast_inference tests --cov-report xml

.PHONY: lint format test deps
