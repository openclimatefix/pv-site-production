SRC=database_cleanup tests

.venv: pyproject.toml poetry.lock
	poetry install
	# Make sure the date of `.venv` is after `pyproject.toml`
	touch .venv

deps: .venv

lint: deps
	poetry run ruff $(SRC)
	poetry run black --check $(SRC)

format: deps
	poetry run ruff --fix $(SRC)
	poetry run black $(SRC)

test: deps
	poetry run pytest --cov=database_cleanup tests --cov-report xml

.PHONY: lint format test deps
