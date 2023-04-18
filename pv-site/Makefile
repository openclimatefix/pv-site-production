SRC=pv_site tests notebooks

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
	poetry run pytest --cov=pv_site tests --cov-report xml

notebook:
	CWD=`pwd` poetry run jupyter notebook --ip 0.0.0.0 --notebook-dir notebooks


.PHONY: lint format test deps notebook
