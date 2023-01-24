SRC=pv_site_production tests

.PHONY: lint
lint:
	poetry run flake8 $(SRC)
	poetry run pydocstyle $(SRC)


.PHONY: format
format:
	poetry run isort $(SRC)
	poetry run black $(SRC)

.PHONY: test
test:
	poetry run pytest --cov=pv_site_production tests --cov-report xml $(ARGS)
