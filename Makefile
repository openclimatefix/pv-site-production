SRC=pv_site_production tests
SERVICES=inference db-backup-and-clean

lint:
	poetry run flake8 $(SRC)
	poetry run pydocstyle $(SRC)
	poetry run mypy $(SRC)

format:
	poetry run isort $(SRC)
	poetry run black $(SRC)

test:
	poetry run pytest --cov=pv_site_production tests --cov-report xml $(ARGS)

build-%:
	docker build . -f infrastructure/$*/Dockerfile

build: $(addprefix build-,$(SERVICES))

all: lint test build

.PHONY: lint format test build
