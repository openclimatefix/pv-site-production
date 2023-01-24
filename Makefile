SRC=pv_site_production tests

.PHONY: lint
lint:
	poetry run flake8 $(SRC)
	poetry run pydocstyle $(SRC)


.PHONY: format
format:
	poetry run isort $(SRC)
	poetry run black $(SRC)

# TODO Use testcontainers instead.
.PHONY: test-db
test-db:
	docker kill psp-test-db; true
	docker run \
		-it --rm \
		-d \
		--name psp-test-db \
		-e POSTGRES_USER=postgres-test \
		-e POSTGRES_PASSWORD=postgres-test \
		-e POSTGRES_DB=psp-test \
		-p 5460:5432 \
		postgres:13-alpine

.PHONY: test
test: test-db
	poetry run pytest tests $(ARGS)
	docker kill psp-test-db
