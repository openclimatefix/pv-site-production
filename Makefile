SRC=pv_site_production
.PHONY: format
format:
	poetry run flake8 $(SRC)
	poetry run isort $(SRC)
	poetry run black $(SRC)

.PHONY: test
test:
	docker stop $(docker ps -a -q); true
	docker-compose -f infrastructure/test-docker-compose.yml build
	docker-compose -f infrastructure/test-docker-compose.yml run tests
