.PHONY: format
format:
	poetry run isort pv_site_production
	poetry run black pv_site_production

.PHONY: test
test:
	docker stop $(docker ps -a -q); true
	docker-compose -f infrastructure/test-docker-compose.yml build
	docker-compose -f infrastructure/test-docker-compose.yml run tests
