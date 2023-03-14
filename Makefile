
build: build-inference build-db-clean-backup

build-inference:
	docker build . -f services/inference/Dockerfile

build-db-clean-backup:
	docker build . -f services/db-clean-backup/Dockerfile


all: build
