#
# Makefile to conveniently lint/format/test/build everything.
#

SERVICES=forecast-inference database-cleanup

lint-%:
	$(MAKE) -C $* lint

lint: $(addprefix lint-,$(SERVICES))

format-%:
	$(MAKE) -C $* format

format: $(addprefix format-,$(SERVICES))

test-%:
	$(MAKE) -C $* test

test: $(addprefix test-,$(SERVICES))

build-%:
	docker build . -f infrastructure/Dockerfile.$*

build: $(addprefix build-,$(SERVICES))

all: lint test build

.PHONY: lint test build all
