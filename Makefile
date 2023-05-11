.PHONY: help
help:
	@echo 'Usage: make <subcommand>'
	@echo ''
	@echo 'Subcommands:'
	@echo '    install          Install locally'
	@echo '    run-postgres     Run postgres locally for tests'
	@echo '    rm-postgres      Remove local postgres instance that is created for tests'
	@echo '    test             Run tests locally'
	@echo '    lint             Run linter locally'
	@echo '    mypy             Run mypy locally'




.PHONY: run-postgres
run-postgres:
	docker run --name postgres-tq-container -e POSTGRES_PASSWORD=password -p 15432:5432 -d postgres

.PHONY: rm-postgres
rm-postgres:
	docker kill postgres-tq-container
	docker rm postgres-tq-container


.PHONY: install
install:
	pdm install

.PHONY: test
test:
	pdm install --dev
	@source .venv/bin/activate && \
	python -m pytest

.PHONY: lint
lint:
	pdm install --dev
	@source .venv/bin/activate && \
	python -m flake8 postgrestq

.PHONY: mypy
mypy:
	pdm install --dev
	@source .venv/bin/activate && \
	python -m mypy --strict --explicit-package-bases postgrestq
