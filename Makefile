PY = python3
VENV = .venv

ifeq ($(OS), Windows_NT)
	BIN=$(VENV)/Scripts
	PY=python.exe
else
	BIN=$(VENV)/bin
endif

POSTGRES_DSN=postgresql://postgres:mysecretpassword@localhost:5432/postgres

.PHONY: run-postgres
run-postgres:
	docker run --name postgres-tq-container -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres

.PHONY: rm-postgres
rm-postgres:
	docker kill postgres-tq-container
	docker rm postgres-tq-container

.PHONY: test
test: $(VENV)
	$(BIN)/python -m pytest