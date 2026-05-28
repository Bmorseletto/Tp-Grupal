SHELL := /bin/bash
PWD := $(shell pwd)

up:
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

inputs:
	uv run generate_inputs.py --output=datasets 10000 3
.PHONY: inputs

accounts:
	uv run generate_accounts.py --output=datasets --source=datasets/LI-Small_accounts.csv --prefix=accounts 800000 3
.PHONY: accounts

# test:
# 	mkdir -p output
# 	rm ./output/* -f
# 	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
# 	PYTHONPATH="$(PWD)/src/common" python3 ./verify_output.py
# 	docker compose -f docker-compose.yaml stop -t 5
# 	docker compose -f docker-compose.yaml down
# .PHONY: test

switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, una sola réplica de cada elemento"
	@echo "2) Múltiples clientes, una sola réplica de cada elemento"
	@echo "3) Múltiples clientes, filtro de currency y de query 1 replicados, un solo aggregation" 
# 	@echo "4) Múltiples clientes, múltiples réplicas"
# 	@echo "5) Múltiples clientes, múltiples réplicas, nombres al azar"
	@read -p "Selecciona uno [1-5]: " option;	\
	cp ./scenarios/$${option}.yaml docker-compose.yaml
.PHONY: switch
