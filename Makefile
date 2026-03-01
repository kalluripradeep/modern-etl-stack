.PHONY: up down restart logs seed dbt-run dbt-test register-connector ps help

# Copy .env.example to .env if it doesn't exist yet
.env:
	cp .env.example .env
	@echo "Created .env from .env.example — edit it with your credentials if needed."

## Start all services
up: .env
	docker-compose up -d

## Stop all services
down:
	docker-compose down

## Restart all services
restart: down up

## Tail logs for all containers (Ctrl-C to stop)
logs:
	docker-compose logs -f

## Show container status
ps:
	docker-compose ps

## Seed the source database with sample e-commerce data
seed:
	docker-compose exec -e SOURCE_DB_HOST=postgres-source airflow-webserver \
		python /opt/airflow/dbt/../../../sample-data/generate_ecommerce.py || \
	docker run --rm \
		--network modern-etl-stack_etl-network \
		-e SOURCE_DB_HOST=postgres-source \
		-e SOURCE_DB_USER=$${SOURCE_DB_USER:-sourceuser} \
		-e SOURCE_DB_PASSWORD=$${SOURCE_DB_PASSWORD:-sourcepass} \
		-e SOURCE_DB_NAME=$${SOURCE_DB_NAME:-sourcedb} \
		-v $(PWD)/sample-data:/data \
		python:3.11-slim bash -c "pip install -q psycopg2-binary faker && python /data/generate_ecommerce.py"

## Run dbt models
dbt-run:
	docker-compose exec airflow-webserver \
		dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt

## Run dbt tests
dbt-test:
	docker-compose exec airflow-webserver \
		dbt test --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt

## Register the Debezium CDC connector (run after `make up`)
register-connector:
	bash scripts/register_debezium_connector.sh

## Show this help
help:
	@echo ""
	@echo "modern-etl-stack — available targets:"
	@echo ""
	@grep -E '^## ' Makefile | sed 's/## /  make /' | paste - <(grep -E '^[a-z]' Makefile | grep -v '^\.') | awk '{print $$1, $$2, "-", substr($$0, index($$0,$$3))}'
	@echo ""
	@echo "Typical first-run sequence:"
	@echo "  make up && sleep 60 && make seed && make register-connector"
	@echo ""
