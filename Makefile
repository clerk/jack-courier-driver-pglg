.PHONY: test lint deps dev

test:
	go test ./...

lint:
	golangci-lint run ./...

deps:
	go mod tidy

# Enable local development with go.work
dev:
	@if [ ! -f go.work ]; then \
		echo "go 1.25.6\n\nuse .\n\nreplace (\n\tgithub.com/clerk/jack-courier-lib v0.0.0 => ../jack-courier-lib\n\tgithub.com/clerk/jack-service/proto/jackpb v0.2.2 => ../jack-service/proto/jackpb\n)" > go.work; \
		echo "go.work created for local development"; \
	else \
		echo "go.work already exists"; \
	fi

###############################################
#                                             #
#  Database connectivity & setup              #
#                                             #
###############################################

GCLOUD_APPLICATION_DEFAULT_CREDENTIALS=$(HOME)/.config/gcloud/application_default_credentials.json
GOOGLE_CLOUD_SQL_PROXY_IMG=gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.14.2
DOCKER_UID_RUN_ARG=
COLOR_GREEN=\033[32m
COLOR_BOLD=\033[1m
COLOR_UNDERLINE=\033[4m
COLOR_RESET=\033[0m

STAGING_CLOUD_SQL_INSTANCE_CONNECTION_NAME=clerk-staging:us-central1:clerk-primary-staging-v17-migration-test
PRODUCTION_CLOUD_SQL_INSTANCE_CONNECTION_NAME=clerk-production:us-central1:clerk-primary-production-v17

STAGING_PROXY_PORT=15432
PRODUCTION_PROXY_PORT=15433

# pglg-setup flags per environment.
# Teams should copy and adjust these for their own prefix/publication/slot.
STAGING_PREFIX=legacy
STAGING_PUBLICATION=legacy_jobs_pub
STAGING_SLOT=legacy_jobs_slot
STAGING_SCHEMA=public
STAGING_CONN_STRING=postgres://$(DB_USER):$(DB_PASS)@127.0.0.1:$(STAGING_PROXY_PORT)/clerk?sslmode=disable

PRODUCTION_PREFIX=legacy
PRODUCTION_PUBLICATION=legacy_jobs_pub
PRODUCTION_SLOT=legacy_jobs_slot
PRODUCTION_SCHEMA=public
PRODUCTION_CONN_STRING=postgres://$(DB_USER):$(DB_PASS)@127.0.0.1:$(PRODUCTION_PROXY_PORT)/clerk?sslmode=disable

db/connect/staging: ## Create a tunnel to Staging Cloud SQL instance using Cloud SQL Auth Proxy
	@test -f $(GCLOUD_APPLICATION_DEFAULT_CREDENTIALS) || (echo "Error: $(GCLOUD_APPLICATION_DEFAULT_CREDENTIALS) does not exist or is not a file. Run 'gcloud auth application-default login' first." && exit 1)
	@echo "Connect to Staging Cloud SQL using $(COLOR_GREEN)$(COLOR_BOLD)$(COLOR_UNDERLINE)hostname: 127.0.0.1 & port: $(STAGING_PROXY_PORT)$(COLOR_RESET)\n"
	@docker run --rm -v $(GCLOUD_APPLICATION_DEFAULT_CREDENTIALS):$(GCLOUD_APPLICATION_DEFAULT_CREDENTIALS) \
	-p 127.0.0.1:$(STAGING_PROXY_PORT):5432 \
	$(DOCKER_UID_RUN_ARG) \
	$(GOOGLE_CLOUD_SQL_PROXY_IMG) \
	--address 0.0.0.0 --port 5432 --credentials-file $(GCLOUD_APPLICATION_DEFAULT_CREDENTIALS) \
	$(STAGING_CLOUD_SQL_INSTANCE_CONNECTION_NAME)

db/connect/production: ## Create a tunnel to Production Cloud SQL instance using Cloud SQL Auth Proxy
	@test -f $(GCLOUD_APPLICATION_DEFAULT_CREDENTIALS) || (echo "Error: $(GCLOUD_APPLICATION_DEFAULT_CREDENTIALS) does not exist or is not a file. Run 'gcloud auth application-default login' first." && exit 1)
	@echo "Connect to Production Cloud SQL using $(COLOR_GREEN)$(COLOR_BOLD)$(COLOR_UNDERLINE)hostname: 127.0.0.1 & port: $(PRODUCTION_PROXY_PORT)$(COLOR_RESET)\n"
	@docker run --rm -v $(GCLOUD_APPLICATION_DEFAULT_CREDENTIALS):$(GCLOUD_APPLICATION_DEFAULT_CREDENTIALS) \
	-p 127.0.0.1:$(PRODUCTION_PROXY_PORT):5432 \
	$(DOCKER_UID_RUN_ARG) \
	$(GOOGLE_CLOUD_SQL_PROXY_IMG) \
	--address 0.0.0.0 --port 5432 --credentials-file $(GCLOUD_APPLICATION_DEFAULT_CREDENTIALS) \
	$(PRODUCTION_CLOUD_SQL_INSTANCE_CONNECTION_NAME)

db/setup/staging: ## Create outbox tables, publication, and replication slot on staging
	@test -n "$(DB_USER)" || (echo "Error: DB_USER is required. Usage: make db/setup/staging DB_USER=master DB_PASS=xxx" && exit 1)
	@test -n "$(DB_PASS)" || (echo "Error: DB_PASS is required. Usage: make db/setup/staging DB_USER=master DB_PASS=xxx" && exit 1)
	go run ./cmd/pglg-setup create \
		--conn-string="$(STAGING_CONN_STRING)" \
		--schema=$(STAGING_SCHEMA) \
		--prefix=$(STAGING_PREFIX) \
		--publication=$(STAGING_PUBLICATION) \
		--slot=$(STAGING_SLOT)

db/setup/production: ## Create outbox tables, publication, and replication slot on production
	@test -n "$(DB_USER)" || (echo "Error: DB_USER is required. Usage: make db/setup/production DB_USER=master DB_PASS=xxx" && exit 1)
	@test -n "$(DB_PASS)" || (echo "Error: DB_PASS is required. Usage: make db/setup/production DB_USER=master DB_PASS=xxx" && exit 1)
	go run ./cmd/pglg-setup create \
		--conn-string="$(PRODUCTION_CONN_STRING)" \
		--schema=$(PRODUCTION_SCHEMA) \
		--prefix=$(PRODUCTION_PREFIX) \
		--publication=$(PRODUCTION_PUBLICATION) \
		--slot=$(PRODUCTION_SLOT)

db/destroy/staging: ## Drop outbox tables, publication, and replication slot on staging
	@test -n "$(DB_USER)" || (echo "Error: DB_USER is required. Usage: make db/destroy/staging DB_USER=master DB_PASS=xxx" && exit 1)
	@test -n "$(DB_PASS)" || (echo "Error: DB_PASS is required. Usage: make db/destroy/staging DB_USER=master DB_PASS=xxx" && exit 1)
	go run ./cmd/pglg-setup destroy \
		--conn-string="$(STAGING_CONN_STRING)" \
		--schema=$(STAGING_SCHEMA) \
		--prefix=$(STAGING_PREFIX) \
		--publication=$(STAGING_PUBLICATION) \
		--slot=$(STAGING_SLOT)

db/destroy/production: ## Drop outbox tables, publication, and replication slot on production
	@test -n "$(DB_USER)" || (echo "Error: DB_USER is required. Usage: make db/destroy/production DB_USER=master DB_PASS=xxx" && exit 1)
	@test -n "$(DB_PASS)" || (echo "Error: DB_PASS is required. Usage: make db/destroy/production DB_PASS=xxx" && exit 1)
	go run ./cmd/pglg-setup destroy \
		--conn-string="$(PRODUCTION_CONN_STRING)" \
		--schema=$(PRODUCTION_SCHEMA) \
		--prefix=$(PRODUCTION_PREFIX) \
		--publication=$(PRODUCTION_PUBLICATION) \
		--slot=$(PRODUCTION_SLOT)

.PHONY: db/connect/staging db/connect/production db/setup/staging db/setup/production db/destroy/staging db/destroy/production

INTEGRATION_DB_USER=pglg
INTEGRATION_DB_PASS=pglg
INTEGRATION_DB_NAME=pglg
INTEGRATION_DB_HOST=127.0.0.1
INTEGRATION_DB_PORT=15434
INTEGRATION_CONN_STRING=host=$(INTEGRATION_DB_HOST) port=$(INTEGRATION_DB_PORT) user=$(INTEGRATION_DB_USER) password=$(INTEGRATION_DB_PASS) dbname=$(INTEGRATION_DB_NAME) sslmode=disable

INTEGRATION_SCHEMA=public
INTEGRATION_PREFIX=outbox
INTEGRATION_PUBLICATION=outbox_pub
INTEGRATION_SLOT=outbox_slot

up: ## Bring up local Postgres and provision the pglg schema
	docker compose up -d --wait
	go run ./cmd/pglg-setup create \
		--conn-string="$(INTEGRATION_CONN_STRING)" \
		--schema=$(INTEGRATION_SCHEMA) \
		--prefix=$(INTEGRATION_PREFIX) \
		--publication=$(INTEGRATION_PUBLICATION) \
		--slot=$(INTEGRATION_SLOT)

down: ## Stop and remove the local Postgres container
	docker compose down -v

integration: ## Run integration tests against the local Postgres (run `make up` first)
	PGLG_INTEGRATION_CONN_STRING="$(INTEGRATION_CONN_STRING)" \
	PGLG_INTEGRATION_SCHEMA="$(INTEGRATION_SCHEMA)" \
	PGLG_INTEGRATION_PREFIX="$(INTEGRATION_PREFIX)" \
	PGLG_INTEGRATION_PUBLICATION="$(INTEGRATION_PUBLICATION)" \
	PGLG_INTEGRATION_SLOT="$(INTEGRATION_SLOT)" \
		go test -tags=integration -count=1 -race -v -run '^TestIntegration_' ./...

.PHONY: up down integration
