DOCKER_DIR := ../docker
COMPOSE_FILE := $(DOCKER_DIR)/docker-compose.bridge-test.yml

.PHONY: build-all up down

build-all: ## Build all docker images needed for the bridge test (debug mode)
	cd $(DOCKER_DIR) && docker build \
		-f ./Dockerfile.exporter \
		--build-arg build_flag="" \
		--build-arg build_folder=debug \
		--build-arg build_features=scylladb,metrics,bridge \
		-t linera-exporter ..
	cd $(DOCKER_DIR) && docker build -f ./Dockerfile \
		--build-arg build_flag="" \
		--build-arg build_folder=debug \
		--build-arg build_features=scylladb \
		-t linera-test ..
	cd $(DOCKER_DIR) && docker build -f ./Dockerfile.bridge \
		--build-arg build_flag="" \
		--build-arg build_folder=debug \
		-t linera-bridge ..
	cd $(DOCKER_DIR) && docker build -f ./Dockerfile.foundry \
		-t foundry-jq .

up: ## Start the bridge test docker compose network
	docker compose -f $(COMPOSE_FILE) down -v
	docker compose -f $(COMPOSE_FILE) up -d

down: ## Stop the bridge test docker compose network
	docker compose -f $(COMPOSE_FILE) down -v
