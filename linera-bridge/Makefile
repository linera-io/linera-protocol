DOCKER_DIR := ../docker
COMPOSE_FILE := $(DOCKER_DIR)/docker-compose.bridge-test.yml
DEMO_DIR := ../examples/bridge-demo
PROJECT := linera-bridge-demo

.PHONY: build-wasm build-all up down demo demo-setup demo-frontend demo-logs

build-wasm: ## Build all Wasm files needed for bridge demo
	cd ../examples && cargo build --release --target wasm32-unknown-unknown \
		-p fungible -p wrapped-fungible -p evm-bridge

build-all: build-wasm ## Build Wasm files and all docker images (debug mode)
	cd $(DOCKER_DIR) && docker build \
		-f ./Dockerfile.exporter \
		--build-arg build_flag="" \
		--build-arg build_folder=debug \
		--build-arg build_features=scylladb,metrics \
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

up: ## Start the bridge docker compose network and wait for services
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT) down -v 2>/dev/null || true
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT) up -d
	@echo "Waiting for services to be healthy..."
	@docker compose -f $(COMPOSE_FILE) -p $(PROJECT) exec -T linera-network \
		bash -c 'until cat < /dev/null > /dev/tcp/localhost/8080 2>/dev/null; do sleep 2; done'
	@echo "Linera network is ready."

down: ## Stop the bridge docker compose network and remove volumes
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT) down -v

demo-setup: ## Run the bridge demo setup script (deploy contracts + Linera apps)
	cd $(DEMO_DIR) && bash setup.sh --compose-file $(CURDIR)/$(COMPOSE_FILE)

demo-frontend: ## Install deps and start the Vite dev server
	cd $(DEMO_DIR) && pnpm install && pnpm dev

demo-logs: ## Tail logs from the relay service
	docker compose -f $(COMPOSE_FILE) -p $(PROJECT) logs -f linera-relay

demo: build-all up demo-setup ## Build everything, start network, deploy contracts, and print next steps
	@echo ""
	@echo "============================================"
	@echo "  Bridge demo is ready!"
	@echo "============================================"
	@echo ""
	@echo "Start the frontend:"
	@echo "  make -C linera-bridge demo-frontend"
	@echo ""
	@echo "Then open http://localhost:5173"
	@echo "  - Connect MetaMask to Anvil (chain ID 31337, http://localhost:8545)"
	@echo "  - Import Anvil account 0:"
	@echo "    0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
	@echo ""
	@echo "Tail relay logs:  make -C linera-bridge demo-logs"
	@echo "Stop everything:  make -C linera-bridge down"
	@echo ""
