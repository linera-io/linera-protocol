DOCKER_DIR := ../docker
COMPOSE_FILE := $(DOCKER_DIR)/docker-compose.bridge-test.yml
DEMO_DIR := ../examples/bridge-demo
PROJECT := linera-bridge-demo

.PHONY: build-wasm build-all build-relayer relayer-up relayer-down relayer-logs up down demo demo-setup demo-frontend demo-logs

build-relayer: ## Build the bridge relayer docker image
	cd $(DOCKER_DIR) && docker build -f ./Dockerfile.bridge \
		-t $(RELAYER_IMAGE) ..

RELAYER_CONTAINER := linera-relay
RELAYER_IMAGE ?= linera-bridge

# Required env vars for relayer-up (must be set by caller)
# RPC_URL, EVM_BRIDGE_ADDRESS, LINERA_BRIDGE_APP, LINERA_FUNGIBLE_APP, EVM_PRIVATE_KEY

relayer-up: ## Run the bridge relayer container
	@missing=""; \
	for var in RPC_URL EVM_BRIDGE_ADDRESS LINERA_BRIDGE_APP LINERA_FUNGIBLE_APP EVM_PRIVATE_KEY; do \
		eval val=\$$$$var; \
		if [ -z "$$val" ]; then missing="$$missing $$var"; fi; \
	done; \
	if [ -n "$$missing" ]; then \
		echo "ERROR: Missing required env vars:$$missing"; \
		echo ""; \
		echo "Usage:"; \
		echo "  RPC_URL=... EVM_BRIDGE_ADDRESS=... \\"; \
		echo "  LINERA_BRIDGE_APP=... LINERA_FUNGIBLE_APP=... EVM_PRIVATE_KEY=... \\"; \
		echo "  make -C linera-bridge relayer-up"; \
		exit 1; \
	fi
	@echo "============================================"
	@echo "  Starting bridge relayer"
	@echo "============================================"
	@echo ""
	@echo "Required:"
	@echo "  RPC_URL              = $(RPC_URL)"
	@echo "  EVM_BRIDGE_ADDRESS   = $(EVM_BRIDGE_ADDRESS)"
	@echo "  LINERA_BRIDGE_APP    = $(LINERA_BRIDGE_APP)"
	@echo "  LINERA_FUNGIBLE_APP  = $(LINERA_FUNGIBLE_APP)"
	@echo "  EVM_PRIVATE_KEY      = $(EVM_PRIVATE_KEY)"
	@echo ""
	@echo "Optional:"
	@echo "  PORT                 = $(or $(PORT),3001)"
	@echo "  MONITOR_SCAN_INTERVAL= $(or $(MONITOR_SCAN_INTERVAL),30)"
	@echo "  MONITOR_START_BLOCK  = $(or $(MONITOR_START_BLOCK),0)"
	@echo "  MAX_RETRIES          = $(or $(MAX_RETRIES),10)"
	@echo "  LINERA_BRIDGE_CHAIN_ID = $(or $(LINERA_BRIDGE_CHAIN_ID),(auto-claim))"
	@echo "  LINERA_WALLET        = $(or $(LINERA_WALLET),(default))"
	@echo "  LINERA_KEYSTORE      = $(or $(LINERA_KEYSTORE),(default))"
	@echo "  LINERA_STORAGE       = $(or $(LINERA_STORAGE),(default))"
	@echo "============================================"
	docker run -d --name $(RELAYER_CONTAINER) \
		-p $(or $(PORT),3001):$(or $(PORT),3001) \
		-e RPC_URL="$(RPC_URL)" \
		-e EVM_BRIDGE_ADDRESS="$(EVM_BRIDGE_ADDRESS)" \
		-e LINERA_BRIDGE_APP="$(LINERA_BRIDGE_APP)" \
		-e LINERA_FUNGIBLE_APP="$(LINERA_FUNGIBLE_APP)" \
		-e EVM_PRIVATE_KEY="$(EVM_PRIVATE_KEY)" \
		$(if $(PORT),-e PORT="$(PORT)") \
		$(if $(MONITOR_SCAN_INTERVAL),-e MONITOR_SCAN_INTERVAL="$(MONITOR_SCAN_INTERVAL)") \
		$(if $(MONITOR_START_BLOCK),-e MONITOR_START_BLOCK="$(MONITOR_START_BLOCK)") \
		$(if $(MAX_RETRIES),-e MAX_RETRIES="$(MAX_RETRIES)") \
		$(if $(LINERA_BRIDGE_CHAIN_ID),-e LINERA_BRIDGE_CHAIN_ID="$(LINERA_BRIDGE_CHAIN_ID)") \
		$(if $(LINERA_WALLET),-e LINERA_WALLET="$(LINERA_WALLET)") \
		$(if $(LINERA_KEYSTORE),-e LINERA_KEYSTORE="$(LINERA_KEYSTORE)") \
		$(if $(LINERA_STORAGE),-e LINERA_STORAGE="$(LINERA_STORAGE)") \
		$(if $(RUST_LOG),-e RUST_LOG="$(RUST_LOG)") \
		$(RELAYER_IMAGE)
	@echo ""
	@echo "Relayer started. Logs: make -C linera-bridge relayer-logs"

relayer-down: ## Stop and remove the bridge relayer container
	docker rm -f $(RELAYER_CONTAINER) 2>/dev/null || true

relayer-logs: ## Tail logs from the standalone relayer container
	docker logs -f $(RELAYER_CONTAINER)

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
