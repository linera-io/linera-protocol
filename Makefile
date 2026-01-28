#!/usr/bin/env make
# Linera Protocol Makefile 🚀

.PHONY: help setup clean verify

# Configuration
NETWORK_NAME ?= testnet-conway
LINERA_BIN := ./target/release/linera

# Public demo infrastructure (safe to expose - read-only public bucket)
# These defaults point to the public Linera demo infrastructure
# Override these variables for private/internal deployments
PUBLIC_GCS_BUCKET := gs://demos.linera.net
PUBLIC_URL_MAP := demos-linera-net

# Allow overrides for private deployments
GCS_BUCKET ?= $(PUBLIC_GCS_BUCKET)
URL_MAP ?= $(PUBLIC_URL_MAP)

# Extract 'testnet' or 'devnet' from network name, otherwise use full name
ifeq ($(findstring testnet,$(NETWORK_NAME)),testnet)
    NETWORK_TYPE := testnet
else ifeq ($(findstring devnet,$(NETWORK_NAME)),devnet)
    NETWORK_TYPE := devnet
else
    NETWORK_TYPE := $(NETWORK_NAME)
endif

# Directories
WALLET_DIR ?= $(shell mktemp -d /tmp/linera-wallet.XXXXXX)
EXAMPLES_DIR := ./examples
DEMO_PATH := $(GCS_BUCKET)/$(NETWORK_TYPE)

# Linera environment
LINERA_WALLET := $(WALLET_DIR)/wallet.json
LINERA_KEYSTORE := $(WALLET_DIR)/keystore.json
LINERA_STORAGE := rocksdb:$(WALLET_DIR)/storage.db
export LINERA_WALLET
export LINERA_KEYSTORE
export LINERA_STORAGE

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
BLUE := \033[0;34m
NC := \033[0m

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@printf "$(BLUE)╔════════════════════════════════════════╗$(NC)\n"
	@printf "$(BLUE)║         Linera Protocol Tools          ║$(NC)\n"
	@printf "$(BLUE)╚════════════════════════════════════════════╝$(NC)\n"
	@echo ""
	@echo "Usage: make [target] NETWORK_NAME=<network>"
	@echo ""
	@printf "$(GREEN)━━━ 1. Setup & Dependencies ━━━$(NC)\n"
	@grep -E '^(build-linera|check-deps|setup|check-gcloud-auth):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@printf "$(GREEN)━━━ 2. Wallet & Chain ━━━$(NC)\n"
	@grep -E '^(init-wallet):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@printf "$(GREEN)━━━ 3. WASM Building ━━━$(NC)\n"
	@grep -E '^(build-wasm-counter|build-wasm-fungible|build-wasm-all):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@printf "$(GREEN)━━━ 4. Blockchain Deployment ━━━$(NC)\n"
	@grep -E '^(deploy-app-counter|deploy-app-fungible|deploy-apps-all):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@printf "$(GREEN)━━━ 5. Frontend Building ━━━$(NC)\n"
	@grep -E '^(build-demo-counter|build-demo-fungible|build-demo-metamask|build-demos-all):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@printf "$(GREEN)━━━ 6. GCS Deployment ━━━$(NC)\n"
	@grep -E '^(deploy-gcs-counter|deploy-gcs-fungible|deploy-gcs-metamask|deploy-gcs-all):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@printf "$(GREEN)━━━ 7. Composite Workflows ━━━$(NC)\n"
	@grep -E '^(counter-full|fungible-full|full-deploy|counter-quick|fungible-quick|quick-deploy):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@printf "$(GREEN)━━━ 8. Utilities ━━━$(NC)\n"
	@grep -E '^(verify|invalidate-cache|create-env|fetch-env-counter|fetch-env-fungible|fetch-env-all):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@printf "$(GREEN)━━━ 9. Cleanup ━━━$(NC)\n"
	@grep -E '^(clean|clean-cargo-all|clean-cargo-main|clean-cargo-counter|clean-cargo-fungible|clean-all):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "Current Configuration:"
	@echo "  📌 NETWORK_NAME: $(NETWORK_NAME)"
	@echo "  📌 NETWORK_TYPE: $(NETWORK_TYPE)"
	@echo "  📂 WALLET_DIR: $(WALLET_DIR)"
	@echo "  📂 EXAMPLES_DIR: $(EXAMPLES_DIR)"
	@echo "  ☁️ DEMO_PATH: $(DEMO_PATH)"
	@if [ -f ".env.wallet" ]; then \
		. ./.env.wallet && \
		echo "" && \
		echo "Wallet Status:" && \
		echo "  🔗 Chain: $$CHAIN" && \
		echo "  👤 Owner: $$OWNER"; \
	else \
		echo "" && \
		echo "Wallet Status:" && \
		echo "  ⚠️  Not initialized (run 'make init-wallet')"; \
	fi

check-deps: ## Check all required dependencies
	@printf "$(YELLOW)🔍 Checking dependencies...$(NC)\n"
	@command -v gcloud >/dev/null 2>&1 || { printf "$(RED)❌ gcloud not found$(NC)\n"; exit 1; }
	@command -v cargo >/dev/null 2>&1 || { printf "$(RED)❌ cargo not found$(NC)\n"; exit 1; }
	@command -v node >/dev/null 2>&1 || { printf "$(RED)❌ node not found$(NC)\n"; exit 1; }
	@command -v pnpm >/dev/null 2>&1 || { printf "$(RED)❌ pnpm not found$(NC)\n"; exit 1; }
	@if [ ! -f "$(LINERA_BIN)" ]; then \
		printf "$(RED)❌ linera binary not found at $(LINERA_BIN)$(NC)\n"; \
		printf "$(YELLOW)   Run 'cargo build --release' first$(NC)\n"; \
		exit 1; \
	fi
	@printf "$(GREEN)✅ All dependencies found$(NC)\n"

setup: check-deps ## Setup environment (builds linera if needed)
	@printf "$(YELLOW)⚙️  Setting up environment...$(NC)\n"
	@if [ ! -f "$(LINERA_BIN)" ]; then \
		printf "$(YELLOW)🔨 Building linera binary first...$(NC)\n"; \
		$(MAKE) build-linera; \
	fi
	@echo "  📂 Using wallet directory: $(WALLET_DIR)"
	@printf "$(YELLOW)🔍 Verifying example applications...$(NC)\n"
	@if [ ! -d "$(EXAMPLES_DIR)/counter" ]; then \
		printf "$(RED)❌ Error: Counter example not found in $(EXAMPLES_DIR)$(NC)\n"; \
		exit 1; \
	fi
	@printf "$(GREEN)✅ Setup complete$(NC)\n"

# ===== Wallet and Chain Management =====
init-wallet: setup ## Initialize wallet and request chain from faucet
	@printf "$(RED)⚠️  WARNING: This will create a new wallet and request a chain from the faucet!$(NC)\n"
	@printf "$(YELLOW)This will overwrite any existing wallet in: $(WALLET_DIR)$(NC)\n"
	@printf "Type 'YES' (all uppercase) to continue: " && read confirm && [ "$$confirm" = "YES" ] || { printf "$(RED)Operation cancelled$(NC)\n"; exit 1; }
	@printf "$(YELLOW)💰 Initializing wallet...$(NC)\n"
	@$(LINERA_BIN) wallet init \
			--faucet https://faucet.$(NETWORK_NAME).linera.net
	@printf "$(YELLOW)⛓️  Requesting chain...$(NC)\n"
	@INFO=$$($(LINERA_BIN) wallet request-chain \
			--faucet https://faucet.$(NETWORK_NAME).linera.net | tr '\n' ' ') && \
		CHAIN=$$(echo "$$INFO" | awk '{print $$1}') && \
		OWNER=$$(echo "$$INFO" | awk '{print $$2}') && \
		printf "$(BLUE)  🔗 Chain ID: $$CHAIN$(NC)\n" && \
		printf "$(BLUE)  👤 Owner: $$OWNER$(NC)\n" && \
		echo "CHAIN=$$CHAIN" > .env.wallet && \
		echo "OWNER=$$OWNER" >> .env.wallet && \
		echo "export CHAIN=$$CHAIN" >> .env.wallet && \
		echo "export OWNER=$$OWNER" >> .env.wallet && \
		echo "export LINERA_WALLET=$(LINERA_WALLET)" >> .env.wallet && \
		echo "export LINERA_KEYSTORE=$(LINERA_KEYSTORE)" >> .env.wallet && \
		echo "export LINERA_STORAGE=$(LINERA_STORAGE)" >> .env.wallet
	@printf "$(GREEN)✅ Wallet initialized and chain info saved to .env.wallet$(NC)\n"

# ===== WASM Building (Per App) =====
build-wasm-counter: ## Build counter WASM binaries
	@printf "$(YELLOW)🔨 Building counter WASM binaries...$(NC)\n"
	@cd $(EXAMPLES_DIR)/counter && \
		cargo build --release --target wasm32-unknown-unknown
	@printf "$(GREEN)✅ Counter WASM binaries built$(NC)\n"

build-wasm-fungible: ## Build fungible token WASM binaries
	@printf "$(YELLOW)🔨 Building fungible token WASM binaries...$(NC)\n"
	@cd $(EXAMPLES_DIR)/native-fungible && \
		cargo build --release --target wasm32-unknown-unknown
	@printf "$(GREEN)✅ Fungible token WASM binaries built$(NC)\n"

build-wasm-all: build-wasm-counter build-wasm-fungible ## Build all WASM binaries

# ===== Blockchain Deployment (Per App) =====
deploy-app-counter: build-wasm-counter ## Deploy counter app to blockchain
	@if [ ! -f ".env.wallet" ]; then \
		printf "$(RED)❌ Wallet not initialized. Run 'make init-wallet' first$(NC)\n"; \
		exit 1; \
	fi
	@. ./.env.wallet && \
		if [ -z "$$CHAIN" ] || [ -z "$$OWNER" ]; then \
			printf "$(RED)❌ Chain or Owner information missing. Re-run 'make init-wallet'$(NC)\n"; \
			exit 1; \
		fi
	@printf "$(RED)⚠️  WARNING: This will deploy counter app to the blockchain!$(NC)\n"
	@printf "$(YELLOW)This operation will consume resources and cannot be undone.$(NC)\n"
	@printf "Type 'YES' (all uppercase) to continue: " && read confirm && [ "$$confirm" = "YES" ] || { printf "$(RED)Deployment cancelled$(NC)\n"; exit 1; }
	@printf "$(YELLOW)📱 Deploying counter application...$(NC)\n"
	@. ./.env.wallet && \
		cd $(EXAMPLES_DIR)/counter && \
		COUNTER_APP_ID=$$(../../$(LINERA_BIN) publish-and-create \
			../target/wasm32-unknown-unknown/release/counter_{contract,service}.wasm \
			--json-argument "1" | grep -oE '[a-f0-9]{64}' | tail -1) && \
		if [ -z "$$COUNTER_APP_ID" ]; then \
			printf "$(RED)❌ Failed to deploy counter app$(NC)\n"; \
			exit 1; \
		fi && \
		echo "  📋 Counter App ID: $$COUNTER_APP_ID" && \
		if [ -f ../../.env.apps ]; then \
			sed -i '/^COUNTER_APP_ID=/d' ../../.env.apps 2>/dev/null || \
			sed -i '' '/^COUNTER_APP_ID=/d' ../../.env.apps 2>/dev/null || true; \
		fi && \
		echo "COUNTER_APP_ID=$$COUNTER_APP_ID" >> ../../.env.apps && \
		echo "export LINERA_APPLICATION_ID=$$COUNTER_APP_ID" > ../../.env.counter && \
		echo "export LINERA_FAUCET_URL=https://faucet.$(NETWORK_NAME).linera.net" >> ../../.env.counter && \
		echo "export CHAIN=$$CHAIN" >> ../../.env.counter && \
		echo "export OWNER=$$OWNER" >> ../../.env.counter
	@printf "$(GREEN)✅ Counter application deployed$(NC)\n"

deploy-app-fungible: build-wasm-fungible ## Deploy fungible app to blockchain
	@if [ ! -f ".env.wallet" ]; then \
		printf "$(RED)❌ Wallet not initialized. Run 'make init-wallet' first$(NC)\n"; \
		exit 1; \
	fi
	@. ./.env.wallet && \
		if [ -z "$$CHAIN" ] || [ -z "$$OWNER" ]; then \
			printf "$(RED)❌ Chain or Owner information missing. Re-run 'make init-wallet'$(NC)\n"; \
			exit 1; \
		fi
	@printf "$(RED)⚠️  WARNING: This will deploy fungible app to the blockchain!$(NC)\n"
	@printf "$(YELLOW)This operation will consume resources and cannot be undone.$(NC)\n"
	@printf "Type 'YES' (all uppercase) to continue: " && read confirm && [ "$$confirm" = "YES" ] || { printf "$(RED)Deployment cancelled$(NC)\n"; exit 1; }
	@printf "$(YELLOW)💱 Deploying fungible token application...$(NC)\n"
	@. ./.env.wallet && \
		cd $(EXAMPLES_DIR)/native-fungible && \
		FUNGIBLE_APP_ID=$$(../../$(LINERA_BIN) publish-and-create \
			../target/wasm32-unknown-unknown/release/native_fungible_{contract,service}.wasm \
			--json-argument '{ "accounts": {} }' \
			--json-parameters '{ "ticker_symbol": "NAT" }' | grep -oE '[a-f0-9]{64}' | tail -1) && \
		if [ -z "$$FUNGIBLE_APP_ID" ]; then \
			printf "$(RED)❌ Failed to deploy fungible app$(NC)\n"; \
			exit 1; \
		fi && \
		echo "  📋 Fungible App ID: $$FUNGIBLE_APP_ID" && \
		if [ -f ../../.env.apps ]; then \
			sed -i '/^FUNGIBLE_APP_ID=/d' ../../.env.apps 2>/dev/null || \
			sed -i '' '/^FUNGIBLE_APP_ID=/d' ../../.env.apps 2>/dev/null || true; \
		fi && \
		echo "FUNGIBLE_APP_ID=$$FUNGIBLE_APP_ID" >> ../../.env.apps && \
		echo "export LINERA_APPLICATION_ID=$$FUNGIBLE_APP_ID" > ../../.env.fungible && \
		echo "export LINERA_FAUCET_URL=https://faucet.$(NETWORK_NAME).linera.net" >> ../../.env.fungible && \
		echo "export CHAIN=$$CHAIN" >> ../../.env.fungible && \
		echo "export OWNER=$$OWNER" >> ../../.env.fungible
	@printf "$(GREEN)✅ Fungible token application deployed$(NC)\n"

deploy-apps-all: deploy-app-counter deploy-app-fungible ## Deploy all apps to blockchain

# ===== Demo Frontend Building (Per App) =====
build-demo-counter: ## Build counter web frontend
	@printf "$(YELLOW)🎨 Building counter web frontend...$(NC)\n"
	@if [ -f "$(EXAMPLES_DIR)/counter/package.json" ]; then \
		cd $(EXAMPLES_DIR)/counter && \
		if [ -f ../../.env.counter ]; then \
			. ../../.env.counter && \
			echo "LINERA_APPLICATION_ID=$$LINERA_APPLICATION_ID" > .env && \
			echo "LINERA_FAUCET_URL=$$LINERA_FAUCET_URL" >> .env; \
		fi && \
		pnpm install && \
		pnpm build; \
	else \
		printf "$(YELLOW)   No web interface to build for counter (using static HTML)$(NC)\n"; \
	fi
	@printf "$(GREEN)✅ Counter web frontend ready$(NC)\n"

build-demo-fungible: ## Build fungible web frontend
	@printf "$(YELLOW)🎨 Building fungible token web frontend...$(NC)\n"
	@if [ -f "$(EXAMPLES_DIR)/native-fungible/package.json" ]; then \
		cd $(EXAMPLES_DIR)/native-fungible && \
		if [ -f ../../.env.fungible ]; then \
			. ../../.env.fungible && \
			echo "LINERA_APPLICATION_ID=$$LINERA_APPLICATION_ID" > .env && \
			echo "LINERA_FAUCET_URL=$$LINERA_FAUCET_URL" >> .env; \
		fi && \
		pnpm install && \
		pnpm build; \
	elif [ -f "$(EXAMPLES_DIR)/fungible/package.json" ]; then \
		cd $(EXAMPLES_DIR)/fungible && \
		if [ -f ../../.env.fungible ]; then \
			. ../../.env.fungible && \
			echo "LINERA_APPLICATION_ID=$$LINERA_APPLICATION_ID" > .env && \
			echo "LINERA_FAUCET_URL=$$LINERA_FAUCET_URL" >> .env; \
		fi && \
		pnpm install && \
		pnpm build; \
	else \
		printf "$(YELLOW)   No web interface to build for fungible token$(NC)\n"; \
	fi
	@printf "$(GREEN)✅ Fungible token web frontend ready$(NC)\n"

build-demo-metamask: ## Build MetaMask web frontend
	@printf "$(YELLOW)🎨 Building MetaMask web frontend...$(NC)\n"
	@if [ -d "$(EXAMPLES_DIR)/counter/metamask" ] && [ -f "$(EXAMPLES_DIR)/counter/metamask/package.json" ]; then \
		cd $(EXAMPLES_DIR)/counter/metamask && \
		if [ -f ../../../.env.counter ]; then \
			. ../../../.env.counter && \
			echo "LINERA_APPLICATION_ID=$$LINERA_APPLICATION_ID" > .env && \
			echo "LINERA_FAUCET_URL=$$LINERA_FAUCET_URL" >> .env; \
		fi && \
		pnpm install && \
		pnpm build; \
	else \
		printf "$(YELLOW)   No MetaMask frontend found$(NC)\n"; \
	fi
	@printf "$(GREEN)✅ MetaMask web frontend ready$(NC)\n"

build-demos-all: build-demo-counter build-demo-fungible build-demo-metamask ## Build all web frontends

# ===== GCloud Authentication Check =====
check-gcloud-auth: ## Verify gcloud authentication for GCS
	@printf "$(YELLOW)🔐 Checking gcloud authentication...$(NC)\n"
	@if ! gcloud auth list --format="value(account)" 2>/dev/null | grep -q '@'; then \
		printf "$(RED)❌ Not authenticated with gcloud$(NC)\n"; \
		printf "$(YELLOW)   Please run: gcloud auth login && gcloud auth application-default login$(NC)\n"; \
		exit 1; \
	fi
	@printf "$(BLUE)   Active account: $$(gcloud auth list --filter=status:ACTIVE --format="value(account)")$(NC)\n"
	@printf "$(YELLOW)🔍 Verifying GCS access...$(NC)\n"
	@if ! gcloud storage ls $(GCS_BUCKET) >/dev/null 2>&1; then \
		printf "$(RED)❌ Cannot access GCS bucket: $(GCS_BUCKET)$(NC)\n"; \
		printf "$(YELLOW)   Please ensure you have proper permissions or run:$(NC)\n"; \
		printf "$(YELLOW)   gcloud auth application-default login$(NC)\n"; \
		exit 1; \
	fi
	@printf "$(GREEN)✅ GCloud authentication verified$(NC)\n"

# ===== Fetch .env from GCS =====
fetch-env-counter: check-gcloud-auth ## Fetch counter .env from GCS
	@printf "$(YELLOW)📥 Fetching counter .env from GCS...$(NC)\n"
	@if gcloud storage cp '$(DEMO_PATH)/counter/.env' .env.counter 2>/dev/null; then \
		printf "$(GREEN)✅ Counter .env downloaded to .env.counter$(NC)\n"; \
		cat .env.counter; \
	else \
		printf "$(RED)❌ Failed to fetch .env from $(DEMO_PATH)/counter/.env$(NC)\n"; \
		printf "$(YELLOW)   Make sure the app has been deployed to GCS first$(NC)\n"; \
		exit 1; \
	fi

fetch-env-fungible: check-gcloud-auth ## Fetch fungible .env from GCS
	@printf "$(YELLOW)📥 Fetching fungible .env from GCS...$(NC)\n"
	@if gcloud storage cp '$(DEMO_PATH)/fungible/.env' .env.fungible 2>/dev/null; then \
		printf "$(GREEN)✅ Fungible .env downloaded to .env.fungible$(NC)\n"; \
		cat .env.fungible; \
	else \
		printf "$(RED)❌ Failed to fetch .env from $(DEMO_PATH)/fungible/.env$(NC)\n"; \
		printf "$(YELLOW)   Make sure the app has been deployed to GCS first$(NC)\n"; \
		exit 1; \
	fi

fetch-env-all: fetch-env-counter fetch-env-fungible ## Fetch all .env files from GCS

# ===== CDN Cache Invalidation =====
invalidate-cache: ## Invalidate CDN cache for a specific path
	@if [ -z "$(CACHE_PATH)" ]; then \
		printf "$(RED)❌ CACHE_PATH not specified$(NC)\n"; \
		printf "$(YELLOW)   Usage: make invalidate-cache CACHE_PATH=/testnet/counter/*$(NC)\n"; \
		exit 1; \
	fi
	@printf "$(YELLOW)🔄 Invalidating CDN cache for path: $(CACHE_PATH)$(NC)\n"
	@gcloud compute url-maps invalidate-cdn-cache $(URL_MAP) \
		--path "$(CACHE_PATH)" \
		--global \
		--async
	@printf "$(GREEN)✅ Cache invalidation initiated for $(CACHE_PATH)$(NC)\n"
	@printf "$(BLUE)   Note: Cache invalidation may take a few minutes to propagate globally$(NC)\n"

# ===== GCS Deployment (Per Demo) =====
deploy-gcs-counter: check-gcloud-auth build-demo-counter ## Deploy counter to GCS
	@printf "$(RED)⚠️  WARNING: This will deploy to Google Cloud Storage!$(NC)\n"
	@printf "$(YELLOW)Target: $(DEMO_PATH)/counter/$(NC)\n"
	@printf "Type 'YES' (all uppercase) to continue: " && read confirm && [ "$$confirm" = "YES" ] || { printf "$(RED)Deployment cancelled$(NC)\n"; exit 1; }
	@printf "$(YELLOW)☁️  Deploying counter demo to GCS...$(NC)\n"
	@if [ -d "$(EXAMPLES_DIR)/counter/dist" ]; then \
		gcloud storage rsync -r --delete-unmatched-destination-objects \
			$(EXAMPLES_DIR)/counter/dist/ \
			'$(DEMO_PATH)/counter/'; \
	else \
		printf "$(YELLOW)   No counter demo found$(NC)\n"; \
	fi
	@if [ -f .env.counter ]; then \
		gcloud storage cp .env.counter '$(DEMO_PATH)/counter/.env'; \
	fi
	@printf "$(GREEN)✅ Counter deployed: https://demos.linera.net/$(NETWORK_TYPE)/counter/$(NC)\n"
	@$(MAKE) invalidate-cache CACHE_PATH=/$(NETWORK_TYPE)/counter/*

deploy-gcs-fungible: check-gcloud-auth build-demo-fungible ## Deploy fungible to GCS
	@printf "$(RED)⚠️  WARNING: This will deploy to Google Cloud Storage!$(NC)\n"
	@printf "$(YELLOW)Target: $(DEMO_PATH)/fungible/$(NC)\n"
	@printf "Type 'YES' (all uppercase) to continue: " && read confirm && [ "$$confirm" = "YES" ] || { printf "$(RED)Deployment cancelled$(NC)\n"; exit 1; }
	@printf "$(YELLOW)☁️  Deploying fungible token demo to GCS...$(NC)\n"
	@if [ -d "$(EXAMPLES_DIR)/native-fungible/dist" ]; then \
		gcloud storage rsync -r --delete-unmatched-destination-objects \
			$(EXAMPLES_DIR)/native-fungible/dist/ \
			'$(DEMO_PATH)/fungible/'; \
	else \
		printf "$(YELLOW)   No fungible token demo found$(NC)\n"; \
	fi
	@if [ -f .env.fungible ]; then \
		gcloud storage cp .env.fungible '$(DEMO_PATH)/fungible/.env'; \
	fi
	@printf "$(GREEN)✅ Fungible token deployed: https://demos.linera.net/$(NETWORK_TYPE)/fungible/$(NC)\n"
	@$(MAKE) invalidate-cache CACHE_PATH=/$(NETWORK_TYPE)/fungible/*

deploy-gcs-metamask: check-gcloud-auth build-demo-metamask ## Deploy MetaMask to GCS
	@printf "$(RED)⚠️  WARNING: This will deploy to Google Cloud Storage!$(NC)\n"
	@printf "$(YELLOW)Target: $(DEMO_PATH)/metamask/$(NC)\n"
	@printf "Type 'YES' (all uppercase) to continue: " && read confirm && [ "$$confirm" = "YES" ] || { printf "$(RED)Deployment cancelled$(NC)\n"; exit 1; }
	@printf "$(YELLOW)☁️  Deploying MetaMask demo to GCS...$(NC)\n"
	@if [ -d "$(EXAMPLES_DIR)/counter/metamask/dist" ]; then \
		gcloud storage rsync -r --delete-unmatched-destination-objects \
			$(EXAMPLES_DIR)/counter/metamask/dist/ \
			'$(DEMO_PATH)/metamask/'; \
	else \
		printf "$(YELLOW)   No MetaMask demo found$(NC)\n"; \
	fi
	@if [ -f .env.counter ]; then \
		gcloud storage cp .env.counter '$(DEMO_PATH)/metamask/.env'; \
	fi
	@printf "$(GREEN)✅ MetaMask deployed: https://demos.linera.net/$(NETWORK_TYPE)/metamask/$(NC)\n"
	@$(MAKE) invalidate-cache CACHE_PATH=/$(NETWORK_TYPE)/metamask/*

deploy-gcs-all: deploy-gcs-counter deploy-gcs-fungible deploy-gcs-metamask ## Deploy all to GCS

# ===== Legacy compatibility targets (will be deprecated) =====
create-env: ## Create .env files from deployed app IDs
	@printf "$(YELLOW)📝 Creating .env files...$(NC)\n"
	@if [ -f .env.apps ]; then \
		. ./.env.apps && \
		mkdir -p $(EXAMPLES_DIR)/counter && \
		cat > $(EXAMPLES_DIR)/counter/.env <<-EOF && \
			LINERA_FAUCET_URL=https://faucet.$(NETWORK_NAME).linera.net/ \
			LINERA_APPLICATION_ID=$$COUNTER_APP_ID \
		EOF \
		cp $(EXAMPLES_DIR)/counter/.env $(EXAMPLES_DIR)/native-fungible/.env 2>/dev/null || true && \
		if [ -d "$(EXAMPLES_DIR)/counter/metamask" ]; then \
			cp $(EXAMPLES_DIR)/counter/.env $(EXAMPLES_DIR)/counter/metamask/.env; \
		fi && \
		printf "$(GREEN)✅ .env files created$(NC)\n"; \
	else \
		printf "$(RED)❌ No .env.apps file found. Deploy apps first$(NC)\n"; \
		exit 1; \
	fi

# ===== Verification =====
verify: ## Verify deployed demos
	@printf "$(YELLOW)🔍 Verifying deployments...$(NC)\n"
	@echo "  Testing Counter demo..."
	@curl -s -o /dev/null -w "%{http_code}" https://demos.linera.net/$(NETWORK_TYPE)/counter/ | grep -q "200" && \
		printf "  $(GREEN)✅ Counter demo accessible$(NC)\n" || printf "  $(RED)❌ Counter demo not accessible$(NC)\n"
	@echo "  Testing Fungible Token demo..."
	@curl -s -o /dev/null -w "%{http_code}" https://demos.linera.net/$(NETWORK_TYPE)/fungible/ | grep -q "200" && \
		printf "  $(GREEN)✅ Fungible token demo accessible$(NC)\n" || printf "  $(RED)❌ Fungible token demo not accessible$(NC)\n"
	@echo "  Testing MetaMask demo..."
	@curl -s -o /dev/null -w "%{http_code}" https://demos.linera.net/$(NETWORK_TYPE)/metamask/ | grep -q "200" && \
		printf "  $(GREEN)✅ MetaMask demo accessible$(NC)\n" || printf "  $(RED)❌ MetaMask demo not accessible$(NC)\n"

# ===== Cleanup =====
clean: ## Clean build artifacts and temporary files
	@printf "$(YELLOW)🧹 Cleaning build artifacts...$(NC)\n"
	@rm -f .env.apps .env.counter .env.fungible .env.wallet
	@if [ -d "$(WALLET_DIR)" ] && [ "$$(echo $(WALLET_DIR) | grep '^/tmp/linera-wallet')" ]; then \
		echo "  📂 Removing temporary wallet directory: $(WALLET_DIR)"; \
		rm -rf $(WALLET_DIR); \
	fi
	@if [ -d "$(EXAMPLES_DIR)" ]; then \
		rm -rf $(EXAMPLES_DIR)/counter/dist; \
		rm -rf $(EXAMPLES_DIR)/counter/node_modules; \
		rm -f $(EXAMPLES_DIR)/counter/.env; \
		rm -rf $(EXAMPLES_DIR)/native-fungible/dist; \
		rm -rf $(EXAMPLES_DIR)/native-fungible/node_modules; \
		rm -f $(EXAMPLES_DIR)/native-fungible/.env; \
		rm -rf $(EXAMPLES_DIR)/fungible/dist; \
		rm -rf $(EXAMPLES_DIR)/fungible/node_modules; \
		rm -f $(EXAMPLES_DIR)/fungible/.env; \
		rm -rf $(EXAMPLES_DIR)/counter/metamask/dist; \
		rm -rf $(EXAMPLES_DIR)/counter/metamask/node_modules; \
		rm -f $(EXAMPLES_DIR)/counter/metamask/.env; \
	fi
	@printf "$(GREEN)✅ Cleaned$(NC)\n"

# ===== Cargo Clean Targets =====
clean-cargo-all: ## Clean all Cargo build artifacts (main + examples)
	@printf "$(YELLOW)🧹 Cleaning all Cargo build artifacts...$(NC)\n"
	@cargo clean
	@cd $(EXAMPLES_DIR)/counter && cargo clean
	@cd $(EXAMPLES_DIR)/native-fungible && cargo clean
	@if [ -d "$(EXAMPLES_DIR)/fungible" ]; then cd $(EXAMPLES_DIR)/fungible && cargo clean; fi
	@printf "$(GREEN)✅ All Cargo artifacts cleaned$(NC)\n"

clean-cargo-main: ## Clean main Cargo build artifacts (linera binary)
	@printf "$(YELLOW)🧹 Cleaning main Cargo build artifacts...$(NC)\n"
	@cargo clean
	@printf "$(GREEN)✅ Main Cargo artifacts cleaned$(NC)\n"

clean-cargo-counter: ## Clean counter Cargo build artifacts
	@printf "$(YELLOW)🧹 Cleaning counter Cargo build artifacts...$(NC)\n"
	@cd $(EXAMPLES_DIR)/counter && cargo clean
	@printf "$(GREEN)✅ Counter Cargo artifacts cleaned$(NC)\n"

clean-cargo-fungible: ## Clean fungible token Cargo build artifacts
	@printf "$(YELLOW)🧹 Cleaning fungible token Cargo build artifacts...$(NC)\n"
	@cd $(EXAMPLES_DIR)/native-fungible && cargo clean
	@if [ -d "$(EXAMPLES_DIR)/fungible" ]; then cd $(EXAMPLES_DIR)/fungible && cargo clean; fi
	@printf "$(GREEN)✅ Fungible token Cargo artifacts cleaned$(NC)\n"

clean-all: clean clean-cargo-all ## Clean everything (npm + cargo)

# ===== Composite Workflows =====
# Full deployments
counter-full: setup init-wallet deploy-app-counter deploy-gcs-counter verify ## Complete counter deployment (wallet to GCS)
fungible-full: setup init-wallet deploy-app-fungible deploy-gcs-fungible verify ## Complete fungible deployment (wallet to GCS)  
full-deploy: setup init-wallet deploy-apps-all deploy-gcs-all verify ## Complete deployment of all apps (wallet to GCS)

# Quick deployments (assumes wallet and apps deployed)
counter-quick: fetch-env-counter build-demo-counter deploy-gcs-counter verify ## Quick counter frontend update (no blockchain)
fungible-quick: fetch-env-fungible build-demo-fungible deploy-gcs-fungible verify ## Quick fungible frontend update (no blockchain)
quick-deploy: fetch-env-all build-demos-all deploy-gcs-all verify ## Quick frontend update all demos (no blockchain)

# ===== Build Linera Binary =====
build-linera: ## Build the linera binary (required first)
	@printf "$(YELLOW)🔨 Building linera binary...$(NC)\n"
	@cargo build --release
	@printf "$(GREEN)✅ Linera binary built at $(LINERA_BIN)$(NC)\n"

# Additional Targets
# Add more targets below as needed for other functionality

.PHONY: clean-cargo-all clean-cargo-main clean-cargo-counter clean-cargo-fungible clean-all
