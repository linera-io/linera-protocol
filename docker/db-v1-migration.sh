#!/usr/bin/env bash
set -euo pipefail

# Linera Cold DB Migration Script
# Usage: ./migration.sh pre   - Run before migration (backup, stop, migrate)
#        ./migration.sh post  - Run after migration (restore normal operation)

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly MIGRATION_IMAGE="us-docker.pkg.dev/linera-io-dev/linera-public-registry/linera:testnet_conway_db_migration"
readonly RELEASE_IMAGE="us-docker.pkg.dev/linera-io-dev/linera-public-registry/linera:testnet_conway_release"
readonly LOG_PATTERN="${LOG_PATTERN:-Running shard}" # Shard logs this after migration
readonly BACKUP_DIR="${SCRIPT_DIR}/backups"

# Colors (ANSI fallback)
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly CYAN='\033[0;36m'
readonly BOLD='\033[1m'
readonly NC='\033[0m' # No Color

# Detect gum availability
detect_gum() {
	if command -v gum &>/dev/null; then
		echo "native"
	elif docker image inspect ghcr.io/charmbracelet/gum:latest &>/dev/null 2>&1; then
		echo "docker"
	elif docker pull ghcr.io/charmbracelet/gum:latest &>/dev/null 2>&1; then
		echo "docker"
	else
		echo "none"
	fi
}

GUM_MODE="$(detect_gum)"

# --- UI Wrapper Functions (gum with bash fallback) ---

ui_header() {
	local title="$1"
	if [[ "$GUM_MODE" == "native" ]]; then
		gum style --border double --padding "1 4" --border-foreground 212 "$title"
	elif [[ "$GUM_MODE" == "docker" ]]; then
		docker run --rm ghcr.io/charmbracelet/gum style --border double --padding "1 4" --border-foreground 212 "$title"
	else
		echo ""
		echo -e "${BOLD}${CYAN}════════════════════════════════════════${NC}"
		echo -e "${BOLD}${CYAN}  $title${NC}"
		echo -e "${BOLD}${CYAN}════════════════════════════════════════${NC}"
		echo ""
	fi
}

ui_step() {
	local step="$1"
	local desc="$2"
	if [[ "$GUM_MODE" == "native" ]]; then
		gum style --foreground 212 "[$step]" --bold "$desc"
	elif [[ "$GUM_MODE" == "docker" ]]; then
		docker run --rm ghcr.io/charmbracelet/gum style --foreground 212 "[$step] $desc"
	else
		echo -e "${BLUE}${BOLD}[$step]${NC} $desc"
	fi
}

ui_info() {
	local msg="$1"
	if [[ "$GUM_MODE" == "native" ]]; then
		gum style --foreground 117 "  → $msg"
	elif [[ "$GUM_MODE" == "docker" ]]; then
		docker run --rm ghcr.io/charmbracelet/gum style --foreground 117 "  → $msg"
	else
		echo -e "  ${CYAN}→${NC} $msg"
	fi
}

ui_success() {
	local msg="$1"
	if [[ "$GUM_MODE" == "native" ]]; then
		gum style --foreground 82 "  ✓ $msg"
	elif [[ "$GUM_MODE" == "docker" ]]; then
		docker run --rm ghcr.io/charmbracelet/gum style --foreground 82 "  ✓ $msg"
	else
		echo -e "  ${GREEN}✓${NC} $msg"
	fi
}

ui_warn() {
	local msg="$1"
	if [[ "$GUM_MODE" == "native" ]]; then
		gum style --foreground 214 "  ⚠ $msg"
	elif [[ "$GUM_MODE" == "docker" ]]; then
		docker run --rm ghcr.io/charmbracelet/gum style --foreground 214 "  ⚠ $msg"
	else
		echo -e "  ${YELLOW}⚠${NC} $msg"
	fi
}

ui_error() {
	local msg="$1"
	if [[ "$GUM_MODE" == "native" ]]; then
		gum style --foreground 196 "  ✗ $msg"
	elif [[ "$GUM_MODE" == "docker" ]]; then
		docker run --rm ghcr.io/charmbracelet/gum style --foreground 196 "  ✗ $msg"
	else
		echo -e "  ${RED}✗${NC} $msg"
	fi
}

ui_spin() {
	local title="$1"
	shift
	if [[ "$GUM_MODE" == "native" ]]; then
		gum spin --spinner dot --title "$title" -- "$@"
	elif [[ "$GUM_MODE" == "docker" ]]; then
		# Can't easily run host commands from docker gum, fall back
		echo -e "  ${CYAN}⏳${NC} $title"
		"$@"
	else
		echo -e "  ${CYAN}⏳${NC} $title"
		"$@"
	fi
}

ui_confirm() {
	local prompt="$1"
	if [[ "$GUM_MODE" == "native" ]]; then
		gum confirm "$prompt"
	elif [[ "$GUM_MODE" == "docker" ]]; then
		docker run --rm -it ghcr.io/charmbracelet/gum confirm "$prompt"
	else
		read -rp "$prompt [y/N] " response
		[[ "$response" =~ ^[Yy]$ ]]
	fi
}

# --- Helper Functions ---

check_prerequisites() {
	ui_step "0" "Checking prerequisites"

	if ! command -v docker &>/dev/null; then
		ui_error "docker not found"
		exit 1
	fi
	ui_success "docker available"

	if ! docker compose version &>/dev/null; then
		ui_error "docker compose not found"
		exit 1
	fi
	ui_success "docker compose available"

	if [[ ! -f "$SCRIPT_DIR/docker-compose.yml" ]]; then
		ui_error "docker-compose.yml not found in $SCRIPT_DIR"
		exit 1
	fi
	ui_success "docker-compose.yml found"

	if [[ ! -f "$SCRIPT_DIR/docker-compose.migration.yml" ]]; then
		ui_error "docker-compose.migration.yml not found - did you git pull?"
		exit 1
	fi
	ui_success "docker-compose.migration.yml found"

	ui_info "Using TUI mode: $GUM_MODE"
}

set_image_tag() {
	local image="$1"
	local env_file="$SCRIPT_DIR/.env"

	if [[ -f "$env_file" ]] && grep -q '^LINERA_IMAGE=' "$env_file"; then
		sed -i "s|^LINERA_IMAGE=.*|LINERA_IMAGE=$image|" "$env_file"
	else
		echo "LINERA_IMAGE=$image" >>"$env_file"
	fi
}

get_current_image() {
	local env_file="$SCRIPT_DIR/.env"
	if [[ -f "$env_file" ]]; then
		grep '^LINERA_IMAGE=' "$env_file" | cut -d= -f2 || echo "$RELEASE_IMAGE"
	else
		echo "$RELEASE_IMAGE"
	fi
}

backup_scylla() {
	local backup_file="$BACKUP_DIR/scylla-backup-$(date +%Y%m%d-%H%M%S).tar.gz"
	mkdir -p "$BACKUP_DIR"

	docker run --rm \
		-v linera-scylla-data:/data:ro \
		-v "$BACKUP_DIR:/backup" \
		alpine tar czf "/backup/$(basename "$backup_file")" -C /data .

	echo "$backup_file"
}

wait_for_migration() {
	local timeout="${1:-1800}"
	local elapsed=0

	ui_info "Watching logs for: '$LOG_PATTERN'"
	ui_info "Expected duration: ~15 minutes (timeout: ${timeout}s)"
	echo ""
	ui_warn "Note: ScyllaDB takes ~3 minutes to initialize before migration starts"
	echo ""
	ui_info "TIP: Monitor progress in another terminal with:"
	ui_info "  docker compose logs -f"
	ui_info "(You can safely Ctrl+C that when done)"
	echo ""

	while ! docker compose -f "$SCRIPT_DIR/docker-compose.yml" -f "$SCRIPT_DIR/docker-compose.migration.yml" logs shard 2>&1 | grep -q "$LOG_PATTERN"; do
		if ((elapsed >= timeout)); then
			ui_error "Migration timed out after ${timeout}s"
			return 1
		fi
		sleep 5
		elapsed=$((elapsed + 5))
		printf "\r  ⏳ Waiting... %ds elapsed" "$elapsed"
	done

	echo ""
	return 0
}

# --- Main Commands ---

cmd_pre() {
	ui_header "Linera Cold Migration - PRE"

	check_prerequisites
	echo ""

	# Step 1: Show current state
	ui_step "1" "Current state"
	ui_info "Image: $(get_current_image)"
	echo ""

	# Step 2: Confirm
	if ! ui_confirm "This will stop your validator for migration. Continue?"; then
		ui_warn "Aborted by user"
		exit 0
	fi
	echo ""

	# Step 3: Switch to migration image
	ui_step "2" "Switching to migration image"
	set_image_tag "$MIGRATION_IMAGE"
	ui_success "Updated .env to use migration image"
	ui_info "Image: $MIGRATION_IMAGE"
	echo ""

	# Step 4: Stop services
	ui_step "3" "Stopping all services"
	docker compose -f "$SCRIPT_DIR/docker-compose.yml" down
	ui_success "Services stopped"
	echo ""

	# Step 5: Pull new image
	ui_step "4" "Pulling migration image"
	docker compose -f "$SCRIPT_DIR/docker-compose.yml" pull
	ui_success "Image pulled"
	echo ""

	# Step 6: Backup
	ui_step "5" "Backing up ScyllaDB data"
	if ui_confirm "Create backup before migration? (recommended)"; then
		local backup_file
		backup_file="$(backup_scylla)"
		ui_success "Backup saved: $backup_file"
	else
		ui_warn "Skipping backup"
	fi
	echo ""

	# Step 7: Run migration
	ui_step "6" "Starting migration (single shard mode)"
	docker compose -f "$SCRIPT_DIR/docker-compose.yml" -f "$SCRIPT_DIR/docker-compose.migration.yml" up -d
	ui_success "Migration shard started"
	echo ""

	# Step 8: Wait for completion
	ui_step "7" "Waiting for migration to complete"
	if wait_for_migration 1800; then
		ui_success "Migration pattern detected!"
	else
		ui_error "Migration may have failed - check logs manually:"
		ui_info "docker compose logs shard"
		exit 1
	fi
	echo ""

	# Step 9: Restart normally
	ui_step "8" "Restarting all services"
	docker compose -f "$SCRIPT_DIR/docker-compose.yml" down
	docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d
	ui_success "All services started with migration image"
	echo ""

	ui_header "PRE-MIGRATION COMPLETE"
	ui_warn "Keep running with migration tag until instructed to run: ./migration.sh post"
}

cmd_post() {
	ui_header "Linera Cold Migration - POST"

	check_prerequisites
	echo ""

	# Step 1: Confirm
	ui_step "1" "Restoring release image"
	ui_info "Current: $(get_current_image)"
	ui_info "Target:  $RELEASE_IMAGE"
	echo ""

	if ! ui_confirm "Switch back to release image?"; then
		ui_warn "Aborted by user"
		exit 0
	fi
	echo ""

	# Step 2: Switch back to release image
	ui_step "2" "Updating image tag"
	set_image_tag "$RELEASE_IMAGE"
	ui_success "Set LINERA_IMAGE to release tag"
	echo ""

	# Step 3: Pull and restart
	ui_step "3" "Pulling and restarting"
	docker compose -f "$SCRIPT_DIR/docker-compose.yml" pull
	docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d
	ui_success "Services restarted with release image"
	echo ""

	ui_header "POST-MIGRATION COMPLETE"
	ui_success "Validator restored to normal operation!"
}

cmd_help() {
	cat <<EOF
Linera Cold DB Migration Script

Usage: ./db-v1-migration.sh <command>

Commands:
  pre     Run pre-migration (backup, stop, migrate, restart)
          Expected duration: ~15 minutes (+3 min ScyllaDB init)
  post    Run post-migration (switch back to release image)
  help    Show this help message

Environment Variables:
  LOG_PATTERN   Pattern to detect migration completion (default: "Running shard")

EOF
}

# --- Entry Point ---

main() {
	cd "$SCRIPT_DIR"

	case "${1:-}" in
	pre) cmd_pre ;;
	post) cmd_post ;;
	help) cmd_help ;;
	*)
		ui_error "Missing or invalid command"
		echo ""
		cmd_help
		exit 1
		;;
	esac
}

main "$@"
