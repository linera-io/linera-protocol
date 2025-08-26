#!/usr/bin/env bash
#
# Deploy Validator Script
#
# Purpose: Deploy a Linera validator node using Docker Compose
#
# Usage: ./deploy-validator.sh <host> [OPTIONS]
#
# Arguments:
#   <host>           - The hostname/domain for the validator (required)
#
# Options:
#   --remote-image      - Use remote Docker image instead of building locally
#   --skip-genesis      - Skip downloading genesis configuration
#   --force-genesis     - Force re-download of genesis configuration
#   --help, -h          - Show help message
#   --dry-run           - Show what would be done without executing
#   --verbose, -v       - Enable verbose output
#
# Environment Variables:
#   ACME_EMAIL       - Email for Let's Encrypt certificates (default: infra@linera.io)
#   LINERA_IMAGE     - Override the Docker image to use
#   GENESIS_URL      - Override the genesis configuration URL
#   PORT             - Internal validator port (default: 19100)
#   METRICS_PORT     - Metrics collection port (default: 21100)
#   NUM_SHARDS       - Number of validator shards (default: 4)
#
# Requirements:
#   - Docker
#   - Docker Compose (as plugin)
#   - Git (for branch detection)
#   - wget (for genesis download)
#
# Author: Linera Team
# Date: $(date +%Y-%m-%d)

set -euo pipefail

# -----------------------------------------------------------------------------
# Configuration and Defaults
# -----------------------------------------------------------------------------

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Color codes for output
readonly RED='[0;31m'
readonly GREEN='[0;32m'
readonly YELLOW='[1;33m'
readonly BLUE='[0;34m'
readonly NC='[0m' # No Color

# Default configuration
readonly DEFAULT_ACME_EMAIL="infra@linera.io"
readonly DEFAULT_PORT="19100"
readonly DEFAULT_METRICS_PORT="21100"
readonly DEFAULT_NUM_SHARDS="4"
readonly DEFAULT_DOCKER_REGISTRY="us-docker.pkg.dev/linera-io-dev/linera-public-registry"

# Configuration paths
readonly VALIDATOR_CONFIG_PATH="docker/validator-config.toml"
readonly GENESIS_CONFIG_PATH="docker/genesis.json"
readonly DOCKER_COMPOSE_DIR="docker"

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------

# Print colored output
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp="$(date '+%Y-%m-%d %H:%M:%S')"

    case "$level" in
        ERROR)
            echo -e "${RED}[ERROR]${NC} ${timestamp} - ${message}" >&2
            ;;
        WARNING)
            echo -e "${YELLOW}[WARNING]${NC} ${timestamp} - ${message}" >&2
            ;;
        INFO)
            echo -e "${GREEN}[INFO]${NC} ${timestamp} - ${message}"
            ;;
        DEBUG)
            if [[ "${DEBUG:-0}" == "1" ]]; then
                echo -e "${BLUE}[DEBUG]${NC} ${timestamp} - ${message}"
            fi
            ;;
        *)
            echo "${timestamp} - ${message}"
            ;;
    esac
}

# Print usage information
usage() {
    cat <<EOF
Usage: $(basename "$0") <host> [OPTIONS]

Deploy a Linera validator node using Docker Compose.

ARGUMENTS:
    <host>              The hostname/domain for the validator (required)

OPTIONS:
    --remote-image      Use remote Docker image instead of building locally
    --skip-genesis      Skip downloading genesis configuration
    --force-genesis     Force re-download of genesis configuration even if it exists
    --help, -h          Show this help message
    --dry-run           Show what would be done without executing
    --verbose, -v       Enable verbose output

ENVIRONMENT VARIABLES:
    ACME_EMAIL          Email for Let's Encrypt certificates
                        Default: ${DEFAULT_ACME_EMAIL}

    LINERA_IMAGE        Override the Docker image to use
                        Default: Auto-detected based on branch

    GENESIS_URL         Override the genesis configuration URL
                        Default: Auto-generated based on branch

    PORT                Internal validator port
                        Default: ${DEFAULT_PORT}

    METRICS_PORT        Metrics collection port
                        Default: ${DEFAULT_METRICS_PORT}

    NUM_SHARDS          Number of validator shards
                        Default: ${DEFAULT_NUM_SHARDS}

EXAMPLES:
    # Deploy using local build
    $(basename "$0") validator.example.com

    # Deploy using remote image
    $(basename "$0") validator.example.com --remote-image

    # Deploy with custom configuration
    ACME_EMAIL=admin@example.com NUM_SHARDS=8 $(basename "$0") validator.example.com

    # Skip genesis download (use existing)
    $(basename "$0") validator.example.com --skip-genesis

    # Force re-download genesis
    $(basename "$0") validator.example.com --force-genesis

    # Dry run to see what would happen
    $(basename "$0") validator.example.com --dry-run

EOF
}

# Check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check if Docker Compose plugin is installed
docker_compose_plugin_installed() {
    docker compose version >/dev/null 2>&1
}

# Verify all required dependencies
verify_dependencies() {
    local missing_deps=()
    local optional_deps=()

    if ! command_exists docker; then
        missing_deps+=("Docker")
    fi

    if ! docker_compose_plugin_installed; then
        missing_deps+=("Docker Compose plugin")
    fi

    if ! command_exists git; then
        missing_deps+=("Git")
    fi

    if ! command_exists wget; then
        optional_deps+=("wget (optional, needed for genesis download)")
    fi

    if [ ${#missing_deps[@]} -gt 0 ]; then
        log ERROR "Missing required dependencies: ${missing_deps[*]}"
        log ERROR "Please install the missing dependencies before running this script."
        exit 1
    fi

    if [ ${#optional_deps[@]} -gt 0 ]; then
        log WARNING "Missing optional dependencies: ${optional_deps[*]}"
    fi

    log DEBUG "All required dependencies verified successfully"
}

# Get Git branch information
get_git_info() {
    local branch_name
    local git_commit

    if ! branch_name=$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null); then
        log WARNING "Unable to detect Git branch, using 'unknown'"
        branch_name="unknown"
    fi

    if ! git_commit=$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null); then
        log WARNING "Unable to detect Git commit, using 'unknown'"
        git_commit="unknown"
    fi

    # Replace underscores with dashes in branch name (for URL compatibility)
    local formatted_branch="${branch_name//_/-}"

    echo "$branch_name|$formatted_branch|$git_commit"
}

# Build Docker image locally
build_local_image() {
    local git_commit="$1"
    local image_tag="${2:-linera}"

    log INFO "Building local Docker image from commit ${git_commit}..."

    if [[ "${DRY_RUN:-0}" == "1" ]]; then
        log INFO "[DRY RUN] Would build: docker build --build-arg git_commit=${git_commit} -f docker/Dockerfile . -t ${image_tag}"
        return 0
    fi

    if ! docker build \
        --build-arg git_commit="${git_commit}" \
        -f "${REPO_ROOT}/docker/Dockerfile" \
        "${REPO_ROOT}" \
        -t "${image_tag}"; then
        log ERROR "Failed to build Docker image"
        return 1
    fi

    log INFO "Successfully built Docker image: ${image_tag}"
    return 0
}

# Generate validator configuration file
generate_validator_config() {
    local host="$1"
    local port="${2:-$DEFAULT_PORT}"
    local metrics_port="${3:-$DEFAULT_METRICS_PORT}"
    local num_shards="${4:-$DEFAULT_NUM_SHARDS}"
    local config_path="${REPO_ROOT}/${VALIDATOR_CONFIG_PATH}"

    log INFO "Generating validator configuration for ${host}..."

    if [[ "${DRY_RUN:-0}" == "1" ]]; then
        log INFO "[DRY RUN] Would create configuration at: ${config_path}"
        return 0
    fi

    # Create configuration directory if it doesn't exist
    mkdir -p "$(dirname "${config_path}")"

    # Generate the configuration file
    cat > "${config_path}" <<EOF

server_config_path = "server.json"
host = "${host}"
port = 443

[external_protocol]
Grpc = "Tls"

[internal_protocol]
Grpc = "ClearText"

[[proxies]]
host = "proxy"
public_port = 443
private_port = 20100
metrics_port = ${metrics_port}

EOF

    # Generate shard configurations
    for i in $(seq 1 "${num_shards}"); do
        cat >> "${config_path}" <<EOF
[[shards]]
host = "docker-shard-${i}"
port = ${port}
metrics_port = ${metrics_port}

EOF
    done

    log INFO "Validator configuration generated at: ${config_path}"
    return 0
}

# Prompt user for confirmation
confirm() {
    local prompt="$1"
    local response

    if [[ "${DRY_RUN:-0}" == "1" ]]; then
        log INFO "[DRY RUN] Would prompt: ${prompt}"
        return 0
    fi

    # If running non-interactively, assume 'no'
    if [ ! -t 0 ]; then
        log DEBUG "Non-interactive mode, defaulting to 'no' for: ${prompt}"
        return 1
    fi

    read -r -p "${prompt} [y/N]: " response
    case "$response" in
        [yY][eE][sS]|[yY])
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# Download genesis configuration
download_genesis_config() {
    local genesis_url="$1"
    local skip_genesis="${2:-0}"
    local force_genesis="${3:-0}"
    local config_path="${REPO_ROOT}/${GENESIS_CONFIG_PATH}"

    # Check if we should skip genesis download
    if [[ "${skip_genesis}" == "1" ]]; then
        log INFO "Skipping genesis configuration download (--skip-genesis specified)"
        return 0
    fi

    # Check if wget is available
    if ! command_exists wget; then
        log WARNING "wget is not installed, cannot download genesis configuration"
        log WARNING "Install wget or use --skip-genesis to continue without genesis download"
        return 1
    fi

    log INFO "Genesis configuration management:"
    log INFO "  URL: ${genesis_url}"
    log INFO "  Target: ${config_path}"

    if [[ "${DRY_RUN:-0}" == "1" ]]; then
        if [ -f "${config_path}" ]; then
            log INFO "[DRY RUN] Genesis configuration exists at: ${config_path}"
            if [[ "${force_genesis}" == "1" ]]; then
                log INFO "[DRY RUN] Would force re-download (--force-genesis specified)"
            else
                log INFO "[DRY RUN] Would prompt for re-download"
            fi
        else
            log INFO "[DRY RUN] Would download genesis configuration to: ${config_path}"
        fi
        return 0
    fi

    # Create configuration directory if it doesn't exist
    mkdir -p "$(dirname "${config_path}")"

    # Check if genesis configuration already exists
    if [ -f "${config_path}" ]; then
        log WARNING "Genesis configuration already exists at: ${config_path}"

        if [[ "${force_genesis}" == "1" ]]; then
            log INFO "Force re-downloading genesis configuration (--force-genesis specified)"
        else
            if confirm "Do you want to re-download and overwrite the existing genesis configuration?"; then
                log INFO "Re-downloading genesis configuration..."
            else
                log INFO "Using existing genesis configuration"
                return 0
            fi
        fi

        # Backup existing configuration
        local backup_path="${config_path}.backup.$(date +%Y%m%d-%H%M%S)"
        log INFO "Creating backup at: ${backup_path}"
        cp "${config_path}" "${backup_path}"
    else
        log INFO "Downloading genesis configuration..."
    fi

    # Download the genesis configuration
    if ! wget -O "${config_path}" "${genesis_url}" 2>&1 | while IFS= read -r line; do
        log DEBUG "wget: ${line}"
    done; then
        log ERROR "Failed to download genesis configuration from: ${genesis_url}"
        if [ -f "${config_path}.backup."* ]; then
            local latest_backup=$(ls -t "${config_path}.backup."* | head -1)
            log INFO "Restoring from backup: ${latest_backup}"
            mv "${latest_backup}" "${config_path}"
        fi
        return 1
    fi

    log INFO "Successfully downloaded genesis configuration"
    return 0
}

# Generate validator keys
generate_validator_keys() {
    local image="$1"
    local config_file="validator-config.toml"

    log INFO "Generating validator keys..."

    if [[ "${DRY_RUN:-0}" == "1" ]]; then
        log INFO "[DRY RUN] Would generate validator keys using image: ${image}"
        echo "DRY_RUN_PUBLIC_KEY"
        return 0
    fi

    local public_key
    public_key=$(docker run --rm \
        -v "${REPO_ROOT}/${DOCKER_COMPOSE_DIR}:/config" \
        -w /config \
        "${image}" \
        /linera-server generate --validators "${config_file}")

    if [ -z "${public_key}" ]; then
        log ERROR "Failed to generate validator keys"
        return 1
    fi

    echo "${public_key}"
    return 0
}

# Start Docker Compose services
start_services() {
    local compose_dir="${REPO_ROOT}/${DOCKER_COMPOSE_DIR}"

    log INFO "Starting Docker Compose services..."

    if [[ "${DRY_RUN:-0}" == "1" ]]; then
        log INFO "[DRY RUN] Would start Docker Compose in: ${compose_dir}"
        return 0
    fi

    cd "${compose_dir}"

    if ! docker compose up --wait; then
        log ERROR "Failed to start Docker Compose services"
        log ERROR "To see logs, run: cd ${compose_dir} && docker compose logs"
        return 1
    fi

    log INFO "Docker Compose services started successfully"
    return 0
}

# Stop Docker Compose services
stop_services() {
    local compose_dir="${REPO_ROOT}/${DOCKER_COMPOSE_DIR}"

    log INFO "Stopping Docker Compose services..."

    cd "${compose_dir}"
    docker compose down || true
}

# Cleanup function
cleanup() {
    local exit_code=$?

    if [ ${exit_code} -ne 0 ]; then
        log WARNING "Script exited with error code: ${exit_code}"
        log WARNING "You may need to clean up partial deployments"
        log WARNING "To stop services: cd ${DOCKER_COMPOSE_DIR} && docker compose down"
    fi

    exit ${exit_code}
}

# Validate host format
validate_host() {
    local host="$1"

    # Basic validation for hostname/domain
    if [[ ! "$host" =~ ^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$ ]]; then
        log ERROR "Invalid hostname format: ${host}"
        log ERROR "Hostname must be a valid domain name (e.g., validator.example.com)"
        return 1
    fi

    return 0
}

# -----------------------------------------------------------------------------
# Main Script Logic
# -----------------------------------------------------------------------------

main() {
    # Parse command line arguments
    local host=""
    local use_remote_image=0
    local skip_genesis=0
    local force_genesis=0
    local dry_run=0
    local verbose=0

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --help|-h)
                usage
                exit 0
                ;;
            --remote-image)
                use_remote_image=1
                shift
                ;;
            --skip-genesis)
                skip_genesis=1
                shift
                ;;
            --force-genesis)
                force_genesis=1
                shift
                ;;
            --dry-run)
                dry_run=1
                DRY_RUN=1
                shift
                ;;
            --verbose|-v)
                verbose=1
                DEBUG=1
                shift
                ;;
            -*)
                log ERROR "Unknown option: $1"
                usage
                exit 1
                ;;
            *)
                if [ -z "${host}" ]; then
                    host="$1"
                else
                    log ERROR "Unexpected argument: $1"
                    usage
                    exit 1
                fi
                shift
                ;;
        esac
    done

    # Validate required arguments
    if [ -z "${host}" ]; then
        log ERROR "Host argument is required"
        usage
        exit 1
    fi

    # Validate host format
    if ! validate_host "${host}"; then
        exit 1
    fi

    # Check for conflicting options
    if [[ "${skip_genesis}" == "1" ]] && [[ "${force_genesis}" == "1" ]]; then
        log ERROR "Cannot use --skip-genesis and --force-genesis together"
        exit 1
    fi

    # Set up error handling
    trap cleanup EXIT

    # Change to repository root
    cd "${REPO_ROOT}"

    # Display configuration
    log INFO "=== Linera Validator Deployment ==="
    log INFO "Host: ${host}"
    log INFO "Repository: ${REPO_ROOT}"

    if [[ ${dry_run} -eq 1 ]]; then
        log WARNING "Running in DRY RUN mode - no changes will be made"
    fi

    # Verify dependencies
    verify_dependencies

    # Get Git information
    IFS='|' read -r branch_name formatted_branch git_commit <<< "$(get_git_info)"

    log INFO "Git branch: ${branch_name} (formatted: ${formatted_branch})"
    log INFO "Git commit: ${git_commit}"

    # Set environment variables
    export DOMAIN="${host}"
    export ACME_EMAIL="${ACME_EMAIL:-$DEFAULT_ACME_EMAIL}"

    # Configure ports and shards
    local port="${PORT:-$DEFAULT_PORT}"
    local metrics_port="${METRICS_PORT:-$DEFAULT_METRICS_PORT}"
    local num_shards="${NUM_SHARDS:-$DEFAULT_NUM_SHARDS}"

    log INFO "Configuration:"
    log INFO "  - ACME Email: ${ACME_EMAIL}"
    log INFO "  - Port: ${port}"
    log INFO "  - Metrics Port: ${metrics_port}"
    log INFO "  - Number of Shards: ${num_shards}"

    # Determine Docker image to use
    if [ ${use_remote_image} -eq 1 ]; then
        export LINERA_IMAGE="${LINERA_IMAGE:-${DEFAULT_DOCKER_REGISTRY}/linera:${branch_name}}"
        log INFO "Using remote Docker image: ${LINERA_IMAGE}"
    else
        export LINERA_IMAGE="${LINERA_IMAGE:-linera}"
        if ! build_local_image "${git_commit}" "${LINERA_IMAGE}"; then
            log ERROR "Failed to build local Docker image"
            exit 1
        fi
    fi

    # Generate genesis URL if not provided
    local genesis_url="${GENESIS_URL:-https://storage.googleapis.com/linera-io-dev-public/${formatted_branch}/genesis.json}"

    # Generate validator configuration
    if ! generate_validator_config "${host}" "${port}" "${metrics_port}" "${num_shards}"; then
        log ERROR "Failed to generate validator configuration"
        exit 1
    fi

    # Download genesis configuration
    if ! download_genesis_config "${genesis_url}" "${skip_genesis}" "${force_genesis}"; then
        log ERROR "Failed to handle genesis configuration"
        if [[ "${skip_genesis}" != "1" ]]; then
            log ERROR "You can retry with --skip-genesis to continue without genesis"
        fi
        exit 1
    fi

    # Generate validator keys
    local public_key
    if ! public_key=$(generate_validator_keys "${LINERA_IMAGE}"); then
        log ERROR "Failed to generate validator keys"
        exit 1
    fi

    log INFO "Validator setup completed successfully"

    # Start services
    if ! start_services; then
        log ERROR "Failed to start services"
        exit 1
    fi

    # Display final information
    echo ""
    log INFO "=== Deployment Complete ==="
    log INFO "Public Key: ${public_key}"
    log INFO "Validator URL: https://${host}"
    echo ""
    log INFO "Useful commands:"
    log INFO "  Check service status:"
    log INFO "    cd ${DOCKER_COMPOSE_DIR} && docker compose ps"
    log INFO ""
    log INFO "  View logs:"
    log INFO "    cd ${DOCKER_COMPOSE_DIR} && docker compose logs -f"
    log INFO ""
    log INFO "  Stop services:"
    log INFO "    cd ${DOCKER_COMPOSE_DIR} && docker compose down"
    log INFO ""
    log INFO "  Restart services:"
    log INFO "    cd ${DOCKER_COMPOSE_DIR} && docker compose restart"

    # Save deployment info
    local deployment_info="${REPO_ROOT}/${DOCKER_COMPOSE_DIR}/.deployment-info"
    cat > "${deployment_info}" <<EOF
# Deployment Information
# Generated: $(date -Iseconds)
HOST=${host}
PUBLIC_KEY=${public_key}
BRANCH=${branch_name}
COMMIT=${git_commit}
IMAGE=${LINERA_IMAGE}
SHARDS=${num_shards}
EOF
    log DEBUG "Deployment info saved to: ${deployment_info}"
}

# Run main function with all arguments
main "$@"
