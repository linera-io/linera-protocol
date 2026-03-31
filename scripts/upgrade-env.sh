#!/usr/bin/env bash
#
# Upgrade .env file with new configuration variables
#
# Purpose: Safely merge new variables from the template into an existing .env
#          without overwriting existing values or touching any data (volumes,
#          wallet.json, server.json, etc.)
#
# Usage: ./upgrade-env.sh [--dry-run]
#
# Run from the repository root or the docker/ directory.
# Creates a timestamped backup before making any changes.

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly DOCKER_DIR="${REPO_ROOT}/docker"
readonly ENV_FILE="${DOCKER_DIR}/.env"
readonly TEMPLATE_FILE="${DOCKER_DIR}/.env.production.template"

readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly NC='\033[0m'

DRY_RUN=0

log() {
    local level="$1"; shift
    case "$level" in
        ERROR)   echo -e "${RED}[ERROR]${NC} $*" >&2 ;;
        WARNING) echo -e "${YELLOW}[WARNING]${NC} $*" ;;
        INFO)    echo -e "${GREEN}[INFO]${NC} $*" ;;
    esac
}

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Safely upgrade an existing docker/.env with new configuration variables.
Preserves all existing values. Creates a backup before changes.

OPTIONS:
    --dry-run    Show what would change without modifying files
    --help, -h   Show this help message

WHAT IT DOES:
    1. Reads your existing docker/.env
    2. Reads the template (docker/.env.production.template)
    3. Rebuilds .env using the template structure:
       - Existing values are preserved exactly as-is
       - New variables are added (commented out) for you to customize
    4. Creates a timestamped backup of the original .env

WHAT IT DOES NOT TOUCH:
    - Docker volumes (ScyllaDB data, Caddy certificates, etc.)
    - wallet.json, server.json, genesis.json, committee.json
    - Running containers (you decide when to restart)
EOF
}

# Extract KEY=VALUE pairs from a file (skipping comments and blank lines)
parse_env_vars() {
    local file="$1"
    grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "$file" 2>/dev/null || true
}

# Check if a variable name exists (uncommented) in the existing .env
var_exists_in_env() {
    local var_name="$1"
    grep -qE "^${var_name}=" "${ENV_FILE}" 2>/dev/null
}

# Get the value of a variable from the existing .env
get_env_value() {
    local var_name="$1"
    grep -E "^${var_name}=" "${ENV_FILE}" | head -1
}

main() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --dry-run) DRY_RUN=1; shift ;;
            --help|-h) usage; exit 0 ;;
            *) log ERROR "Unknown option: $1"; usage; exit 1 ;;
        esac
    done

    if [[ ! -f "${ENV_FILE}" ]]; then
        log ERROR "No existing .env found at: ${ENV_FILE}"
        log ERROR "This script upgrades an existing .env file."
        log ERROR "If this is a fresh deployment, use deploy-validator.sh instead."
        exit 1
    fi

    if [[ ! -f "${TEMPLATE_FILE}" ]]; then
        log ERROR "Template not found at: ${TEMPLATE_FILE}"
        log ERROR "Make sure you're running from an up-to-date repository."
        exit 1
    fi

    log INFO "=== Linera .env Upgrade ==="
    log INFO "Existing .env: ${ENV_FILE}"
    log INFO "Template:      ${TEMPLATE_FILE}"

    if [[ ${DRY_RUN} -eq 1 ]]; then
        log WARNING "DRY RUN mode — no files will be modified"
    fi

    # Collect existing variables (split on first '=' only to preserve '=' in values)
    local -A existing_vars
    while IFS= read -r line; do
        local key="${line%%=*}"
        local value="${line#*=}"
        existing_vars["${key}"]="${value}"
    done < <(parse_env_vars "${ENV_FILE}")

    log INFO "Found ${#existing_vars[@]} existing variables in .env"

    # Build the new .env by processing the template line by line
    local new_env=""
    local added=0
    local preserved=0

    while IFS= read -r line || [[ -n "$line" ]]; do
        # Blank line or pure comment (no variable) — pass through
        if [[ -z "$line" ]] || [[ "$line" =~ ^#[^A-Za-z]*$ ]] || [[ "$line" =~ ^#\ [^A-Z] ]] || [[ "$line" =~ ^#$ ]]; then
            new_env+="${line}"$'\n'
            continue
        fi

        # Commented-out variable: #VAR_NAME=value
        if [[ "$line" =~ ^#([A-Za-z_][A-Za-z0-9_]*)= ]]; then
            local var_name="${BASH_REMATCH[1]}"
            if [[ -v "existing_vars[${var_name}]" ]]; then
                # Variable exists uncommented in current .env — keep the active value
                new_env+="${var_name}=${existing_vars[${var_name}]}"$'\n'
                preserved=$((preserved + 1))
            else
                # New variable, keep as comment from template
                new_env+="${line}"$'\n'
                added=$((added + 1))
            fi
            continue
        fi

        # Active variable: VAR_NAME=value
        if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)= ]]; then
            local var_name="${BASH_REMATCH[1]}"
            if [[ -v "existing_vars[${var_name}]" ]]; then
                # Preserve existing value
                new_env+="${var_name}=${existing_vars[${var_name}]}"$'\n'
                preserved=$((preserved + 1))
            else
                # New required variable from template — add as comment so user reviews
                new_env+="#${line}"$'\n'
                added=$((added + 1))
            fi
            continue
        fi

        # Anything else (comment lines with variable-like content, section headers)
        new_env+="${line}"$'\n'
    done < "${TEMPLATE_FILE}"

    # Append any existing variables NOT in the template (custom user vars)
    local custom=0
    for key in "${!existing_vars[@]}"; do
        if ! grep -qE "^#?${key}=" "${TEMPLATE_FILE}" 2>/dev/null; then
            if [[ ${custom} -eq 0 ]]; then
                new_env+=$'\n'"# Existing variables not in template (preserved from your .env)"$'\n'
            fi
            new_env+="${key}=${existing_vars[${key}]}"$'\n'
            custom=$((custom + 1))
        fi
    done

    # Summary
    log INFO "--- Summary ---"
    log INFO "  Preserved existing values: ${preserved}"
    log INFO "  New variables added (commented): ${added}"
    if [[ ${custom} -gt 0 ]]; then
        log INFO "  Custom variables kept: ${custom}"
    fi

    if [[ ${DRY_RUN} -eq 1 ]]; then
        log INFO "--- New .env would be: ---"
        echo "${new_env}"
        log INFO "--- End of dry run ---"
        exit 0
    fi

    # Create backup
    local backup="${ENV_FILE}.backup.$(date +%Y%m%d-%H%M%S)"
    cp "${ENV_FILE}" "${backup}"
    log INFO "Backup created: ${backup}"

    # Write new .env
    printf '%s' "${new_env}" > "${ENV_FILE}"
    log INFO "Updated: ${ENV_FILE}"

    echo ""
    log INFO "=== Upgrade Complete ==="
    log INFO "Review the new variables in ${ENV_FILE}"
    log INFO "Uncomment and adjust any values you want to customize."
    log INFO "Then restart services: cd docker && docker compose up -d"
    log WARNING "No data was modified — volumes, keys, and configs are untouched."
}

main "$@"
