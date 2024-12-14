#!/bin/bash

# Enable error handling
set -e

# Maximum number of attempts and delay between retries
MAX_ATTEMPTS=10
RETRY_DELAY=5

# Environment variables for configuration
STORAGE="scylladb:tcp:scylla:9042"
GENESIS="/config/genesis.json"
LOG_FILE="/var/log/compose-server-init.log"

# Function for logging messages
log_message() {
  local msg="$1"
  echo "$(date +'%Y-%m-%d %H:%M:%S') - $msg" >> "$LOG_FILE"
}

# Function to check if the database exists
check_database_existence() {
  ./linera storage check_existence --storage "$STORAGE"
}

# Function to initialize the database
initialize_database() {
  ./linera-server initialize --storage "$STORAGE" --genesis "$GENESIS"
}

# Main loop with attempt limit
attempt=0
while [ $attempt -lt $MAX_ATTEMPTS ]; do
  attempt=$((attempt + 1))
  log_message "Attempt $attempt of $MAX_ATTEMPTS..."

  # Check if the database exists
  check_database_existence
  status=$?

  if [ $status -eq 0 ]; then
    log_message "Database already exists, no need to initialize."
    echo "Database already exists, no need to initialize."
    exit 0
  elif [ $status -eq 1 ]; then
    log_message "Database does not exist, attempting to initialize..."
    echo "Database does not exist, attempting to initialize..."

    # Try initializing the database
    if initialize_database; then
      log_message "Initialization successful."
      echo "Initialization successful."
      exit 0
    else
      log_message "Initialization failed. Retrying in $RETRY_DELAY seconds..."
      echo "Initialization failed, retrying in $RETRY_DELAY seconds..."
      sleep "$RETRY_DELAY"
    fi
  else
    log_message "Unexpected error occurred (status: $status). Retrying in $RETRY_DELAY seconds..."
    echo "An unexpected error occurred (status: $status), retrying in $RETRY_DELAY seconds..."
    sleep "$RETRY_DELAY"
  fi
done

# If this line is reached, max attempts were exhausted
log_message "Max attempts reached. Database initialization failed."
echo "Max attempts reached. Database initialization failed."
exit 1
