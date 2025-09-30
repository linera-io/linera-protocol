#!/bin/bash
# Copyright (c) Zefchain Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SERVICE_URL="${1:-}"
PROCESS_PID="${2:-}"
LOG_FILE="${3:-}"

if [ -z "$SERVICE_URL" ] || [ -z "$PROCESS_PID" ] || [ -z "$LOG_FILE" ]; then
    echo "Usage: $0 <service-url> <process-pid> <log-file>" >&2
    echo "Example: $0 http://localhost:8079 12345 /tmp/output.log" >&2
    exit 1
fi

echo "Waiting for service at $SERVICE_URL to be ready..."
echo "Monitoring process PID: $PROCESS_PID"
echo "Streaming logs from: $LOG_FILE"
echo ""

tail -f "$LOG_FILE" 2>/dev/null &
TAIL_PID=$!
trap "kill $TAIL_PID 2>/dev/null || true" EXIT

while true; do
    if ! kill -0 "$PROCESS_PID" 2>/dev/null; then
        wait "$PROCESS_PID" 2>/dev/null || EXIT_CODE=$?
        echo ""
        echo "ERROR: Network creation process (PID: $PROCESS_PID) terminated unexpectedly with exit code: ${EXIT_CODE:-unknown}" >&2
        echo "Check the logs above for errors." >&2
        exit 1
    fi

    if curl -sf "$SERVICE_URL" >/dev/null 2>&1; then
        echo ""
        echo "SUCCESS: Service at $SERVICE_URL is ready!"
        exit 0
    fi

    sleep 1
done
