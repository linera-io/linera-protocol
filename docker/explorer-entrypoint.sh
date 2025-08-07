#!/bin/sh
set -e

echo "Starting Linera Explorer..."

# Wait for database file to exist (created by indexer)
echo "Waiting for indexer database..."
while [ ! -f "${DB_PATH:-/data/indexer.db}" ]; do
    echo "Database not found at ${DB_PATH:-/data/indexer.db}, waiting..."
    sleep 2
done

echo "Database found, starting services..."

# Start the API server in the background
echo "Starting API server on port 3002..."
cd /app && node server/index.js &
API_PID=$!

# Wait a moment for API to start
sleep 3

# Start frontend server (serve static files)
echo "Starting frontend server on port 3001..."
npx serve -s dist -l 3001 &
FRONTEND_PID=$!

# Function to handle shutdown
shutdown() {
    echo "Shutting down services..."
    kill $API_PID $FRONTEND_PID 2>/dev/null || true
    wait $API_PID $FRONTEND_PID 2>/dev/null || true
    exit 0
}

# Handle signals
trap shutdown SIGTERM SIGINT

# Wait for either process to exit
wait $API_PID $FRONTEND_PID