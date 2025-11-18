#!/bin/sh
set -e

echo "Starting Linera Explorer..."

# Log which database type is being used
if [ -n "$DATABASE_URL" ]; then
    echo "Using PostgreSQL database"
else
    echo "Using SQLite database at ${DB_PATH:-/data/indexer.db}"
fi

echo "Starting services..."

# Start the API server in the background
echo "Starting API server on port ${EXPLORER_API_PORT:-3002}..."
cd /app && node server/index.js &
API_PID=$!

# Wait a moment for API to start
sleep 3

# Start frontend server (serve static files)
echo "Starting frontend server on port ${EXPLORER_FRONTEND_PORT:-3001}..."
npx serve -s dist -l ${EXPLORER_FRONTEND_PORT:-3001} &
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