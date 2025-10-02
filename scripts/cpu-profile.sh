#!/bin/bash

# CPU profiling wrapper script for easy flamegraph generation
# Usage: cpu-profile.sh <PID> [duration_seconds]

if [ $# -lt 1 ]; then
    echo "Usage: cpu-profile.sh <PID> [duration_seconds]"
    echo "Example: cpu-profile.sh 1234 30"
    exit 1
fi

PID=$1
DURATION=${2:-30}
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
PERF_DATA="/tmp/cpu-profile-${PID}-${TIMESTAMP}.perf.data"
FOLDED_FILE="/tmp/cpu-profile-${PID}-${TIMESTAMP}.folded"
SVG_FILE="/tmp/cpu-profile-${PID}-${TIMESTAMP}.svg"

echo "Profiling PID $PID for $DURATION seconds..."
echo "NOTE: The process must be actively using CPU for samples to be collected."
echo "      If profiling an idle server, generate load during this time."
echo ""

# Profile using perf (works without kernel headers)
perf record -F 99 -p $PID -g -o $PERF_DATA -- sleep $DURATION

if [ $? -ne 0 ]; then
    echo "ERROR: perf record failed. The process may have exited or perf may not have permissions."
    exit 1
fi

echo "Perf data: $PERF_DATA"

echo "Converting to folded format..."
# Convert perf data to folded format
perf script -i $PERF_DATA | stackcollapse-perf > $FOLDED_FILE

if [ ! -s "$FOLDED_FILE" ]; then
    echo "WARNING: Folded file is empty. This likely means:"
    echo "  - The process was idle (not using CPU) during profiling"
    echo "  - No samples were collected"
    echo ""
    echo "For an idle server, generate some load and try again."
    exit 1
fi

echo "Folded output: $FOLDED_FILE"

echo "Generating flamegraph..."
# Generate flamegraph
flamegraph $FOLDED_FILE > $SVG_FILE
echo "SVG output: $SVG_FILE"

echo ""
echo "CPU profiling complete:"
echo "  Perf data:   $PERF_DATA"
echo "  Folded data: $FOLDED_FILE"
echo "  Flamegraph:  $SVG_FILE"
