#!/bin/bash

# Check if wallet and storage arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <wallet> <storage>"
    exit 1
fi

# Assign arguments to variables
wallet=$1
storage=$2

# Initialize sum variables
sum_block_proposals=0
sum_certificates=0

# Number of iterations
iterations=10

# Run the binary 10 times and collect the output
for ((i=1; i<=iterations; i++))
do
    output=$(linera --wallet $wallet --storage $storage benchmark 2>&1)

    # Extract block proposals per second
    block_proposals=$(echo "$output" | grep "block proposals per sec" | awk '{print $7}')

    # Extract certificates per second
    certificates=$(echo "$output" | grep "certificates per sec" | awk '{print $7}')

    # Add to the sum
    sum_block_proposals=$(echo "$sum_block_proposals + $block_proposals" | bc)
    sum_certificates=$(echo "$sum_certificates + $certificates" | bc)
done

# Calculate the average
avg_block_proposals=$(echo "scale=2; $sum_block_proposals / $iterations" | bc)
avg_certificates=$(echo "scale=2; $sum_certificates / $iterations" | bc)

# Print the average benchmarks
echo "Average block proposals per second: $avg_block_proposals"
echo "Average certificates per second: $avg_certificates"