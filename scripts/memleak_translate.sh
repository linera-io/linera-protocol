#!/usr/bin/env bash

# memleak_translate.sh <PID>
#
# IMPORTANT: This script is for FILE PROCESSING ONLY - NOT for real-time translation.
# It reads the ENTIRE input into memory first, then translates all addresses
# in a single batch for maximum performance.
#
# Usage:
#   1. Save memleak output to a file: memleak -p <PID> > memleak_output.txt
#   2. Translate the file: cat memleak_output.txt | ./memleak_translate.sh <PID>
#
# Works with the default `memleak-libbpf`/`memleak` output that looks like:
#   0 [<0000aaaaeac54784>] [/linera-server]
#
# Requires llvm-symbolizer to be installed. Makes exactly ONE call to
# llvm-symbolizer for ALL unique addresses.

set -euo pipefail

[[ $# -eq 1 ]] || { echo "usage: $0 <pid>" >&2; exit 1; }

# Check that llvm-symbolizer is available
if ! command -v llvm-symbolizer &> /dev/null; then
    echo "Error: llvm-symbolizer not found. Please install it first." >&2
    echo "  Ubuntu/Debian: apt-get install llvm" >&2
    echo "  macOS: brew install llvm" >&2
    exit 1
fi

PID=$1
exe="/proc/$PID/exe"

BASE=$(awk -v bin="$(readlink -f /proc/$PID/exe)" '$NF==bin { split($1,a,"-"); print "0x"a[1]; exit }' /proc/$PID/maps)

[[ -n $BASE ]] || { echo "could not find base address for $exe" >&2; exit 1; }

# Read entire input into array
mapfile -t LINES

# Extract all unique addresses
declare -A UNIQUE_ADDRS
declare -a ADDR_ORDER

for line in "${LINES[@]}"; do
    if [[ $line =~ \[\<([0-9a-fA-F]+)\>\] ]]; then
        addr="${BASH_REMATCH[1]}"
        [[ $addr != 0x* ]] && addr="0x${addr}"
        if [[ -z "${UNIQUE_ADDRS[$addr]:-}" ]]; then
            UNIQUE_ADDRS[$addr]=1
            ADDR_ORDER+=("$addr")
        fi
    fi
done

# If no addresses found, just output the original lines
if [[ ${#ADDR_ORDER[@]} -eq 0 ]]; then
    printf "%s\n" "${LINES[@]}"
    exit 0
fi

# Convert all addresses to relative offsets
declare -a REL_ADDRS
for addr in "${ADDR_ORDER[@]}"; do
    rel=$(printf "0x%x" $(( addr - BASE )))
    REL_ADDRS+=("$rel")
done

# Translate ALL addresses in ONE llvm-symbolizer call
declare -A TRANSLATIONS

# Run llvm-symbolizer
if ! SYMBOLS=$(printf "%s\n" "${REL_ADDRS[@]}" | llvm-symbolizer -e "$exe" --demangle --functions=linkage --inlining=false 2>&1); then
    echo "Error: llvm-symbolizer failed to run" >&2
    echo "$SYMBOLS" >&2
    exit 1
fi

# Parse output - llvm-symbolizer outputs exactly 3 lines per address:
# Line 1: function name, Line 2: file path and line, Line 3: empty line
readarray -t SYMBOL_LINES <<< "$SYMBOLS"

for ((i=0; i<${#ADDR_ORDER[@]}; i++)); do
    addr="${ADDR_ORDER[$i]}"
    line_base=$((i * 3))

    if [[ $line_base -lt ${#SYMBOL_LINES[@]} ]]; then
        func_line="${SYMBOL_LINES[$line_base]}"
    else
        func_line=""
    fi

    if [[ $((line_base + 1)) -lt ${#SYMBOL_LINES[@]} ]]; then
        loc_line="${SYMBOL_LINES[$((line_base + 1))]}"
    else
        loc_line=""
    fi

    translation="${func_line} ${loc_line}"
    translation=$(echo "$translation" | sed 's/  */ /g' | sed 's/ $//')

    if [[ -z "$translation" || "$translation" =~ ^"?? " ]]; then
        translation="$addr"
    fi

    TRANSLATIONS[$addr]="$translation"
done

# Ensure all addresses have translations
for addr in "${ADDR_ORDER[@]}"; do
    if [[ -z "${TRANSLATIONS[$addr]:-}" ]]; then
        TRANSLATIONS[$addr]="$addr"
    fi
done

# Output all lines with translations applied
for line in "${LINES[@]}"; do
    if [[ $line =~ ([0-9]+)\ \[\<([0-9a-fA-F]+)\>\]\ \[([^]]+)\] ]]; then
        stack_pos="${BASH_REMATCH[1]}"
        addr="${BASH_REMATCH[2]}"
        origin="${BASH_REMATCH[3]}"
        [[ $addr != 0x* ]] && addr="0x${addr}"
        echo "        $stack_pos ${TRANSLATIONS[$addr]} [$origin]"
    else
        echo "$line"
    fi
done
