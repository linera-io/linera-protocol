#!/usr/bin/env bash

# memleak_translate.sh <PID>
#
# Works with the default `memleak-libbpf`/`memleak` output that looks like
#   0 [<0000aaaaeac54784>] [/linera-server]
# and replaces the address line with the addr2line translated result, which should be the function
# name, file name and line number.
#
# add2line takes a while to execute, so it is recommended to run `memleak` for a while, while
# redirecting the output to a file. Then inspect the file for what you want (maybe only the last
# few lines of the output), and then run this script while the binary is still running, from within
# the same container.

set -euo pipefail

[[ $# -eq 1 ]] || { echo "usage: $0 <pid>" >&2; exit 1; }

PID=$1
exe="/proc/$PID/exe"

BASE=$(awk -v bin="$(readlink -f /proc/$PID/exe)" '$NF==bin { split($1,a,"-"); print "0x"a[1]; exit }' /proc/$PID/maps)

[[ -n $BASE ]] || { echo "could not find base address for $exe" >&2; exit 1; }

# helper: translate <$addr> using addr2line, compensating for PIE base
translate () {
    local addr=$1
    addr=${addr//[<>[\]]}
    [[ $addr != 0x* ]] && addr="0x${addr}"
    local rel=$(printf "0x%x" $(( addr - BASE )))
    addr2line -e "$exe" -f -C -p "$rel" 2>/dev/null || echo "$addr"
}

while IFS= read -r line; do
    if [[ $line =~ ([0-9]+)\ \[\<([0-9a-fA-F]+)\>\]\ \[([^]]+)\] ]]; then
        stack_pos="${BASH_REMATCH[1]}"
        addr="${BASH_REMATCH[2]}"
        origin="${BASH_REMATCH[3]}"
        echo "        $stack_pos $(translate "$addr") [$origin]"
    else
        echo "$line"
    fi
done
