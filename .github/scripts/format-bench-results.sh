#!/usr/bin/env bash
# Copyright (c) Zefchain Labs, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Format gungraun benchmark JSON summaries as a GitHub PR comment.
#
# Reads the JSON summary files produced by `--save-summary=json` and produces
# a markdown comment with two collapsible tables: deterministic metrics
# (Instructions, Total R+W) and cache-dependent metrics (L1/LL/RAM Hits,
# Estimated Cycles).
#
# Usage: format-bench-results.sh <json-dir> [--baseline-sha SHA]

set -euo pipefail

JSON_DIR="${1:?Usage: format-bench-results.sh <json-dir> [--baseline-sha SHA]}"
BASELINE_SHA=""
shift || true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --baseline-sha) BASELINE_SHA="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Formatting helpers ───────────────────────────────────────────────────────

# Add commas to an integer: 1234567 → 1,234,567
fmt_num() {
  if [[ -z "$1" ]]; then printf "?"; return; fi
  local n
  n=$(printf "%.0f" "$1")
  local result="" len=${#n}
  for ((k = 0; k < len; k++)); do
    if ((k > 0 && (len - k) % 3 == 0)); then result+=","; fi
    result+="${n:k:1}"
  done
  printf "%s" "$result"
}

# Format percentage with LaTeX coloring (1% threshold)
fmt_change() {
  local pct="$1"
  if [[ -z "$pct" ]]; then return; fi
  local formatted
  formatted=$(printf "%+.2f" "$pct")
  if awk "BEGIN { exit !($pct == 0) }"; then
    printf "No change"
  elif awk "BEGIN { exit !($pct > 1) }"; then
    printf '%s' "\${\\color{red}\\textbf{${formatted}\\%}}\$"
  elif awk "BEGIN { exit !($pct < -1) }"; then
    printf '%s' "\${\\color{green}\\textbf{${formatted}\\%}}\$"
  else
    printf "%s%%" "$formatted"
  fi
}

# Format one table cell: " value (change) |" or " value |"
fmt_cell() {
  local fv fc
  fv=$(fmt_num "$1")
  if [[ -n "$2" ]]; then
    fc=$(fmt_change "$2")
    printf " %s (%s) |" "$fv" "$fc"
  else
    printf " %s |" "$fv"
  fi
}

# Map group key → display name
group_label() {
  case "$1" in
    map_view_group)          echo "MapView" ;;
    queue_view_group)        echo "QueueView" ;;
    bucket_queue_view_group) echo "BucketQueueView" ;;
    register_view_group)     echo "RegisterView" ;;
    collection_view_group)   echo "CollectionView" ;;
    cold_load_group)         echo "Cold Load" ;;
    *)                       echo "$1" ;;
  esac
}

# ── Read benchmark data from JSON files ──────────────────────────────────────

# jq extracts 14 lines per file: name, group, then value+pct for each metric.
# Each metric value uses simple path lookups with // fallbacks — no jq functions.
# Callgrind stores values as {"Int": n} or {"Float": n}, and comparison data
# as Both[0] (new) / Both[1] (old) or Left (no baseline).

declare -a B_NAME B_GROUP
declare -a B_IV B_IP B_TV B_TP B_L1V B_L1P B_LLV B_LLP B_RV B_RP B_CV B_CP

i=0
HAS_CMP=false

while IFS= read -r f; do
  {
    read -r name
    read -r group
    read -r iv;  read -r ip
    read -r tv;  read -r tp
    read -r l1v; read -r l1p
    read -r llv; read -r llp
    read -r rv;  read -r rp
    read -r cv;  read -r cp
  } < <(jq -r '
    (.profiles[0].summaries.total.summary.Callgrind // {}) as $c |
    .id,
    (.module_path | split("::") | if length > 2 then .[1] else "other" end),
    (($c.Ir.metrics           | (.Both[0] // .Left) | (.Int // .Float)) // ""),
    ($c.Ir.diffs.diff_pct                                               // ""),
    (($c.TotalRW.metrics      | (.Both[0] // .Left) | (.Int // .Float)) // ""),
    ($c.TotalRW.diffs.diff_pct                                          // ""),
    (($c.L1hits.metrics       | (.Both[0] // .Left) | (.Int // .Float)) // ""),
    ($c.L1hits.diffs.diff_pct                                           // ""),
    (($c.LLhits.metrics       | (.Both[0] // .Left) | (.Int // .Float)) // ""),
    ($c.LLhits.diffs.diff_pct                                           // ""),
    (($c.RamHits.metrics      | (.Both[0] // .Left) | (.Int // .Float)) // ""),
    ($c.RamHits.diffs.diff_pct                                          // ""),
    (($c.EstimatedCycles.metrics | (.Both[0] // .Left) | (.Int // .Float)) // ""),
    ($c.EstimatedCycles.diffs.diff_pct                                     // "")
  ' "$f")

  B_NAME[i]="$name";  B_GROUP[i]="$group"
  B_IV[i]="$iv";      B_IP[i]="$ip"
  B_TV[i]="$tv";      B_TP[i]="$tp"
  B_L1V[i]="$l1v";    B_L1P[i]="$l1p"
  B_LLV[i]="$llv";    B_LLP[i]="$llp"
  B_RV[i]="$rv";      B_RP[i]="$rp"
  B_CV[i]="$cv";      B_CP[i]="$cp"

  for p in "$ip" "$tp" "$l1p" "$llp" "$rp" "$cp"; do
    [[ -n "$p" ]] && HAS_CMP=true
  done
  i=$((i + 1))
done < <(find "$JSON_DIR" -name summary.json | sort)

COUNT=$i

if [[ $COUNT -eq 0 ]]; then
  echo "*No benchmark results found.*"
  exit 0
fi

# Build ordered list of unique groups
declare -a BENCH_GROUPS
for ((j = 0; j < COUNT; j++)); do
  g="${B_GROUP[j]}"
  skip=false
  for ug in "${BENCH_GROUPS[@]+"${BENCH_GROUPS[@]}"}"; do
    [[ "$ug" == "$g" ]] && skip=true && break
  done
  $skip || BENCH_GROUPS+=("$g")
done

# ── Markdown output ──────────────────────────────────────────────────────────

echo "## Instruction Count Benchmark Results"
echo ""

if $HAS_CMP && [[ -n "$BASELINE_SHA" ]]; then
  echo "> Baseline: \`${BASELINE_SHA:0:10}\`"
  echo ""
fi

if ! $HAS_CMP; then
  echo "> No baseline available for comparison. Showing absolute counts only. Please rebase onto \`main\` to get a comparison."
  echo ""
fi

# Table 1: Deterministic metrics
echo "<details>"
echo "<summary>Deterministic metrics — reproducible across runs ($COUNT benchmarks)</summary>"
echo ""
echo "| Benchmark | Instructions | Total R+W |"
echo "|:---|---:|---:|"

for g in "${BENCH_GROUPS[@]}"; do
  echo "| **$(group_label "$g")** | | |"
  for ((j = 0; j < COUNT; j++)); do
    [[ "${B_GROUP[j]}" != "$g" ]] && continue
    row="| \`${B_NAME[j]}\` |"
    row+=$(fmt_cell "${B_IV[j]}" "${B_IP[j]}")
    row+=$(fmt_cell "${B_TV[j]}" "${B_TP[j]}")
    echo "$row"
  done
done

echo ""
echo "</details>"
echo ""
echo '> Regression threshold: **1%** — ${\color{red}\textbf{red}}$ = regression, ${\color{green}\textbf{green}}$ = improvement.'
echo ""

# Table 2: Cache-dependent metrics
echo "<details>"
echo "<summary>Cache-dependent metrics — expect fluctuations between runs ($COUNT benchmarks)</summary>"
echo ""
echo "| Benchmark | L1 Hits | LL Hits | RAM Hits | Est. Cycles |"
echo "|:---|---:|---:|---:|---:|"

for g in "${BENCH_GROUPS[@]}"; do
  echo "| **$(group_label "$g")** | | | | |"
  for ((j = 0; j < COUNT; j++)); do
    [[ "${B_GROUP[j]}" != "$g" ]] && continue
    row="| \`${B_NAME[j]}\` |"
    row+=$(fmt_cell "${B_L1V[j]}" "${B_L1P[j]}")
    row+=$(fmt_cell "${B_LLV[j]}" "${B_LLP[j]}")
    row+=$(fmt_cell "${B_RV[j]}" "${B_RP[j]}")
    row+=$(fmt_cell "${B_CV[j]}" "${B_CP[j]}")
    echo "$row"
  done
done

echo ""
echo "</details>"
echo ""
echo "> Cache metrics fluctuate because anything that changes the virtual memory layout"
echo "> shifts which data lands on which cache lines, changing the L1/LL/RAM distribution."
echo "> Probable causes: ASLR (even across identical binaries), executable binary size changes,"
echo "> shared library size changes, and even filename length differences."
