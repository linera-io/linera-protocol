# `linera validator benchmark` — reference

Full reference for the pre-onboarding validator benchmark. For a quick start see
[README.md](README.md); for a real sample run see [examples/](examples/).

## Purpose & model

Before a validator joins the committee we want to know: is it reachable and on the
right build, can it serve reads under load, can it ingest blocks fast enough, and
is it keeping up with the network. This tool answers those by probing a **single
candidate** and emitting a structured report. It does **not**:

- compare against the committee in-process (build baselines by running the same
  tool against known-good validators and diffing the JSON offline);
- emit a PASS/FAIL verdict (the operator interprets the numbers).

### Prerequisite (important for cold candidates)

The read layers (L3–L5) only measure real work if the candidate **already holds
the `--chain` you pass**. A not-yet-committee candidate may hold no blocks, in
which case those layers exercise only the bare request path (still useful for
catching proxy/timeout/networking issues, but not storage under load). Options:

- let the candidate boot and **follow/sync** the network first, or
  `linera validator sync` to push chains into it; then benchmark; or
- pass **`--deep`** — the L2 partial-sync seeds a bounded run of blocks into the
  candidate **before** the read layers, so they exercise real data.

The tool prints a warning when a `--chain` is not held by the candidate.

## Layers (execution order)

| # | Layer | What it measures | Needs data on candidate? | Side effect |
|---|---|---|---|---|
| **L1** | Preflight | Reachability, version/commit, network description, baseline RTT (10 pings) | no | none |
| **L2** | Partial sync (seed) | Write-path ingest throughput; also seeds blocks so the read layers have data. Opt-in `--deep`, runs before the reads | n/a (it provides data) | **stateful**: pushed blocks stay on the candidate |
| **L3** | Read baseline | Latency floor of `handle_chain_info_query` at concurrency 1 | yes | none |
| **L4** | Read stress | Concurrency ramp → sustainable throughput and saturation point | yes | none |
| **L5** | Bulk download | `download_certificates_by_heights` throughput (MB/s, certs/s) | yes | none |
| **L6** | Tip-lag | Candidate tip vs the network (max committee tip), sampled over time → catch-up trend | partial | none (reads committee tips) |

Skip any layer with `--skip-<layer>` (`--skip-preflight`, `--skip-read-baseline`,
`--skip-read-stress`, `--skip-bulk-download`, `--skip-tip-lag`); L2 is off unless
`--deep`. Every RPC is wrapped in a timeout (`--rpc-timeout-secs`) so a hung
validator is recorded as a `timeout` and the run keeps going.

## Output

`--output <SPEC>`, repeatable or `,`/`+`-separated; `SPEC` is `<format>` (stdout)
or `<format>:<path>` (file). Default `md` to stdout.

- **json / yaml** — full structured report (schema below); for baseline diffing.
- **md** — human report: a Summary recap + a detail table per layer.
- **brief** — a few headline lines.

Files are rewritten after **every** layer, and `metadata.complete` is `false`
until the final write — so an interrupted run leaves a valid file with the layers
done so far. stdout targets are written once at the end.

## Report data

| Section / field | Description |
|---|---|
| `metadata.tool_version` | Version of the `linera` binary that produced the report |
| `metadata.complete` | `false` while running (flushed per layer), `true` only on the final write — tells a partial run from a complete one |
| `metadata.chains_tested` | The `--chain` list |
| `metadata.config` | Echo of all effective flags (reproducibility) |
| `candidate.address` | Candidate address (`grpcs:host:port`) |
| `candidate.public_key` | Expected public key (if `--public-key` was given) |
| `candidate.version_info` | Candidate build: crate version, git commit, API hashes (text) |
| `candidate.network_description` | Network the candidate reports (admin chain, genesis…) |
| `observer.location` | Tag for the observing VM (e.g. `OVH US-EAST`); latencies are relative to it |
| `observer.hostname / started_at / ended_at / duration_secs` | Host, start/end (RFC 3339), total duration |
| *Latency stats* (reused in L1, L3–L5) | Per bucket: `count`, `min`, `max`, `mean`, `stddev`, `p50`, `p95`, `p99`, and `errors` (per category: `timeout` / `unavailable` / `other`) |
| `layers.preflight.status` | `ok` / `fail` |
| `layers.preflight.rtt_ms` | RTT distribution from 10 lightweight pings |
| `layers.preflight.version_match / network_description_match` | Match against the local client view (if evaluated) |
| `layers.partial_sync` (L2, `--deep`) | `from_height`/`to_height`, `blocks_attempted`/`blocks_accepted`, `bytes_in`, `duration_secs`, `blocks_per_sec` |
| `layers.read_baseline.per_chain[].latency_ms` | L3 sequential read latency, per chain |
| `layers.read_stress.per_chain[].levels[]` | L4 per concurrency level: `concurrency`, `duration_secs`, `completed`, `throughput_per_sec`, `latency_ms` |
| `layers.bulk_download.per_chain[].runs[]` | L5 per concurrency: `batch_size`, `heights_range`, `bytes_in`, `certs_received`, `duration_secs`, `mb_per_sec`, `certs_per_sec`, `latency_ms` |
| `layers.tip_lag.per_chain[].samples[]` | L6 per sample: `t_secs`, `candidate_tip`, `reference_tip`, `lag_blocks` |
| `layers.tip_lag.per_chain[].trend` | `converging` / `stable` / `diverging` |
| *Summary* (md/brief only, derived) | Preflight + rtt; seed blocks/s; read p50/p95/p99; peak throughput @ concurrency; bulk MB/s + certs/s; tip-lag + trend; total errors |

## Options

The canonical, always-current flag list is generated from the clap definitions
into the repo's top-level **`CLI.md`** (`linera validator help-markdown`). Summary:

| Option | Default | Description |
|---|---|---|
| `<ADDRESS>` | — (required) | Candidate address, `grpcs:host:port` |
| `--public-key <PK>` | — | Expected public key (identity check) |
| `--chain <ID>` | — (≥1, repeatable) | Chain(s) to exercise; candidate should hold them (or use `--deep`) |
| `--deep` | off | L2 partial sync (seed); pushes blocks before the read layers; **stateful** |
| `--deep-blocks <N>` | 1000 | Blocks to seed in L2 |
| `--deep-chain <ID>` | first `--chain` | Chain used for L2 |
| `--skip-preflight` | off | Skip L1 |
| `--skip-read-baseline` | off | Skip L3 |
| `--skip-read-stress` | off | Skip L4 |
| `--skip-bulk-download` | off | Skip L5 |
| `--skip-tip-lag` | off | Skip L6 |
| `--baseline-requests <N>` | 200 | Sequential requests per chain (L3) |
| `--stress-levels <list>` | 1,2,4,8,16,32,64 | Concurrency ramp (L4) |
| `--stress-duration-secs <N>` | 30 | Seconds per L4 level |
| `--bulk-batch-size <N>` | 100 | Heights per L5 batch |
| `--bulk-concurrency <list>` | 1,8 | Concurrency levels (L5) |
| `--bulk-height-range <auto\|FROM:TO>` | auto | L5 range; `auto` = last `batch_size×100` heights up to the candidate tip |
| `--tip-lag-samples <N>` | 3 | Samples (L6) |
| `--tip-lag-interval-secs <N>` | 120 | Seconds between L6 samples (shown as a countdown) |
| `--rpc-timeout-secs <N>` | 30 | Per-RPC timeout; a slow call is recorded as `timeout` and the run continues |
| `--abort-on-preflight-fail` | off | Stop if L1 fails (default: continue and report) |
| `--observer-location <str>` | unspecified | Observer tag in the report |
| `--no-progress` | off | Disable the progress UI (auto-off when stderr is not a TTY) |
| `--output <SPEC>` | `md` to stdout | Repeatable / `,`/`+`-separated; `<format>` (stdout) or `<format>:<path>` (file); formats `json`, `yaml`, `md`, `brief` |

## Baseline workflow

The tool stores no baselines itself. Typical flow:

1. Once per network, run it against each current committee validator, saving JSON
   per validator (`--output json:baselines/<name>.json`).
2. Run it against the candidate, dumping JSON next to the baselines plus `brief`
   to stdout for quick triage.
3. Compare offline (`jq` / `diff` / a script). The operator decides.

## Notes

- Address format is `grpcs:host:port` (single colons), not a `://` URL.
- The committee membership for L6's reference tip is read from the wallet's local
  genesis config (validators serve committee info only to local clients), so no
  particular chain needs to be tracked for L6.
- Runs from a fixed observer VM; all latencies carry that geography
  (`observer.location`). Run from multiple VMs and diff the JSON for a wider view.
