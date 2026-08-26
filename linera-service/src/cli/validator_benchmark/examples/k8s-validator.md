# Validator Benchmark Report

- **Candidate:** `grpcs:k8s-validator-1.infra.linera.net:443`
- **Public key:** `02bf16cbc02a2ac4c04f07d62c156764d8227b5ae7f9f72811db5ef9a10e4114f1`
- **Version:** VersionInfo { crate_version: Pretty { value: CrateVersion { major: 0, minor: 15, patch: 17 }, _phantom: PhantomData<fn(semver::Version) -> semver::Version> }, git_commit: "ec344b2e3abfeff9606bc9202567636827e41883", git_dirty: false, rpc_hash: "Dkm4Oqv4jN/LcTL1GBBH1eB7TWhYZWTxz/jWgUAN/vY", graphql_hash: "qLS1+EhLpmWSmZBCegXBpE4Wfk6NPHWkSg6hNlJGn5o", wit_hash: "LBDK3ZORkwFJX1WI+P0neR3xZNtvhufYjvfROgBR3QM" }
- **Observer:** OVH US-EAST @  · Started 2026-05-23T02:58:20.492457166+00:00 · Duration 493s · Complete: yes
- **Chains:** 192907fcb85eec2b071f30a097c9152bd6a108486592ab52b53a80e01eaab304
- **RPC timeout:** 30s

## Summary

| Metric | Value |
|---|---|
| Preflight | ✓ OK (rtt p50 95ms, p99 107ms) |
| Read latency (p50/p95/p99) | 98 / 114 / 124 ms |
| Peak sustained throughput | 609 req/s @ conc 64 |
| Bulk download | 8.0 MB/s · 5558 certs/s |
| Tip lag (last) | 433740 blocks · converging |
| Partial sync | — (not run) |
| Total errors | 0 |

## L1 Preflight

Status: ✓ OK · RTT ms: min 95 / p50 95 / p95 107 / p99 107 / max 107

## L2 Read baseline

| chain | count | min | p50 | p95 | p99 | max | errors |
|---|---|---|---|---|---|---|---|
| 192907fc… | 200 | 95 | 98 | 114 | 124 | 137 | 0 |

## L3 Read stress

| chain | conc | req/s | p50 | p95 | p99 | errors |
|---|---|---|---|---|---|---|
| 192907fc… | 1 | 10 | 97 | 112 | 138 | 0 |
| 192907fc… | 2 | 20 | 97 | 119 | 149 | 0 |
| 192907fc… | 4 | 40 | 97 | 116 | 135 | 0 |
| 192907fc… | 8 | 79 | 99 | 119 | 134 | 0 |
| 192907fc… | 16 | 158 | 99 | 117 | 135 | 0 |
| 192907fc… | 32 | 312 | 100 | 120 | 134 | 0 |
| 192907fc… | 64 | 609 | 100 | 131 | 159 | 0 |

## L4 Bulk download

| chain | conc | MB/s | certs/s | p95 ms | errors |
|---|---|---|---|---|---|
| 192907fc… | 1 | 0.8 | 584 | 504 | 0 |
| 192907fc… | 8 | 8.0 | 5558 | 207 | 0 |

## L5 Tip-lag 192907fc… (trend: converging)

| t(s) | candidate | reference | lag |
|---|---|---|---|
| 0 | 735408 | 1169211 | 433803 |
| 121 | 735460 | 1169234 | 433774 |
| 241 | 735525 | 1169265 | 433740 |

