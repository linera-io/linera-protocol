# `linera validator benchmark`

A pre-onboarding benchmark for a single candidate validator: probe its serving
capacity, network path, and (optionally) block-ingest throughput **before**
adding it to the committee. It targets one validator, emits a structured report,
and gives no verdict — the operator reads the numbers and decides.

## Quick start

```bash
# Candidate that already follows the chain(s) you pass:
linera validator benchmark grpcs:host:443 \
  --chain <CHAIN_ID> \
  --output md

# Cold candidate (no blocks yet): seed first, then the read layers run on real data:
linera validator benchmark grpcs:host:443 \
  --chain <CHAIN_ID> --deep \
  --output "json:report.json,md:report.md,brief"
```

Address format is `grpcs:host:port` (scheme, host, port separated by single
colons — not a `://` URL).

## What it runs

In execution order: **L1** preflight · **L2** partial sync (seed, opt-in
`--deep`) · **L3** read baseline · **L4** read stress · **L5** bulk download ·
**L6** tip-lag.

The read layers (L3–L5) are only meaningful if the candidate already holds the
`--chain`. A not-yet-committee candidate may hold nothing — pass `--deep` to seed
blocks first, or pre-sync it (`linera validator sync`). The tool warns when a
chain is not held.

## Output

`--output <format>[:<path>]`, repeatable or `,`/`+`-separated. Formats: `json`,
`yaml`, `md`, `brief` (default `md` to stdout). Files are rewritten after each
layer, so an interrupt still leaves the completed layers on disk.

## More

- **[REFERENCE.md](REFERENCE.md)** — full reference: layer semantics, report
  schema, and all options.
- **[examples/](examples/)** — a real run captured in every format.
- The authoritative, always-current flag list is generated from the code into the
  repo's top-level `CLI.md` (`linera validator help-markdown`).
