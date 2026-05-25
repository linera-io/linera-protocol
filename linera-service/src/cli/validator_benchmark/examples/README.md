# `linera validator benchmark` — example output

These files are a real run of `linera validator benchmark` against a testnet
validator, captured in each output format. They illustrate what the tool
produces; the numbers are a point-in-time sample, not a target.

## Prerequisite

The read layers (L3 baseline, L4 stress, L5 bulk) are only meaningful if the
candidate already holds the `--chain` you pass. A not-yet-committee candidate may
hold no blocks, in which case those layers only exercise the bare request path.
Either let the candidate sync the chains first (`linera validator sync`, or boot
it and let it follow the network) or pass `--deep` (L2 partial sync), which seeds
a bounded run of blocks **before** the read layers. The tool warns when a chain
is not held.

The layers, in execution order: **L1** preflight, **L2** partial sync (seed,
opt-in `--deep`), **L3** read baseline, **L4** read stress, **L5** bulk download,
**L6** tip-lag.

## Command

```bash
linera validator benchmark grpcs:k8s-validator-1.infra.linera.net:443 \
  --public-key 02bf16cbc02a2ac4c04f07d62c156764d8227b5ae7f9f72811db5ef9a10e4114f1 \
  --chain 192907fcb85eec2b071f30a097c9152bd6a108486592ab52b53a80e01eaab304 \
  --observer-location "OVH US-EAST" \
  --output md \
  --output md:k8s-validator.md \
  --output json:k8s-validator.json \
  --output yaml:k8s-validator.yaml
```

`--output md` (no path) prints the Markdown report to stdout; the `<format>:<path>`
forms also write each format to a file. `brief` is also available for a short
headline recap.

## Files

- [`k8s-validator.md`](k8s-validator.md) — human-readable report (summary recap + per-layer tables)
- [`k8s-validator.json`](k8s-validator.json) — structured report (for baseline diffing)
- [`k8s-validator.yaml`](k8s-validator.yaml) — same structure, YAML
