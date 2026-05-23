# `linera validator benchmark` — example output

These files are a real run of `linera validator benchmark` against a testnet
validator, captured in each output format. They illustrate what the tool
produces; the numbers are a point-in-time sample, not a target.

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
