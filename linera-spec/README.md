# Linera specification

This crate contains no code. It is the entry point to the correctness specification of the
Linera microchain consensus protocol: the reading order, the conventions, and the index of every
numbered result.

The statements themselves live next to the code they describe, in `linera_chain::manager::proof`,
`linera_chain::data_types::proof`, `linera_chain::justification::proof` and `linera_core::proof`.
This crate exists so that a single index can link to all of them — it depends on both
`linera-chain` and `linera-core`, which neither of those crates can do.

```bash
cargo doc -p linera-spec --open
```
