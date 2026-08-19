# Linera specification

This crate contains no code. It is the entry point to the correctness specification of the
Linera protocol: the reading order, the conventions, and the index of every statement.

The specification is written subsystem by subsystem. What it establishes today is agreement on one
microchain's sequence of blocks, and what a certified block guarantees to nodes that were absent
when it was certified; the index's `Coverage` section says what is not yet constrained by any
statement.

The statements themselves live next to the code they describe, in `linera_chain::manager::proof`,
`linera_chain::data_types::proof`, `linera_chain::justification::proof`, `linera_chain::proof` and
`linera_core::proof`.
This crate exists so that a single index can link to all of them — it depends on both
`linera-chain` and `linera-core`, which neither of those crates can do.

## Reading it

Without cloning the repository, at <https://docs.rs/linera-spec/latest/linera_spec/>.

Locally, document the crates the specification links into and open the index:

```bash
cargo doc --no-deps -p linera-spec -p linera-chain -p linera-core \
                    -p linera-base -p linera-execution -p linera-views
open target/doc/linera_spec/index.html   # xdg-open on Linux
```

A few seconds on a warm workspace. Two things to note if you shorten it:

- `--open` picks one package when several are passed, and it is not `linera-spec`; open the file
  directly instead.
- Dropping `--no-deps` documents the whole dependency closure — several hundred crates — instead
  of six.

All six are needed for the links to resolve: the statement pages reference `linera_base` heavily
(rounds, ownership, block heights, crypto types) and `linera_execution` for committee thresholds,
and clicking through to `ChainManager` itself reaches `linera_views`.
