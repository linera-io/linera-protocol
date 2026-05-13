# linera-storage-udf

ScyllaDB Wasm UDFs that decode and encode Linera `RootKey` partition keys so engineers can
inspect raw `system.large_partitions` output, or query for a specific chain by ID, directly
in `cqlsh`.

## What's in here

Two UDFs:

- `decode_root_key(raw blob) RETURNS text` — given a `root_key` column value, return the
  `Debug` representation of the BCS-encoded `RootKey` (e.g. `ChainState(ChainId(...))`).
- `encode_root_key(variant text, payload_hex text) RETURNS blob` — given a variant name and
  hex payload, return the partition key blob, suitable for `WHERE root_key = ?`.

Both transparently handle the leading `0x00` tag byte that
`linera-views::backends::scylla_db::get_big_root_key` prepends.

## Building

```bash
./linera-storage/udf/build.sh
```

Requires `cargo`, `rustup` (with the `wasm32-wasip1` target), and `wasm2wat` (`npm i -g wabt`).

Output:
- `target/wasm32-wasip1/wasm-udf/linera_storage_udf.wasm` — the binary
- `target/wasm32-wasip1/wasm-udf/linera_storage_udf.wat` — text format, ready for CQL

## Registering

ScyllaDB Wasm UDFs are still experimental, so each node's `scylla.yaml` must include
`udf` in `experimental_features`. Then:

```bash
WAT=$(sed "s/'/''/g" target/wasm32-wasip1/wasm-udf/linera_storage_udf.wat)
cqlsh -e "
  CREATE OR REPLACE FUNCTION linera.decode_root_key(raw blob)
    RETURNS NULL ON NULL INPUT
    RETURNS text
    LANGUAGE wasm
    AS '${WAT}';
"
```

Same for `encode_root_key`.

## Example queries

Top partitions by size, decoded:

```sql
SELECT decode_root_key(partition_key) AS what, partition_size
FROM system.large_partitions
WHERE keyspace_name = 'linera'
ORDER BY partition_size DESC
LIMIT 20;
```

All rows for a known chain, across the four `ChainId`-wrapping variants:

```sql
SELECT decode_root_key(root_key), length(k), length(v)
FROM linera."ns_42"
WHERE root_key IN (
  encode_root_key('ChainState',       '7a3f1c4d...4b9c'),
  encode_root_key('Event',            '7a3f1c4d...4b9c'),
  encode_root_key('BlockByHeight',    '7a3f1c4d...4b9c'),
  encode_root_key('EventBlockHeight', '7a3f1c4d...4b9c')
);
```

## Payload format for `encode_root_key`

| Variant | payload_hex |
|---|---|
| `NetworkDescription` | empty |
| `BlockExporterState` | 4 bytes (u32, little-endian) |
| `ChainState` / `Event` / `BlockByHeight` / `EventBlockHeight` | 32 bytes (CryptoHash) |
| `BlockHash` | 32 bytes (CryptoHash) |
| `BlobId` | 32 bytes (CryptoHash) + 1 byte (BlobType BCS variant tag) |

A `0x` prefix is accepted and stripped. Unknown variants or wrong payload lengths return an
empty blob, which causes `WHERE` clauses to match nothing (rather than the wrong rows).

## Source-of-truth note

`RootKey` is mirrored here from `linera-storage::db_storage` because compiling the full
`linera-storage` crate to `wasm32-wasip1` would drag in the validator dependency tree. The
test in `linera-storage/tests/root_key_drift.rs` asserts that the two definitions BCS-encode
identically. If you change the canonical `RootKey`, update this crate's mirror too — the
drift test will fail otherwise.
