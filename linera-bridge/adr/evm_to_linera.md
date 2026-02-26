# EVM → Linera bridge constants (final)

This document records the **finalized constants and invariants** for ingesting EVM deposits into Linera.

## Scope

- **Source network**: **Base only** (single supported EVM source network).
- The EVM→Linera ingestion pipeline MUST reject deposits from any non-Base chain.

## Finality trust model

- Block/log finality is trusted through:
  - `RuntimeApi::is_block_finalized(source_chain_id, block_hash)`
- A deposit event is eligible for processing only if the runtime reports the containing block as finalized.

## Replay protection key

Replay detection MUST use the tuple:

- `(source_chain_id, block_hash, tx_index, log_index)`

This key is canonical and must be stored/checked before duplicate processing.

## Canonical `DepositInitiated` ABI

Canonical event declaration (field order and types are normative):

```solidity
event DepositInitiated(
    uint256 source_chain_id,
    bytes32 target_chain_id,
    bytes32 target_application_id,
    bytes32 target_account_owner,
    address token,
    uint256 amount
);
```

### Event-signature hash reference string

Use the following exact signature string for `topic0`/event hash derivation:

```text
DepositInitiated(uint256,bytes32,bytes32,bytes32,address,uint256)
```

> ⚠️ **Do not reorder event fields without migration.**
> Reordering changes the event signature and breaks existing indexers/replay records. Any change requires an explicit migration plan and versioned parser support.

## Amount mapping rule

- **1:1 amount rule**: credited amount on Linera MUST equal the `amount` emitted by `DepositInitiated`.
- No implicit scaling, fee subtraction, or decimal conversion is allowed in the bridge ingestion step.

## Retry / consumption semantics

- A deposit must be marked consumed **only after successful Linera credit**.
- If crediting fails, the event remains retryable and must not be consumed.
- Retries must preserve idempotency through the replay key above.
