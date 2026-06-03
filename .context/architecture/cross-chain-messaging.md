# Cross-chain messaging (inbox / outbox)

## Purpose
Microchains communicate by sending **messages** that are queued in a sender's **outbox**
and consumed from a recipient's **inbox**. Read this for anything touching message
delivery, bundles, cursors, ordering/idempotency, or the cross-chain RPC path.

## Entry points
- `linera-chain/src/outbox.rs` — outbox state: pending sends per destination.
- `linera-chain/src/inbox.rs` — `InboxStateView<C>`: FIFO bundle queue + `Cursor`.
- `linera-rpc/src/cross_chain_message_queue.rs` — cross-chain propagation between servers.
- `linera-chain/src/unit_tests/inbox_tests.rs`, `outbox_tests.rs` — behavior anchors.
- `docs/developers/backend/messages.md` — canonical conceptual doc (SDK view).

## How it works
When a block executes, outgoing messages are recorded in the **outbox** keyed by
destination chain. A `MessageBundle` (chain id, block height, messages) is propagated to
the destination validator/shard via cross-chain RPC and appended to the recipient's
**inbox**. The inbox tracks position with a `Cursor` (`height`, `index`); the recipient
consumes bundles in order and advances the cursor. Reconciliation prevents duplicate or
out-of-order delivery.

## Invariants & gotchas
- **Cursor monotonicity**: inbox cursors only move forward; delivery must be idempotent.
  Re-delivered bundles below the cursor are skipped, not re-applied.
- **Bundles are the unit**, not individual messages — preserve bundle boundaries
  (`height`/`index`) when touching delivery code.
- Ordering guarantees are per-(sender→recipient) FIFO via cursors; do not assume a global
  order across senders.
- See `#5997` (`PreviousEventBlocks` / `sender_chain_ttl`) for sender-side retention
  nuance.

## Related
- Packs: `shard-workers.md` (where delivery is driven), `validator.md`.
- Metrics: `inbox_size`, `incoming_bundle_count`, `incoming_message_count`,
  `cross_chain_batch_size`, `outbox_counters_size` (added in #6440), `message_count`.
- Pitfalls: `../known-pitfalls/cross-chain.md`.

## How agents should use this
For "trace what happens when chain A receives a message from chain B", start at
`inbox.rs` (`InboxStateView`) + `outbox.rs`, then follow `cross_chain_message_queue.rs`.
Use `find_tests_for(target="linera-chain/src/inbox.rs")` for runnable examples.

## Freshness
- Depends on: `linera-chain/src/inbox.rs`, `linera-chain/src/outbox.rs`,
  `linera-rpc/src/cross_chain_message_queue.rs`.
- verified_commit: `cfa6e9e`
- verified_at: 2026-06-03
