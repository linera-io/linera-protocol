# Cross-chain messaging pitfalls

## Assuming global message ordering
- **Symptom:** Logic breaks when messages from different senders interleave.
- **Cause:** Only per-(sender→recipient) FIFO is guaranteed, via inbox cursors.
- **Fix:** Never assume a global order across senders. See
  `.context/architecture/cross-chain-messaging.md`.

## Re-applying already-consumed bundles
- **Symptom:** Duplicate effects after a retry/redelivery.
- **Cause:** Treating delivery as non-idempotent; ignoring the inbox `Cursor`.
- **Fix:** Delivery must be idempotent; bundles at/below the cursor are skipped, not
  re-applied. Cursors are monotonic.

## Breaking bundle boundaries
- **Symptom:** Off-by-one in `(height, index)`; messages lost or duplicated.
- **Cause:** Operating on individual messages instead of `MessageBundle` units.
- **Fix:** Preserve bundle boundaries; the unit of delivery is the bundle.

## Sender-side retention / TTL
- **Symptom:** Recipient can't fetch the block it needs to process an old message.
- **Cause:** `sender_chain_ttl` / `PreviousEventBlocks` retention window (#5997).
- **Fix:** Account for retention when reasoning about late delivery.

## Freshness
- Depends on: `linera-chain/src/{inbox,outbox}.rs`,
  `linera-rpc/src/cross_chain_message_queue.rs`. verified_commit: `cfa6e9e`; verified_at: 2026-06-03.
