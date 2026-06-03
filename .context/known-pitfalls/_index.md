# Known pitfalls

Gotchas that aren't obvious from reading a single file, and that agents repeatedly
re-discover. Each entry: **symptom → cause → what to do**, with a citation. These feed
the `pitfalls` field of `context_packet`.

| File | Scope |
|---|---|
| [`performance.md`](./performance.md) | Latency/throughput/memory traps |
| [`storage.md`](./storage.md) | RocksDB/Scylla/views traps |
| [`async-concurrency.md`](./async-concurrency.md) | Tokio, cancellation, channels |
| [`cross-chain.md`](./cross-chain.md) | Inbox/outbox delivery traps |

Keep entries short and falsifiable. If a pitfall is fixed, delete it (don't leave stale
warnings — imprecise context is worse than none).
