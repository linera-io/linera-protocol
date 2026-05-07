<!-- cargo-rdme start -->

Caching utilities for the Linera protocol.

## Hash-consing and the "one allocation per content" invariant

[`ValueCache`] is the canonical home for content-addressed immutable data
(also known as hash-consed data) such as `Block`, `Blob`, and
`ConfirmedBlockCertificate`. For such types the cache guarantees that at
most one allocation exists per distinct content at any time, and all
consumers share the same `Arc<T>`.

The guarantee is implemented by combining two structures:

- A bounded `quick_cache` (S3-FIFO eviction) for hot-path lookups.
- A lock-free `papaya::HashMap<K, Weak<V>>` weak index that survives bounded
  eviction. If the bounded cache evicts an entry while a consumer still
  holds an `Arc`, re-requesting the same key returns the existing
  allocation instead of creating a duplicate.

A background task periodically sweeps dead `Weak` entries from the index to
prevent unbounded growth.

For the invariant to hold, all inserts of hash-consed values must go
through the cache (e.g. [`ValueCache::insert_hashed`] or
[`ValueCache::insert_arc`]). Constructing an `Arc::new(value)` off-path
creates a duplicate allocation that bypasses the dedup index.

<!-- cargo-rdme end -->

## Contributing

See the [CONTRIBUTING](../CONTRIBUTING.md) file for how to help out.

## License

This project is available under the terms of the [Apache 2.0 license](../LICENSE).
