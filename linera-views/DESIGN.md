This document collects some design choices that were made in the `linera-views`.
For an overview, see `README.md`.

# Design

## Key Value Store Clients

We have designed a `KeyValueStore` trait that represents the basic functionalities
of a key-value store whose keys are `Vec<u8>` and whose values are `Vec<u8>`.

We provide an implementation of the trait `KeyValueStore` for the following key-value stores:
* `MemoryStore` uses the memory (and uses internally a simple B-Tree map).
* `RocksDbStore` is a disk-based key-value store
* `DynamoDbStore` is the AWS-based DynamoDB service.
* `ScyllaDbStore` is a cloud-based Cassandra-compatible database.

The trait `KeyValueStore` was designed so that more storage solutions can be easily added in the future.

The `KeyValueStore` trait is also implemented for several internal constructions of clients:
* The `LruCachingStore<K>` client implements the Least Recently Used (LRU)
caching of reads into the client.
* The `ViewContainer<C>` client implements a key-value store client from a context.
* The `ValueSplittingStore<K>` implements a client for which the
size of the values is unbounded, on top of another client for which it is bounded.
(Some databases have strict limitations on the value size.)

## Views

A view is a container that mimics some of the properties of classic containers such
as `VecDeque`, `BTreeMap`, etc. with the following goals:
* The data structure can be written to the key-value store and loaded from there.
* The changes are first done in memory and accessed there. The changes can be committed
to the key-value store client when adequate.

There are various views corresponding to several use cases. All of those are
implemented by mapping the values in their specific types to their serializations.

## Allowed keys, base key and prefixes

The keys are constructed step by step from the prefix to the full key. So, whenever
we have a view or any other object then we have a `base_key` associated with it and
with no other objects. A view is associated with many keys, all of which share the same
`base_key` as prefix.
This leads us to introduce a trait `Context` that can be implemented with a specific
`KeyValueStore` client and a `base_key`.

Another issue to consider is that we do not want to just store the values, but we
also need to accommodate other features:
* Administrative data (like journal) need to store some data starting from the `base_key`.
* Some of the values need to be split into several smaller values.

The rules for constructing keys are the following:
* For the construction of `struct` objects of associated base key `base_key` we do the
following: If the corresponding type has entries `entry_0`, ..., `entry_k` then the
base key of the object associated with the `k`-th entry is `[base_key * * * *]` where
`[* * * *]` are the four bytes of `k` as a `u32`.
* For each view type, we have some associated data. Some are the hash, others the counts
(e.g. for `LogView` and `QueueView`) and some the index. The corresponding enum is
named `KeyTag` for each of those containers. For the index, the values are serialized.
* For a given base key `base_key` the keys of the form `[base_key tag]` with tags in
`0..MIN_VIEW_TAG` are reserved for administrative data.

These constructions are done in order to satisfy the following property: The set of keys
of a view is prefix-free. That is if a key is of the form `key = [a b]` then `a` is
not a key. This property is important for other functionalities.
If the user is using a single view then for whatever `base_key` is chosen, the set of keys
created will be prefix-free. If several views are created then their base keys must be
chosen to be prefix-free so that the whole set of keys is prefix-free.

## API of the `KeyValueStore`

The design of the key-value store client is done in the following way:
* We can put a `(key, value)` and delete a `key`.
* We can delete keys having a specific prefix.
* We can retrieve the list of keys having a specific prefix. Similarly, we can
retrieve the list of `(key, value)` whose keys have a specific prefix.
* Also, the list of the keys is provided in the lexicographic order.

## Journaling

We want to make sure that all operations are done on the database in an atomic
way. That is the operations have to be processed completely. The idea is to
store the journal in the `[base_key 0 * * * *]` keys with `[* * * *]` the
serialization of an `u32` entry.
Those keys are not colliding with the other keys since their value is of the
form `[base_key tag *]` with `tag >= MIN_VIEW_TAG` and currently, we have
`MIN_VIEW_TAG = 1`.

The journaling allows to have operations in an atomic way. It is done in the
following way:
* When a `Batch` of operations needs to be processed, the operations are written
into blocks that can be processed atomically. All together the blocks for the
"journal". When the journal has been written then the counter is written and
only at that point is the journal considered written.
* Each block is then processed atomically. If an interruption occurs then
the block entries would be processed after accessing the database the next time.

## Splitting large values across keys

Some key-value store clients limit the size of the values (named `MAX_VALUE_SIZE`
in the code). `ValueSplittingStore` is a wrapper that accepts values
of any size. Internally, it splits them into smaller pieces and stores them
using a wrapped, possibly size-limited, client.

The built key-value store client has `MAX_VALUE_SIZE` set to its maximum possible
value. This forces us to split the value into several smaller values. One key
such example is `DynamoDb` which has a limit of 400kB on its value size.

Design is the following:
* For every `key` we build several corresponding keys `[key * * * *]` that contain
each a piece of the full value.
The `[* * * *]` is the serialization of the index which is `u32` so that
the order of the serialization is the same as the order of the `u32` entries.
* For the first key, that is `[key 0 0 0 0]` the corresponding value is `[* * * * value_0]`
with `[* * * *]` being the serialization of the `u32` count of the number of blocks
and `value_0` the first block of the value. For the other keys `[key * * * *]` the
corresponding value is `value_k` with `value_k` the k-th entry in the value. The full
value is reconstructed as `value = [value_0 value_1 .... value_N]`
The number of blocks is always non-zero, even if the value is empty.

The effective working of the system relies on several design choices.
* In `find_keys_by_prefix` and similar, the keys are
returned in lexicographic order.
* The count and indices are serialized as `u32` and the serialization is done
so that the `u32` ordering is the same as the lexicographic ordering. So, we
get first the first key (and so the count) and then the segment keys one by
one in the correct order.
* Suppose that we have written a `(key, value_1)` with several segments and then
we replace by a `(key, value_2)` which has fewer segments.
Our choice is to leave the old segments in the database. Same with delete:
we just delete the first key.
* We avoid the overflowing of the system by the following trick: When we delete
a view, we do a delete by prefix, which thus deletes the old segments if present.
* In order to delete all the unused segments when overwriting a value, every write
would need to have a read just before which is expensive.

A key difficulty of this modelization is that we are using prefixes extensively
and so by adding the index of the form `[* * * *]` we are potentially creating
some collisions. That is if we delete a key of the form `[key 0 0]` we would
also delete segments of the key of the form `key`. However, this cannot happen
because the set of keys is prefix-free.
