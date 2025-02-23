// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::marker::PhantomData;

#[cfg(with_dynamodb)]
use linera_views::dynamo_db::DynamoDbStore;
#[cfg(with_rocksdb)]
use linera_views::rocks_db::RocksDbStore;
#[cfg(with_scylladb)]
use linera_views::scylla_db::ScyllaDbStore;
use linera_views::{
    memory::MemoryStore,
    store::TestKeyValueStore,
    test_utils::{namespace_admin_test, root_key_admin_test},
};
use test_case::test_case;

#[test_case(PhantomData::<MemoryStore>; "MemoryStore")]
#[cfg_attr(with_rocksdb, test_case(PhantomData::<RocksDbStore>; "RocksDbStore"))]
#[cfg_attr(with_dynamodb, test_case(PhantomData::<DynamoDbStore>; "DynamoDbStore"))]
#[cfg_attr(with_scylladb, test_case(PhantomData::<ScyllaDbStore>; "ScyllaDbStore"))]
#[tokio::test]
async fn namespace_admin_test_cases<K: TestKeyValueStore>(_view_type: PhantomData<K>) {
    namespace_admin_test::<K>().await;
}

#[test_case(PhantomData::<MemoryStore>; "MemoryStore")]
#[cfg_attr(with_rocksdb, test_case(PhantomData::<RocksDbStore>; "RocksDbStore"))]
#[cfg_attr(with_dynamodb, test_case(PhantomData::<DynamoDbStore>; "DynamoDbStore"))]
#[cfg_attr(with_scylladb, test_case(PhantomData::<ScyllaDbStore>; "ScyllaDbStore"))]
#[tokio::test]
async fn root_key_admin_test_cases<K: TestKeyValueStore>(_view_type: PhantomData<K>) {
    root_key_admin_test::<K>().await;
}
