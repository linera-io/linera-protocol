// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::marker::PhantomData;

#[cfg(with_dynamodb)]
use linera_views::dynamo_db::DynamoDbDatabase;
#[cfg(with_rocksdb)]
use linera_views::rocks_db::RocksDbDatabase;
#[cfg(with_scylladb)]
use linera_views::scylla_db::ScyllaDbDatabase;
use linera_views::{
    memory::MemoryDatabase,
    store::{KeyValueStore, TestKeyValueDatabase},
    test_utils::{namespace_admin_test, root_key_admin_test},
};
use test_case::test_case;

#[test_case(PhantomData::<MemoryDatabase>; "MemoryDatabase")]
#[cfg_attr(with_rocksdb, test_case(PhantomData::<RocksDbDatabase>; "RocksDbDatabase"))]
#[cfg_attr(with_dynamodb, test_case(PhantomData::<DynamoDbDatabase>; "DynamoDbDatabase"))]
#[cfg_attr(with_scylladb, test_case(PhantomData::<ScyllaDbDatabase>; "ScyllaDbDatabase"))]
#[tokio::test]
async fn namespace_admin_test_cases<K: TestKeyValueDatabase>(_view_type: PhantomData<K>)
where
    K::Store: KeyValueStore,
{
    namespace_admin_test::<K>().await;
}

#[test_case(PhantomData::<MemoryDatabase>; "MemoryDatabase")]
#[cfg_attr(with_rocksdb, test_case(PhantomData::<RocksDbDatabase>; "RocksDbDatabase"))]
#[cfg_attr(with_dynamodb, test_case(PhantomData::<DynamoDbDatabase>; "DynamoDbDatabase"))]
#[cfg_attr(with_scylladb, test_case(PhantomData::<ScyllaDbDatabase>; "ScyllaDbDatabase"))]
#[tokio::test]
async fn root_key_admin_test_cases<K: TestKeyValueDatabase>(_view_type: PhantomData<K>)
where
    K::Store: KeyValueStore,
{
    root_key_admin_test::<K>().await;
}
