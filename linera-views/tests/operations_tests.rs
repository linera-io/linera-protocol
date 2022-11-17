// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::common::{Batch, KeyValueOperations};
use tokio::sync::{RwLock, Mutex};
use std::sync::Arc;
use linera_views::memory::MemoryContainer;
use std::collections::BTreeMap;
use linera_views::rocksdb::{RocksdbContainer, DB};
use linera_views::dynamo_db::{DynamodbContainer, TableName};
use linera_views::test_utils::{LocalStackTestContext, get_random_vec_keyvalues};
use aws_sdk_dynamodb::Client;
use std::str::FromStr;

#[cfg(test)]
async fn test_ordering_keys<OP: KeyValueOperations>(key_value_operation: OP) {
    let n = 1000;
    let l_kv = get_random_vec_keyvalues(n);
    let mut batch = Batch::default();
    for e_kv in l_kv {
        batch.put_key_value_bytes(e_kv.0, e_kv.1);
    }
    key_value_operation.write_batch(batch).await.unwrap();
    let key_prefix = Vec::new();
    let l_keys = key_value_operation.find_keys_with_prefix(&key_prefix).await.unwrap();
    for i in 0..l_keys.len() {
        println!("i={} key={:?}", i, l_keys[i].clone());
    }
    for i in 1..l_keys.len() {
        let key1 = l_keys[i-1].clone();
        let key2 = l_keys[i].clone();
        println!("key1={:?} key2={:?}", key1, key2);
        assert!(key1 < key2);
    }
}

#[tokio::test]
async fn test_ordering_memory() {
    let map = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = map.clone().lock_owned().await;
    let key_value_operation : MemoryContainer = Arc::new(RwLock::new(guard));
    test_ordering_keys(key_value_operation).await;
}

#[tokio::test]
async fn test_ordering_rocksdb() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    let key_value_operation : RocksdbContainer = Arc::new(DB::open(&options, &dir).unwrap());
    //
    test_ordering_keys(key_value_operation).await;
}


#[tokio::test]
#[ignore]
async fn test_ordering_dynamodb() {
    let localstack =  LocalStackTestContext::new().await.unwrap();
    let config = localstack.dynamo_db_config();
    let client = Client::from_conf(config.into());
    let tablename_str = "test_table".to_string();
    let tablename = TableName::from_str(&tablename_str).unwrap();
    let key_value_operation = DynamodbContainer { client: client, table: tablename };
    //
    test_ordering_keys(key_value_operation).await;
}

