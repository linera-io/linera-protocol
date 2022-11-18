// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use aws_sdk_dynamodb::Client;
use linera_views::{
    common::{Batch, KeyValueOperations},
    dynamo_db::{DynamodbContainer, TableName},
    memory::MemoryContainer,
    rocksdb::{RocksdbContainer, DB},
    test_utils::{get_random_vec_keyvalues, LocalStackTestContext},
};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock};

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
    let l_keys = key_value_operation
        .find_keys_with_prefix(&key_prefix)
        .await
        .unwrap();
    for i in 1..l_keys.len() {
        let key1 = l_keys[i - 1].clone();
        let key2 = l_keys[i].clone();
        assert!(key1 < key2);
    }
    let mut map = HashMap::new();
    for key in &l_keys {
        for u in 1..key.len() {
            let key_prefix = key[0..u].to_vec();
            match map.get_mut(&key_prefix) {
                Some(v) => {
                    *v += 1;
                }
                None => {
                    map.insert(key_prefix, 1);
                }
            }
        }
    }
    for (key_prefix, value) in map {
        let l_keys_ret = key_value_operation
            .find_keys_with_prefix(&key_prefix)
            .await
            .unwrap();
        assert!(l_keys_ret.len() == value);
    }
}

#[tokio::test]
async fn test_ordering_memory() {
    let map = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = map.clone().lock_owned().await;
    let key_value_operation: MemoryContainer = Arc::new(RwLock::new(guard));
    test_ordering_keys(key_value_operation).await;
}

#[tokio::test]
async fn test_ordering_rocksdb() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    let key_value_operation: RocksdbContainer = Arc::new(DB::open(&options, &dir).unwrap());
    //
    test_ordering_keys(key_value_operation).await;
}

#[tokio::test]
#[ignore]
async fn test_ordering_dynamodb() {
    let localstack = LocalStackTestContext::new().await.unwrap();
    let config = localstack.dynamo_db_config();
    let client = Client::from_conf(config);
    let tablename_str = "test_table".to_string();
    let table = TableName::from_str(&tablename_str).unwrap();
    let key_value_operation = DynamodbContainer { client, table };
    //
    test_ordering_keys(key_value_operation).await;
}
