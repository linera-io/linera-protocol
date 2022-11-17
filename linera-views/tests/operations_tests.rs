// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::common::{Batch, KeyValueOperations};
use tokio::sync::{RwLock, Mutex};
use std::sync::Arc;
use linera_views::memory::MemoryContainer;
use std::collections::BTreeMap;

#[cfg(test)]
use linera_views::common_test::get_random_vec_keyvalues;


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
    for i in 1..l_keys.len() {
        let key1 = l_keys[i-1].clone();
        let key2 = l_keys[i].clone();
        assert!(key1 >= key2);
    }
}

#[tokio::test]
async fn test_operations_in_memory() {
    let map = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = map.clone().lock_owned().await;
    let key_value_operation : MemoryContainer = Arc::new(RwLock::new(guard));
    test_ordering_keys(key_value_operation).await;
}
