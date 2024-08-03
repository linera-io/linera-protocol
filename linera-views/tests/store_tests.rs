// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    batch::Batch,
    common::{ReadableKeyValueStore, WritableKeyValueStore},
    key_value_store_view::ViewContainer,
    memory::{create_test_memory_context, create_test_memory_store},
    test_utils::{
        self, get_random_test_scenarios, run_big_write_read, run_reads, run_writes_from_blank,
        run_writes_from_state,
    },
    value_splitting::create_value_splitting_memory_store,
};
#[cfg(web)]
use wasm_bindgen_test::wasm_bindgen_test;

#[cfg(web)]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

#[tokio::test]
async fn test_reads_test_memory() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_value_splitting_memory_store();
        run_reads(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_reads_memory() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_test_memory_store();
        run_reads(key_value_store, scenario).await;
    }
}

#[cfg(with_rocksdb)]
#[tokio::test]
async fn test_reads_rocks_db() {
    for scenario in get_random_test_scenarios() {
        let (key_value_store, _dir) = linera_views::rocks_db::create_rocks_db_test_store().await;
        run_reads(key_value_store, scenario).await;
    }
}

#[cfg(with_dynamodb)]
#[tokio::test]
async fn test_reads_dynamo_db() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = linera_views::dynamo_db::create_dynamo_db_test_store().await;
        run_reads(key_value_store, scenario).await;
    }
}

#[cfg(with_scylladb)]
#[tokio::test]
async fn test_reads_scylla_db() {
    for scenario in get_random_test_scenarios() {
        let store = linera_views::scylla_db::create_scylla_db_test_store().await;
        run_reads(store, scenario).await;
    }
}

#[cfg(with_indexeddb)]
#[wasm_bindgen_test]
async fn test_reads_indexed_db() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = linera_views::indexed_db::create_indexed_db_test_store().await;
        run_reads(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_reads_key_value_store_view_memory() {
    for scenario in get_random_test_scenarios() {
        let context = create_test_memory_context();
        let key_value_store = ViewContainer::new(context).await.unwrap();
        run_reads(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_specific_reads_memory() {
    let key_value_store = create_test_memory_store();
    let key_values = vec![
        (vec![0, 1, 255], Vec::new()),
        (vec![0, 1, 255, 37], Vec::new()),
        (vec![0, 2], Vec::new()),
        (vec![0, 2, 0], Vec::new()),
    ];
    run_reads(key_value_store, key_values).await;
}

#[tokio::test]
async fn test_test_memory_writes_from_blank() {
    let key_value_store = create_value_splitting_memory_store();
    run_writes_from_blank(&key_value_store).await;
}

#[tokio::test]
async fn test_memory_writes_from_blank() {
    let key_value_store = create_test_memory_store();
    run_writes_from_blank(&key_value_store).await;
}

#[tokio::test]
async fn test_key_value_store_view_memory_writes_from_blank() {
    let context = create_test_memory_context();
    let key_value_store = ViewContainer::new(context).await.unwrap();
    run_writes_from_blank(&key_value_store).await;
}

#[cfg(with_rocksdb)]
#[tokio::test]
async fn test_rocks_db_writes_from_blank() {
    let (key_value_store, _dir) = linera_views::rocks_db::create_rocks_db_test_store().await;
    run_writes_from_blank(&key_value_store).await;
}

#[cfg(with_dynamodb)]
#[tokio::test]
async fn test_dynamo_db_writes_from_blank() {
    let key_value_store = linera_views::dynamo_db::create_dynamo_db_test_store().await;
    run_writes_from_blank(&key_value_store).await;
}

#[cfg(with_scylladb)]
#[tokio::test]
async fn test_scylla_db_writes_from_blank() {
    let key_value_store = linera_views::scylla_db::create_scylla_db_test_store().await;
    run_writes_from_blank(&key_value_store).await;
}

#[cfg(with_indexeddb)]
#[wasm_bindgen_test]
async fn test_indexed_db_writes_from_blank() {
    let key_value_store = linera_views::indexed_db::create_indexed_db_test_store().await;
    run_writes_from_blank(&key_value_store).await;
}

#[tokio::test]
async fn test_big_value_read_write() {
    use rand::{distributions::Alphanumeric, Rng};
    let context = create_test_memory_context();
    for count in [50, 1024] {
        let rng = test_utils::make_deterministic_rng();
        let test_string = rng
            .sample_iter(&Alphanumeric)
            .take(count)
            .map(char::from)
            .collect::<String>();
        let mut batch = Batch::new();
        let key = vec![43, 23, 56];
        batch.put_key_value(key.clone(), &test_string).unwrap();
        context.store.write_batch(batch, &[]).await.unwrap();
        let read_string = context
            .store
            .read_value::<String>(&key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read_string, test_string);
    }
}

#[cfg(with_scylladb)]
#[tokio::test]
async fn scylla_db_tombstone_triggering_test() {
    let key_value_store = linera_views::scylla_db::create_scylla_db_test_store().await;
    linera_views::test_utils::tombstone_triggering_test(key_value_store).await;
}

#[cfg(with_scylladb)]
#[tokio::test]
async fn test_scylla_db_big_write_read() {
    let key_value_store = linera_views::scylla_db::create_scylla_db_test_store().await;
    let value_sizes = vec![100, 1000, 200000, 5000000];
    let target_size = 20000000;
    run_big_write_read(key_value_store, target_size, value_sizes).await;
}

#[tokio::test]
async fn test_memory_big_write_read() {
    let key_value_store = linera_views::memory::create_test_memory_store();
    let value_sizes = vec![100, 1000, 200000, 5000000];
    let target_size = 20000000;
    run_big_write_read(key_value_store, target_size, value_sizes).await;
}

#[cfg(with_rocksdb)]
#[tokio::test]
async fn test_rocks_db_big_write_read() {
    let (key_value_store, _dir) = linera_views::rocks_db::create_rocks_db_test_store().await;
    let value_sizes = vec![100, 1000, 200000, 5000000];
    let target_size = 20000000;
    run_big_write_read(key_value_store, target_size, value_sizes).await;
}

#[cfg(with_indexeddb)]
#[wasm_bindgen_test]
async fn test_indexed_db_big_write_read() {
    let key_value_store = linera_views::indexed_db::create_indexed_db_test_store().await;
    let value_sizes = vec![100, 1000, 200000, 5000000];
    let target_size = 20000000;
    run_big_write_read(key_value_store, target_size, value_sizes).await;
}

#[cfg(with_dynamodb)]
#[tokio::test]
async fn test_dynamo_db_big_write_read() {
    let key_value_store = linera_views::dynamo_db::create_dynamo_db_test_store().await;
    let value_sizes = vec![100, 1000, 200000, 5000000];
    let target_size = 20000000;
    run_big_write_read(key_value_store, target_size, value_sizes).await;
}

#[tokio::test]
async fn test_memory_writes_from_state() {
    let key_value_store = create_test_memory_store();
    run_writes_from_state(&key_value_store).await;
}

#[cfg(with_rocksdb)]
#[tokio::test]
async fn test_rocks_db_writes_from_state() {
    let (key_value_store, _dir) = linera_views::rocks_db::create_rocks_db_test_store().await;
    run_writes_from_state(&key_value_store).await;
}

#[cfg(with_indexeddb)]
#[wasm_bindgen_test]
async fn test_indexed_db_writes_from_state() {
    let key_value_store = linera_views::indexed_db::create_indexed_db_test_store().await;
    run_writes_from_state(&key_value_store).await;
}

#[cfg(with_dynamodb)]
#[tokio::test]
async fn test_dynamo_db_writes_from_state() {
    let key_value_store = linera_views::dynamo_db::create_dynamo_db_test_store().await;
    run_writes_from_state(&key_value_store).await;
}

#[cfg(with_scylladb)]
#[tokio::test]
async fn test_scylla_db_writes_from_state() {
    let key_value_store = linera_views::scylla_db::create_scylla_db_test_store().await;
    run_writes_from_state(&key_value_store).await;
}
