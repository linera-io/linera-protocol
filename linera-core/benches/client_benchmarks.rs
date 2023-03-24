// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use criterion::{criterion_group, criterion_main, measurement::Measurement, BatchSize, Criterion};
use linera_base::{
    data_types::{Amount, Balance},
    identifiers::ChainDescription,
};
use linera_core::client::{
    self,
    client_test_utils::{MakeMemoryStoreClient, NodeProvider, StoreBuilder, TestBuilder},
};
use linera_execution::system::{Account, Recipient, UserData};
use linera_storage::{
    Store, READ_CERTIFICATE_COUNTER, READ_VALUE_COUNTER, WRITE_CERTIFICATE_COUNTER,
    WRITE_VALUE_COUNTER,
};
use linera_views::{views::ViewError, LOAD_VIEW_COUNTER, SAVE_VIEW_COUNTER};
use recorder::{BenchRecorder, BenchRecorderMeasurement};
use std::time::Duration;
use tokio::runtime;

type ChainClient<B> =
    client::ChainClient<NodeProvider<<B as StoreBuilder>::Store>, <B as StoreBuilder>::Store>;

mod recorder;

/// Creates root chains 1 and 2, the first one with a positive balance.
pub fn setup_claim_bench<B>() -> (ChainClient<B>, ChainClient<B>)
where
    B: StoreBuilder + Default,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let store_builder = B::default();
    // Criterion doesn't allow setup functions to be async, but it runs them inside an async
    // context. But our setup uses async functions:
    let handle = runtime::Handle::current();
    let _guard = handle.enter();
    futures::executor::block_on(async move {
        let mut builder = TestBuilder::new(store_builder, 4, 1).await.unwrap();
        let chain1 = builder
            .add_initial_chain(ChainDescription::Root(1), Balance::from(10))
            .await
            .unwrap();
        let chain2 = builder
            .add_initial_chain(ChainDescription::Root(2), Balance::from(0))
            .await
            .unwrap();
        (chain1, chain2)
    })
}

/// Sends a token from the first chain to the first chain's owner on chain 2, then
/// reclaims that amount.
pub async fn run_claim_bench<B>((mut chain1, mut chain2): (ChainClient<B>, ChainClient<B>))
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let owner1 = chain1.identity().await.unwrap();
    let amt = Amount::from(1);

    let account = Account::owner(chain2.chain_id(), owner1);
    let cert = chain1
        .transfer_to_account(None, amt, account, UserData(None))
        .await
        .unwrap();

    chain2.receive_certificate(cert).await.unwrap();
    chain2.process_inbox().await.unwrap();
    assert_eq!(chain1.local_balance().await.unwrap(), Balance::from(9));

    let account = Recipient::Account(Account::chain(chain1.chain_id()));
    let cert = chain1
        .claim(owner1, chain2.chain_id(), account, amt, UserData(None))
        .await
        .unwrap();

    chain2.receive_certificate(cert).await.unwrap();
    let cert = chain2.process_inbox().await.unwrap().pop().unwrap();

    chain1.receive_certificate(cert).await.unwrap();
    chain1.process_inbox().await.unwrap();
    assert_eq!(chain1.local_balance().await.unwrap(), Balance::from(10));
}

fn criterion_benchmark<M: Measurement + 'static>(c: &mut Criterion<M>) {
    c.bench_function("claim", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter_batched(
                setup_claim_bench::<MakeMemoryStoreClient>,
                run_claim_bench::<MakeMemoryStoreClient>,
                BatchSize::PerIteration,
            )
    });
}

fn create_recorder() -> BenchRecorder {
    let recorder = BenchRecorder::default();
    recorder.clone().install().unwrap();
    recorder
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(40))
        .with_measurement(BenchRecorderMeasurement::new(create_recorder(), vec![
            READ_VALUE_COUNTER, WRITE_VALUE_COUNTER,
            READ_CERTIFICATE_COUNTER, WRITE_CERTIFICATE_COUNTER,
            LOAD_VIEW_COUNTER, SAVE_VIEW_COUNTER
        ]));
    targets = criterion_benchmark
);
criterion_main!(benches);
