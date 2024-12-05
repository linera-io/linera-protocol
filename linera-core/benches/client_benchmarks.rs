// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use criterion::{criterion_group, criterion_main, measurement::Measurement, BatchSize, Criterion};
use linera_base::{data_types::Amount, identifiers::Account, time::Duration};
use linera_core::{
    client,
    test_utils::{MemoryStorageBuilder, NodeProvider, StorageBuilder, TestBuilder},
};
use linera_execution::system::Recipient;
use linera_storage::{
    READ_CERTIFICATE_COUNTER, READ_HASHED_CONFIRMED_BLOCK_COUNTER, WRITE_CERTIFICATE_COUNTER,
};
use linera_views::metrics::{LOAD_VIEW_COUNTER, SAVE_VIEW_COUNTER};
use prometheus::core::Collector;
use recorder::BenchRecorderMeasurement;
use tokio::runtime;

type ChainClient<B> = client::ChainClient<
    NodeProvider<<B as StorageBuilder>::Storage>,
    <B as StorageBuilder>::Storage,
>;

mod recorder;

/// Creates root chains 1 and 2, the first one with a positive balance.
pub fn setup_claim_bench<B>() -> (ChainClient<B>, ChainClient<B>)
where
    B: StorageBuilder + Default,
{
    let storage_builder = B::default();
    // Criterion doesn't allow setup functions to be async, but it runs them inside an async
    // context. But our setup uses async functions:
    let handle = runtime::Handle::current();
    let _guard = handle.enter();
    futures::executor::block_on(async move {
        let mut builder = TestBuilder::new(storage_builder, 4, 1).await.unwrap();
        let chain1 = builder
            .add_root_chain(1, Amount::from_tokens(10))
            .await
            .unwrap();
        let chain2 = builder.add_root_chain(2, Amount::ZERO).await.unwrap();
        (chain1, chain2)
    })
}

/// Sends a token from the first chain to the first chain's owner on chain 2, then
/// reclaims that amount.
pub async fn run_claim_bench<B>((chain1, chain2): (ChainClient<B>, ChainClient<B>))
where
    B: StorageBuilder,
{
    let owner1 = chain1.identity().await.unwrap();
    let amt = Amount::ONE;

    let account = Account::owner(chain2.chain_id(), owner1);
    let cert = chain1
        .transfer_to_account(None, amt, account)
        .await
        .unwrap()
        .unwrap();

    chain2
        .receive_certificate_and_update_validators(cert)
        .await
        .unwrap();
    chain2.process_inbox().await.unwrap();
    assert_eq!(
        chain1.local_balance().await.unwrap(),
        Amount::from_tokens(9)
    );

    let account = Recipient::chain(chain1.chain_id());
    let cert = chain1
        .claim(owner1, chain2.chain_id(), account, amt)
        .await
        .unwrap()
        .unwrap();

    chain2
        .receive_certificate_and_update_validators(cert)
        .await
        .unwrap();
    let cert = chain2.process_inbox().await.unwrap().0.pop().unwrap();

    chain1
        .receive_certificate_and_update_validators(cert)
        .await
        .unwrap();
    chain1.process_inbox().await.unwrap();
    assert_eq!(
        chain1.local_balance().await.unwrap(),
        Amount::from_tokens(10)
    );
}

fn criterion_benchmark<M: Measurement + 'static>(c: &mut Criterion<M>) {
    c.bench_function("claim", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter_batched(
                setup_claim_bench::<MemoryStorageBuilder>,
                run_claim_bench::<MemoryStorageBuilder>,
                BatchSize::PerIteration,
            )
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(40))
        .with_measurement(BenchRecorderMeasurement::new(vec![
            READ_HASHED_CONFIRMED_BLOCK_COUNTER.desc()[0].fq_name.as_str(),
            READ_CERTIFICATE_COUNTER.desc()[0].fq_name.as_str(), WRITE_CERTIFICATE_COUNTER.desc()[0].fq_name.as_str(),
            LOAD_VIEW_COUNTER.desc()[0].fq_name.as_str(), SAVE_VIEW_COUNTER.desc()[0].fq_name.as_str(),
        ]));
    targets = criterion_benchmark
);
criterion_main!(benches);
