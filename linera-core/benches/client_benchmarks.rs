// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use criterion::{criterion_group, criterion_main, measurement::Measurement, BatchSize, Criterion};
use linera_base::{
    data_types::Amount,
    identifiers::{Account, AccountOwner},
    time::Duration,
};
use linera_core::{
    client,
    test_utils::{MemoryStorageBuilder, NodeProvider, StorageBuilder, TestBuilder},
};
use linera_execution::system::Recipient;
use linera_storage::{
    READ_CERTIFICATE_COUNTER, READ_CONFIRMED_BLOCK_COUNTER, WRITE_CERTIFICATE_COUNTER,
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

pub fn setup_claim_bench<B>() -> (ChainClient<B>, ChainClient<B>)
where
    B: StorageBuilder + Default,
{
    let storage_builder = B::default();
    let handle = runtime::Handle::current();
    let _guard = handle.enter();
    futures::executor::block_on(async move {
        let mut builder = TestBuilder::new(storage_builder, 4, 1)
            .await
            .expect("Failed to create TestBuilder");
        let chain1 = builder
            .add_root_chain(1, Amount::from_tokens(10))
            .await
            .expect("Failed to add chain 1");
        let chain2 = builder
            .add_root_chain(2, Amount::ZERO)
            .await
            .expect("Failed to add chain 2");
        (chain1, chain2)
    })
}

pub async fn run_claim_bench<B>((chain1, chain2): (ChainClient<B>, ChainClient<B>))
where
    B: StorageBuilder,
{
    let owner1 = chain1.identity().await.expect("Failed to get identity");
    let amt = Amount::ONE;

    let account = Account::new(chain2.chain_id(), owner1);
    let cert = chain1
        .transfer_to_account(AccountOwner::CHAIN, amt, account)
        .await
        .expect("Transfer failed")
        .expect("No certificate produced");

    chain2
        .receive_certificate_and_update_validators(cert)
        .await
        .expect("Chain2 failed to receive cert");
    chain2.process_inbox().await.expect("Chain2 failed to process inbox");

    assert_eq!(
        chain1.local_balance().await.expect("Failed to get balance"),
        Amount::from_tokens(9)
    );

    let recipient = Recipient::chain(chain1.chain_id());
    let cert = chain1
        .claim(owner1, chain2.chain_id(), recipient, amt)
        .await
        .expect("Claim failed")
        .expect("No certificate produced");

    chain2
        .receive_certificate_and_update_validators(cert)
        .await
        .expect("Chain2 failed to receive claim cert");

    let cert = chain2
        .process_inbox()
        .await
        .expect("Chain2 inbox processing failed")
        .0
        .pop()
        .expect("No cert in processed inbox");

    chain1
        .receive_certificate_and_update_validators(cert)
        .await
        .expect("Chain1 failed to receive cert");
    chain1.process_inbox().await.expect("Chain1 failed to process inbox");

    assert_eq!(
        chain1.local_balance().await.expect("Failed to get balance"),
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
            READ_CONFIRMED_BLOCK_COUNTER.desc()[0].fq_name.as_str(),
            READ_CERTIFICATE_COUNTER.desc()[0].fq_name.as_str(),
            WRITE_CERTIFICATE_COUNTER.desc()[0].fq_name.as_str(),
            LOAD_VIEW_COUNTER.desc()[0].fq_name.as_str(),
            SAVE_VIEW_COUNTER.desc()[0].fq_name.as_str(),
        ]));
    targets = criterion_benchmark
);
criterion_main!(benches);
