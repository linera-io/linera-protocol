// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for delegating the formation of confirmation certificates.
//!
//! A delegate is never trusted, so each test asks the same two questions: did the block get
//! committed, and did it get committed to the *right* value. A delegate that lies, fails, or
//! hands the work back must cost the client a fallback and nothing else.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[cfg(not(web))]
use futures::future::BoxFuture;
#[cfg(web)]
use futures::future::LocalBoxFuture as BoxFuture;
use linera_base::{
    crypto::InMemorySigner,
    data_types::{Amount, Blob, BlockHeight},
    identifiers::{Account, AccountOwner, ChainId},
};
use linera_chain::{
    data_types::BlockProposal,
    manager::LockingBlock,
    types::{Block, ConfirmedBlockCertificate, ValidatedBlockCertificate},
};
use test_case::test_case;

use crate::{
    client::chain_client,
    data_types::ChainInfo,
    delegate::{DelegatedOutcome, LocalProposerDelegate, ProposerDelegate},
    environment::Impl,
    node::{CrossChainMessageDelivery, NodeError},
    test_utils::{
        ClientOutcomeResultExt as _, FaultType, MemoryStorageBuilder, NodeProvider, StorageBuilder,
        TestBuilder,
    },
};

/// The environment a [`TestBuilder`]'s clients run in.
type TestEnv<S> = Impl<S, NodeProvider<S>>;

/// Options that route block commits through a delegate, including the block's broadcast.
fn delegating_options() -> chain_client::Options {
    chain_client::Options {
        delegate_validator_updates: true,
        ..chain_client::Options::test_default()
    }
}

/// Builds a delegate backed by its own client, which starts out knowing only the genesis state
/// and has to catch up with the validators the way a real one would.
async fn make_delegate<B: StorageBuilder>(
    builder: &mut TestBuilder<B>,
    chain_id: ChainId,
) -> anyhow::Result<Arc<LocalProposerDelegate<TestEnv<B::Storage>>>> {
    let host = builder
        .make_client(chain_id, None, BlockHeight::ZERO)
        .await?;
    Ok(Arc::new(LocalProposerDelegate::new(
        host.client().clone(),
        "delegate",
    )))
}

/// A delegate that answers whatever it was built with, and counts the calls it was asked to make.
#[derive(Debug)]
struct StubDelegate {
    answer: StubAnswer,
    calls: AtomicUsize,
}

#[derive(Debug)]
enum StubAnswer {
    /// Hand the block straight back, as a delegate does when the round has moved on.
    NeedsOwner(Box<ChainInfo>),
    /// Return a certificate for some other block than the one that was proposed.
    WrongBlock(Box<ConfirmedBlockCertificate>),
    /// Fail outright, as an unreachable delegate does.
    Unreachable,
}

impl StubDelegate {
    fn new(answer: StubAnswer) -> Arc<Self> {
        Arc::new(Self {
            answer,
            calls: AtomicUsize::new(0),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn answer(&self) -> Result<DelegatedOutcome, NodeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match &self.answer {
            StubAnswer::NeedsOwner(info) => Ok(DelegatedOutcome::NeedsOwner(info.clone())),
            StubAnswer::WrongBlock(certificate) => {
                Ok(DelegatedOutcome::Confirmed(certificate.clone()))
            }
            StubAnswer::Unreachable => Err(NodeError::ClientIoError {
                error: "delegate is unreachable".to_string(),
            }),
        }
    }
}

impl ProposerDelegate for StubDelegate {
    fn address(&self) -> String {
        "stub".to_string()
    }

    fn submit_and_confirm<'a>(
        &'a self,
        _proposal: BlockProposal,
        _block: Block,
        _blobs: Vec<Blob>,
        _delivery: CrossChainMessageDelivery,
    ) -> BoxFuture<'a, Result<DelegatedOutcome, NodeError>> {
        Box::pin(async move { self.answer() })
    }

    fn finalize<'a>(
        &'a self,
        _certificate: ValidatedBlockCertificate,
        _delivery: CrossChainMessageDelivery,
    ) -> BoxFuture<'a, Result<DelegatedOutcome, NodeError>> {
        Box::pin(async move { self.answer() })
    }
}

/// A delegate carries a proposal through both rounds and the block commits, with the client
/// never talking to a validator about it.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[test_log::test(tokio::test)]
async fn test_delegate_commits_a_block<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let mut sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    *sender.options_mut() = delegating_options();
    let recipient = builder.add_root_chain(2, Amount::ZERO).await?;
    let delegate = make_delegate(&mut builder, sender.chain_id()).await?;
    sender.client().set_proposer_delegate(Some(delegate));

    let certificate = sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(recipient.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    // The default test options skip the fast round, so this went through the two-round path:
    // the delegate collected the validation votes and then closed the round itself.
    assert!(!certificate.round().is_fast());
    assert_eq!(certificate.block().header.height, BlockHeight(0));
    assert_eq!(sender.local_balance().await?, Amount::from_tokens(3));
    // Everyone heard about it, so the delegate's broadcast stood in for our own.
    for index in 0..4 {
        assert_eq!(
            builder.next_block_height(index, sender.chain_id()).await,
            BlockHeight(1)
        );
    }
    Ok(())
}

/// The same, in the fast round, where validators vote to confirm directly and there is no
/// validated certificate to finalize.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[test_log::test(tokio::test)]
async fn test_delegate_commits_a_fast_block<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let mut sender = builder
        .add_root_chain_with_ownership(
            1,
            Amount::from_tokens(4),
            linera_base::ownership::ChainOwnership::single_super,
        )
        .await?;
    *sender.options_mut() = chain_client::Options {
        allow_fast_blocks: true,
        ..delegating_options()
    };
    let recipient = builder.add_root_chain(2, Amount::ZERO).await?;
    let delegate = make_delegate(&mut builder, sender.chain_id()).await?;
    sender.client().set_proposer_delegate(Some(delegate));

    let certificate = sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(recipient.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    assert!(certificate.round().is_fast());
    assert_eq!(sender.local_balance().await?, Amount::from_tokens(3));
    Ok(())
}

/// A delegate takes a lock left behind by an interrupted attempt straight to a confirmation
/// certificate, which is the entry point a client resuming work has to use.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[test_log::test(tokio::test)]
async fn test_delegate_finalizes_a_lock<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let mut sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    *sender.options_mut() = delegating_options();
    let recipient = builder.add_root_chain(2, Amount::ZERO).await?;

    // Two of four validators withhold their confirmation vote, so the block gets validated but
    // never confirmed and the attempt leaves a lock behind.
    builder.set_fault_type([2, 3], FaultType::DontSendConfirmVote);
    let interrupted = sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(recipient.chain_id()),
        )
        .await;
    assert!(interrupted.is_err());
    let manager = sender.chain_info_with_manager_values().await?.manager;
    let locking = *manager
        .requested_locking
        .expect("the interrupted attempt should have left a lock");
    assert!(matches!(locking, LockingBlock::Regular(_)));

    // With the validators answering again, the delegate closes the round on our behalf. We
    // never form the confirmation certificate ourselves.
    builder.set_fault_type([2, 3], FaultType::Honest);
    let delegate = make_delegate(&mut builder, sender.chain_id()).await?;
    sender.client().set_proposer_delegate(Some(delegate));

    let certificate = sender
        .process_pending_block()
        .await
        .unwrap_ok_committed()
        .expect("the locking block should have been finalized");
    assert_eq!(certificate.block().header.height, BlockHeight(0));
    assert_eq!(sender.local_balance().await?, Amount::from_tokens(3));
    Ok(())
}

/// A delegate that hands the block back costs us a fallback, not the block.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[test_log::test(tokio::test)]
async fn test_client_falls_back_when_the_delegate_hands_back<B>(
    storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let mut sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    *sender.options_mut() = delegating_options();
    let recipient = builder.add_root_chain(2, Amount::ZERO).await?;

    let info = sender.chain_info_with_manager_values().await?;
    let stub = StubDelegate::new(StubAnswer::NeedsOwner(info));
    sender.client().set_proposer_delegate(Some(stub.clone()));

    let certificate = sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(recipient.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    assert!(stub.calls() > 0, "the delegate should have been asked");
    assert_eq!(certificate.block().header.height, BlockHeight(0));
    assert_eq!(sender.local_balance().await?, Amount::from_tokens(3));
    Ok(())
}

/// An unreachable delegate costs us a fallback, not the block.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[test_log::test(tokio::test)]
async fn test_client_falls_back_when_the_delegate_fails<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let mut sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    *sender.options_mut() = delegating_options();
    let recipient = builder.add_root_chain(2, Amount::ZERO).await?;

    let stub = StubDelegate::new(StubAnswer::Unreachable);
    sender.client().set_proposer_delegate(Some(stub.clone()));

    let certificate = sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(recipient.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    assert!(stub.calls() > 0, "the delegate should have been asked");
    assert_eq!(certificate.block().header.height, BlockHeight(0));
    assert_eq!(sender.local_balance().await?, Amount::from_tokens(3));
    Ok(())
}

/// A delegate that returns a real certificate for the wrong block is refused, and the block we
/// actually proposed is the one that commits.
///
/// The certificate here is genuine and carries a real quorum, so only the check against the
/// block we proposed can catch it. That is the check that makes delegation safe to enable
/// without trusting whoever runs the delegate.
#[test_case(MemoryStorageBuilder::default(); "memory")]
#[test_log::test(tokio::test)]
async fn test_client_refuses_a_certificate_for_a_different_block<B>(
    storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let signer = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, signer).await?;
    let mut sender = builder.add_root_chain(1, Amount::from_tokens(4)).await?;
    *sender.options_mut() = delegating_options();
    let recipient = builder.add_root_chain(2, Amount::ZERO).await?;

    // Commit one block the honest way, to have a real certificate for the wrong block.
    let first = sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(recipient.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    let stub = StubDelegate::new(StubAnswer::WrongBlock(Box::new(first.clone())));
    sender.client().set_proposer_delegate(Some(stub.clone()));

    let second = sender
        .transfer_to_account(
            AccountOwner::CHAIN,
            Amount::from_tokens(1),
            Account::chain(recipient.chain_id()),
        )
        .await
        .unwrap_ok_committed();

    assert!(stub.calls() > 0, "the delegate should have been asked");
    assert_ne!(second.hash(), first.hash());
    assert_eq!(second.block().header.height, BlockHeight(1));
    assert_eq!(sender.local_balance().await?, Amount::from_tokens(2));
    Ok(())
}
