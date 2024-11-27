// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use std::{collections::BTreeMap, vec};

use anyhow::bail;
use linera_base::{
    crypto::{CryptoHash, PublicKey},
    data_types::{
        Amount, Blob, BlockHeight, CompressedBytecode, Timestamp, UserApplicationDescription,
    },
    identifiers::{
        Account, AccountOwner, ApplicationId, BytecodeId, ChainDescription, ChainId, MessageId,
        Owner,
    },
    ownership::ChainOwnership,
};
use linera_execution::{
    test_utils::{
        create_dummy_message_context, create_dummy_operation_context, ExpectedCall,
        RegisterMockApplication, SystemExecutionState,
    },
    ContractRuntime, ExecutionOutcome, Message, MessageContext, Operation, OperationContext,
    ResourceController, SystemExecutionStateView, TestExecutionRuntimeContext, TransactionTracker,
};
use linera_views::context::MemoryContext;
use test_case::test_matrix;

/// Tests the contract system API to transfer tokens between accounts.
#[test_matrix(
    [TransferTestEndpoint::Chain, TransferTestEndpoint::User, TransferTestEndpoint::Application],
    [TransferTestEndpoint::Chain, TransferTestEndpoint::User, TransferTestEndpoint::Application]
)]
#[test_log::test(tokio::test)]
async fn test_transfer_system_api(
    sender: TransferTestEndpoint,
    recipient: TransferTestEndpoint,
) -> anyhow::Result<()> {
    let amount = Amount::ONE;

    let mut view = sender.create_system_state(amount).into_view().await;

    let (application_id, application) = view
        .register_mock_application_with(
            TransferTestEndpoint::sender_application_description(),
            TransferTestEndpoint::sender_application_contract_blob(),
            TransferTestEndpoint::sender_application_service_blob(),
        )
        .await?;

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, context, _operation| {
            runtime.transfer(
                sender.sender_account_owner(),
                Account {
                    owner: recipient.recipient_account_owner(),
                    chain_id: context.chain_id,
                },
                amount,
            )?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = OperationContext {
        authenticated_signer: sender.signer(),
        ..create_dummy_operation_context()
    };
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let mut tracker = TransactionTracker::new(0, Some(Vec::new()));
    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut tracker,
        &mut controller,
    )
    .await?;

    let (outcomes, oracle_responses, next_message_index) = tracker.destructure()?;
    assert_eq!(outcomes.len(), 3);
    assert!(oracle_responses.is_empty());
    assert_eq!(next_message_index, 1);

    let ExecutionOutcome::System(ref outcome) = outcomes[0] else {
        bail!("Missing system outcome with expected credit message");
    };

    assert_eq!(outcome.messages.len(), 1);

    view.execute_message(
        create_dummy_message_context(None),
        Timestamp::from(0),
        Message::System(outcome.messages[0].message.clone()),
        None,
        &mut TransactionTracker::new(0, Some(Vec::new())),
        &mut controller,
    )
    .await?;

    recipient.verify_recipient(&view.system, amount).await?;

    Ok(())
}

/// Tests the contract system API to claim tokens from a remote account.
#[test_matrix(
    [TransferTestEndpoint::User, TransferTestEndpoint::Application],
    [TransferTestEndpoint::Chain, TransferTestEndpoint::User, TransferTestEndpoint::Application]
)]
#[test_log::test(tokio::test)]
async fn test_claim_system_api(
    sender: TransferTestEndpoint,
    recipient: TransferTestEndpoint,
) -> anyhow::Result<()> {
    let amount = Amount::ONE;

    let claimer_chain_description = ChainDescription::Root(1);

    let source_state = sender.create_system_state(amount);
    let claimer_state = SystemExecutionState {
        description: Some(claimer_chain_description),
        ..SystemExecutionState::default()
    };

    let source_chain_id = ChainId::from(
        source_state
            .description
            .expect("System state created by sender should have a `ChainDescription`"),
    );
    let claimer_chain_id = ChainId::from(claimer_chain_description);

    let mut source_view = source_state.into_view().await;
    let mut claimer_view = claimer_state.into_view().await;

    let (application_id, application) = claimer_view
        .register_mock_application_with(
            TransferTestEndpoint::sender_application_description(),
            TransferTestEndpoint::sender_application_contract_blob(),
            TransferTestEndpoint::sender_application_service_blob(),
        )
        .await?;

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, context, _operation| {
            runtime.claim(
                Account {
                    owner: sender.sender_account_owner(),
                    chain_id: source_chain_id,
                },
                Account {
                    owner: recipient.recipient_account_owner(),
                    chain_id: context.chain_id,
                },
                amount,
            )?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = OperationContext {
        authenticated_signer: sender.signer(),
        chain_id: claimer_chain_id,
        ..create_dummy_operation_context()
    };
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let mut tracker = TransactionTracker::new(0, Some(Vec::new()));
    claimer_view
        .execute_operation(
            context,
            Timestamp::from(0),
            operation,
            &mut tracker,
            &mut controller,
        )
        .await?;

    let (outcomes, oracle_responses, next_message_index) = tracker.destructure()?;
    assert_eq!(outcomes.len(), 3);
    assert!(oracle_responses.is_empty());
    assert_eq!(next_message_index, 1);

    let ExecutionOutcome::System(ref outcome) = outcomes[0] else {
        bail!("Missing system outcome with expected withdraw message");
    };

    assert_eq!(outcome.messages.len(), 1);

    let mut tracker = TransactionTracker::new(0, Some(Vec::new()));
    source_view
        .execute_message(
            create_dummy_message_context(None),
            Timestamp::from(0),
            Message::System(outcome.messages[0].message.clone()),
            None,
            &mut tracker,
            &mut controller,
        )
        .await?;

    assert_eq!(*source_view.system.balance.get(), Amount::ZERO);
    source_view
        .system
        .balances
        .for_each_index_value(|owner, balance| {
            panic!(
                "No accounts should have tokens after the claim message has been handled, \
                but {owner} has {balance} tokens"
            );
        })
        .await?;

    let (outcomes, oracle_responses, next_message_index) = tracker.destructure()?;
    assert_eq!(outcomes.len(), 1);
    assert!(oracle_responses.is_empty());
    assert_eq!(next_message_index, 1);

    let ExecutionOutcome::System(ref outcome) = outcomes[0] else {
        bail!("Missing system outcome with expected credit message");
    };

    assert_eq!(outcome.messages.len(), 1);

    let mut tracker = TransactionTracker::new(0, Some(Vec::new()));
    let context = MessageContext {
        chain_id: claimer_chain_id,
        ..create_dummy_message_context(None)
    };
    claimer_view
        .execute_message(
            context,
            Timestamp::from(0),
            Message::System(outcome.messages[0].message.clone()),
            None,
            &mut tracker,
            &mut controller,
        )
        .await?;

    recipient
        .verify_recipient(&claimer_view.system, amount)
        .await?;

    Ok(())
}

/// A test helper representing a transfer endpoint.
#[derive(Clone, Copy, Debug)]
enum TransferTestEndpoint {
    Chain,
    User,
    Application,
}

impl TransferTestEndpoint {
    /// Returns the [`Owner`] used to represent a sender that's a user.
    fn sender_owner() -> Owner {
        Owner(CryptoHash::test_hash("sender"))
    }

    /// Returns the [`ApplicationId`] used to represent a sender that's an application.
    fn sender_application_id() -> ApplicationId {
        ApplicationId::from(&Self::sender_application_description())
    }

    /// Returns the [`ApplicationDescription`] used to represent a sender that's an application.
    fn sender_application_description() -> UserApplicationDescription {
        let contract_id = Self::sender_application_contract_blob().id().hash;
        let service_id = Self::sender_application_service_blob().id().hash;

        UserApplicationDescription {
            bytecode_id: BytecodeId::new(contract_id, service_id),
            creation: MessageId {
                chain_id: ChainId::root(1000),
                height: BlockHeight(0),
                index: 0,
            },
            parameters: vec![],
            required_application_ids: vec![],
        }
    }

    /// Returns the [`Blob`] that represents the contract bytecode used when representing the
    /// sender as an application.
    fn sender_application_contract_blob() -> Blob {
        Blob::new_contract_bytecode(CompressedBytecode {
            compressed_bytes: b"sender contract".to_vec(),
        })
    }

    /// Returns the [`Blob`] that represents the service bytecode used when representing the sender
    /// as an application.
    fn sender_application_service_blob() -> Blob {
        Blob::new_service_bytecode(CompressedBytecode {
            compressed_bytes: b"sender service".to_vec(),
        })
    }

    /// Returns the [`Owner`] used to represent a recipient that's a user.
    fn recipient_owner() -> Owner {
        Owner(CryptoHash::test_hash("recipient"))
    }

    /// Returns the [`ApplicationId`] used to represent a recipient that's an application.
    fn recipient_application_id() -> ApplicationId {
        ApplicationId {
            bytecode_id: BytecodeId::new(
                CryptoHash::test_hash("recipient contract bytecode"),
                CryptoHash::test_hash("recipient service bytecode"),
            ),
            creation: MessageId {
                chain_id: ChainId::root(2000),
                height: BlockHeight(0),
                index: 0,
            },
        }
    }

    /// Returns a [`SystemExecutionState`] initialized with this transfer endpoint's account
    /// having `transfer_amount` tokens.
    ///
    /// The state is also configured so that authentication will succeed when this endpoint is used
    /// as a sender.
    pub fn create_system_state(&self, transfer_amount: Amount) -> SystemExecutionState {
        let (balance, balances, owner_pair) = match self {
            TransferTestEndpoint::Chain => (
                transfer_amount,
                vec![],
                Some((Self::sender_owner(), PublicKey::test_key(1))),
            ),
            TransferTestEndpoint::User => {
                let owner = Self::sender_owner();
                (
                    Amount::ZERO,
                    vec![(AccountOwner::User(owner), transfer_amount)],
                    Some((owner, PublicKey::test_key(1))),
                )
            }
            TransferTestEndpoint::Application => (
                Amount::ZERO,
                vec![(
                    AccountOwner::Application(Self::sender_application_id()),
                    transfer_amount,
                )],
                None,
            ),
        };

        let ownership = ChainOwnership {
            super_owners: BTreeMap::from_iter(owner_pair),
            ..ChainOwnership::default()
        };

        SystemExecutionState {
            description: Some(ChainDescription::Root(0)),
            ownership,
            balance,
            balances: BTreeMap::from_iter(balances),
            ..SystemExecutionState::default()
        }
    }

    /// Returns the [`AccountOwner`] to represent this transfer endpoint as a sender.
    pub fn sender_account_owner(&self) -> Option<AccountOwner> {
        match self {
            TransferTestEndpoint::Chain => None,
            TransferTestEndpoint::User => Some(AccountOwner::User(Self::sender_owner())),
            TransferTestEndpoint::Application => {
                Some(AccountOwner::Application(Self::sender_application_id()))
            }
        }
    }

    /// Returns the [`Owner`] that should be used as the authenticated signer in the transfer
    /// operation.
    pub fn signer(&self) -> Option<Owner> {
        match self {
            TransferTestEndpoint::Chain | TransferTestEndpoint::User => Some(Self::sender_owner()),
            TransferTestEndpoint::Application => None,
        }
    }

    /// Returns the [`AccountOwner`] to represent this transfer endpoint as a recipient.
    pub fn recipient_account_owner(&self) -> Option<AccountOwner> {
        match self {
            TransferTestEndpoint::Chain => None,
            TransferTestEndpoint::User => Some(AccountOwner::User(Self::recipient_owner())),
            TransferTestEndpoint::Application => {
                Some(AccountOwner::Application(Self::recipient_application_id()))
            }
        }
    }

    /// Verifies that the [`SystemExecutionStateView`] has the expected `amount` in this transfer
    /// endpoint's account, and that all other accounts are empty.
    pub async fn verify_recipient(
        &self,
        system: &SystemExecutionStateView<MemoryContext<TestExecutionRuntimeContext>>,
        amount: Amount,
    ) -> anyhow::Result<()> {
        let (expected_chain_balance, expected_balances) = match self.recipient_account_owner() {
            None => (amount, vec![]),
            Some(account_owner) => (Amount::ZERO, vec![(account_owner, amount)]),
        };

        let mut balances = Vec::new();

        system
            .balances
            .for_each_index_value(|owner, balance| {
                balances.push((owner, balance));
                Ok(())
            })
            .await?;

        assert_eq!(*system.balance.get(), expected_chain_balance);
        assert_eq!(balances, expected_balances);

        Ok(())
    }
}