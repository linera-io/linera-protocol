// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use std::{
    collections::{BTreeMap, BTreeSet},
    vec,
};

use assert_matches::assert_matches;
use linera_base::{
    crypto::CryptoHash,
    data_types::{
        Amount, ApplicationPermissions, Blob, BlockHeight, CompressedBytecode, OracleResponse,
        Timestamp, UserApplicationDescription,
    },
    http,
    identifiers::{
        Account, AccountOwner, ApplicationId, ChainDescription, ChainId, ModuleId, Owner,
    },
    ownership::ChainOwnership,
    vm::VmRuntime,
};
use linera_execution::{
    test_utils::{
        blob_oracle_responses, create_dummy_message_context, create_dummy_operation_context,
        test_accounts_strategy, ExpectedCall, RegisterMockApplication, SystemExecutionState,
    },
    BaseRuntime, ContractRuntime, ExecutionError, Message, MessageContext, Operation,
    OperationContext, ResourceController, SystemExecutionStateView, TestExecutionRuntimeContext,
    TransactionOutcome, TransactionTracker,
};
use linera_views::context::MemoryContext;
use test_case::{test_case, test_matrix};
use test_strategy::proptest;

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

    let contract_blob = TransferTestEndpoint::sender_application_contract_blob();
    let service_blob = TransferTestEndpoint::sender_application_service_blob();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let application_description = TransferTestEndpoint::sender_application_description();
    let application_description_blob = Blob::new_application_description(&application_description);
    let app_desc_blob_id = application_description_blob.id();

    let (application_id, application) = view
        .register_mock_application_with(application_description, contract_blob, service_blob)
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
    let mut tracker = TransactionTracker::new(
        0,
        0,
        Some(vec![
            OracleResponse::Blob(app_desc_blob_id),
            OracleResponse::Blob(contract_blob_id),
            OracleResponse::Blob(service_blob_id),
        ]),
    );
    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut tracker,
        &mut controller,
    )
    .await?;

    let TransactionOutcome {
        outgoing_messages,
        oracle_responses,
        next_message_index,
        ..
    } = tracker.into_outcome()?;
    assert_eq!(outgoing_messages.len(), 1);
    assert_eq!(oracle_responses.len(), 3);
    assert_eq!(next_message_index, 1);
    assert!(matches!(outgoing_messages[0].message, Message::System(_)));

    view.execute_message(
        create_dummy_message_context(None),
        Timestamp::from(0),
        outgoing_messages[0].message.clone(),
        None,
        &mut TransactionTracker::new(0, 0, Some(Vec::new())),
        &mut controller,
    )
    .await?;

    recipient.verify_recipient(&view.system, amount).await?;

    Ok(())
}

/// Tests the contract system API to transfer tokens between accounts.
#[test_matrix(
    [TransferTestEndpoint::Chain, TransferTestEndpoint::User, TransferTestEndpoint::Application],
    [TransferTestEndpoint::Chain, TransferTestEndpoint::User, TransferTestEndpoint::Application]
)]
#[test_log::test(tokio::test)]
async fn test_unauthorized_transfer_system_api(
    sender: TransferTestEndpoint,
    recipient: TransferTestEndpoint,
) -> anyhow::Result<()> {
    let amount = Amount::ONE;

    let mut view = sender.create_system_state(amount).into_view().await;

    let contract_blob = TransferTestEndpoint::sender_application_contract_blob();
    let service_blob = TransferTestEndpoint::sender_application_service_blob();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let application_description = TransferTestEndpoint::sender_application_description();
    let application_description_blob = Blob::new_application_description(&application_description);
    let app_desc_blob_id = application_description_blob.id();

    let (application_id, application) = view
        .register_mock_application_with(application_description, contract_blob, service_blob)
        .await?;

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, context, _operation| {
            runtime.transfer(
                sender.unauthorized_sender_account_owner(),
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
        authenticated_signer: sender.unauthorized_signer(),
        ..create_dummy_operation_context()
    };
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let result = view
        .execute_operation(
            context,
            Timestamp::from(0),
            operation,
            &mut TransactionTracker::new(
                0,
                0,
                Some(vec![
                    OracleResponse::Blob(app_desc_blob_id),
                    OracleResponse::Blob(contract_blob_id),
                    OracleResponse::Blob(service_blob_id),
                ]),
            ),
            &mut controller,
        )
        .await;

    assert_matches!(result, Err(ExecutionError::UnauthenticatedTransferOwner));

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

    let contract_blob = TransferTestEndpoint::sender_application_contract_blob();
    let service_blob = TransferTestEndpoint::sender_application_service_blob();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let application_description = TransferTestEndpoint::sender_application_description();
    let application_description_blob = Blob::new_application_description(&application_description);
    let app_desc_blob_id = application_description_blob.id();

    let (application_id, application) = claimer_view
        .register_mock_application_with(application_description, contract_blob, service_blob)
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
    let mut tracker = TransactionTracker::new(
        0,
        0,
        Some(vec![
            OracleResponse::Blob(app_desc_blob_id),
            OracleResponse::Blob(contract_blob_id),
            OracleResponse::Blob(service_blob_id),
        ]),
    );
    claimer_view
        .execute_operation(
            context,
            Timestamp::from(0),
            operation,
            &mut tracker,
            &mut controller,
        )
        .await?;

    let TransactionOutcome {
        outgoing_messages,
        oracle_responses,
        next_message_index,
        ..
    } = tracker.into_outcome()?;
    assert_eq!(outgoing_messages.len(), 1);
    assert_eq!(oracle_responses.len(), 3);
    assert_eq!(next_message_index, 1);
    assert!(matches!(outgoing_messages[0].message, Message::System(_)));

    let mut tracker = TransactionTracker::new(0, 0, Some(Vec::new()));
    source_view
        .execute_message(
            create_dummy_message_context(None),
            Timestamp::from(0),
            outgoing_messages[0].message.clone(),
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

    let TransactionOutcome {
        outgoing_messages,
        oracle_responses,
        next_message_index,
        ..
    } = tracker.into_outcome()?;
    assert_eq!(outgoing_messages.len(), 1);
    assert!(oracle_responses.is_empty());
    assert_eq!(next_message_index, 1);
    assert!(matches!(outgoing_messages[0].message, Message::System(_)));

    let mut tracker = TransactionTracker::new(0, 0, Some(Vec::new()));
    let context = MessageContext {
        chain_id: claimer_chain_id,
        ..create_dummy_message_context(None)
    };
    claimer_view
        .execute_message(
            context,
            Timestamp::from(0),
            outgoing_messages[0].message.clone(),
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

/// Tests the contract system API to claim tokens from an unauthorized remote account.
#[test_matrix(
    [TransferTestEndpoint::User, TransferTestEndpoint::Application],
    [TransferTestEndpoint::Chain, TransferTestEndpoint::User, TransferTestEndpoint::Application]
)]
#[test_log::test(tokio::test)]
async fn test_unauthorized_claims(
    sender: TransferTestEndpoint,
    recipient: TransferTestEndpoint,
) -> anyhow::Result<()> {
    let amount = Amount::ONE;

    let claimer_chain_description = ChainDescription::Root(1);

    let claimer_state = SystemExecutionState {
        description: Some(claimer_chain_description),
        ..SystemExecutionState::default()
    };

    let source_chain_id = ChainId::root(0);
    let claimer_chain_id = ChainId::from(claimer_chain_description);

    let mut claimer_view = claimer_state.into_view().await;

    let contract_blob = TransferTestEndpoint::sender_application_contract_blob();
    let service_blob = TransferTestEndpoint::sender_application_service_blob();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let application_description = TransferTestEndpoint::sender_application_description();
    let application_description_blob = Blob::new_application_description(&application_description);
    let app_desc_blob_id = application_description_blob.id();

    let (application_id, application) = claimer_view
        .register_mock_application_with(application_description, contract_blob, service_blob)
        .await?;

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, context, _operation| {
            runtime.claim(
                Account {
                    owner: sender.unauthorized_sender_account_owner(),
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
        authenticated_signer: sender.unauthorized_signer(),
        chain_id: claimer_chain_id,
        ..create_dummy_operation_context()
    };
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let mut tracker = TransactionTracker::new(
        0,
        0,
        Some(vec![
            OracleResponse::Blob(app_desc_blob_id),
            OracleResponse::Blob(contract_blob_id),
            OracleResponse::Blob(service_blob_id),
        ]),
    );
    let result = claimer_view
        .execute_operation(
            context,
            Timestamp::from(0),
            operation,
            &mut tracker,
            &mut controller,
        )
        .await;

    assert_matches!(result, Err(ExecutionError::UnauthenticatedClaimOwner));

    Ok(())
}

/// Tests the contract system API to read the chain balance.
#[proptest(async = "tokio")]
async fn test_read_chain_balance_system_api(chain_balance: Amount) {
    let mut view = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        balance: chain_balance,
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    let contract_blob = TransferTestEndpoint::sender_application_contract_blob();
    let service_blob = TransferTestEndpoint::sender_application_service_blob();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let application_description = TransferTestEndpoint::sender_application_description();
    let application_description_blob = Blob::new_application_description(&application_description);
    let app_desc_blob_id = application_description_blob.id();

    let (application_id, application) = view
        .register_mock_application_with(application_description, contract_blob, service_blob)
        .await
        .unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            assert_eq!(runtime.read_chain_balance().unwrap(), chain_balance);
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context();
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut TransactionTracker::new(
            0,
            0,
            Some(vec![
                OracleResponse::Blob(app_desc_blob_id),
                OracleResponse::Blob(contract_blob_id),
                OracleResponse::Blob(service_blob_id),
            ]),
        ),
        &mut controller,
    )
    .await
    .unwrap();
}

/// Tests the contract system API to read a single account balance.
#[proptest(async = "tokio")]
async fn test_read_owner_balance_system_api(
    #[strategy(test_accounts_strategy())] accounts: BTreeMap<AccountOwner, Amount>,
) {
    let mut view = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        balances: accounts.clone(),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    let (application_id, application, blobs) = view.register_mock_application(0).await.unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            for (owner, balance) in accounts {
                assert_eq!(runtime.read_owner_balance(owner).unwrap(), balance);
            }
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context();
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut TransactionTracker::new(0, 0, Some(blob_oracle_responses(blobs.iter()))),
        &mut controller,
    )
    .await
    .unwrap();
}

/// Tests if reading the balance of a missing account returns zero.
#[proptest(async = "tokio")]
async fn test_read_owner_balance_returns_zero_for_missing_accounts(missing_account: AccountOwner) {
    let mut view = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    let (application_id, application, blobs) = view.register_mock_application(0).await.unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            assert_eq!(
                runtime.read_owner_balance(missing_account).unwrap(),
                Amount::ZERO
            );
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context();
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut TransactionTracker::new(0, 0, Some(blob_oracle_responses(blobs.iter()))),
        &mut controller,
    )
    .await
    .unwrap();
}

/// Tests the contract system API to read all account balances.
#[proptest(async = "tokio")]
async fn test_read_owner_balances_system_api(
    #[strategy(test_accounts_strategy())] accounts: BTreeMap<AccountOwner, Amount>,
) {
    let mut view = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        balances: accounts.clone(),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    let (application_id, application, blobs) = view.register_mock_application(0).await.unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            assert_eq!(
                runtime.read_owner_balances().unwrap(),
                accounts.into_iter().collect::<Vec<_>>()
            );
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context();
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut TransactionTracker::new(0, 0, Some(blob_oracle_responses(blobs.iter()))),
        &mut controller,
    )
    .await
    .unwrap();
}

/// Tests the contract system API to read all account owners.
#[proptest(async = "tokio")]
async fn test_read_balance_owners_system_api(
    #[strategy(test_accounts_strategy())] accounts: BTreeMap<AccountOwner, Amount>,
) {
    let mut view = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        balances: accounts.clone(),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    let (application_id, application, blobs) = view.register_mock_application(0).await.unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            assert_eq!(
                runtime.read_balance_owners().unwrap(),
                accounts.keys().copied().collect::<Vec<_>>()
            );
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context();
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut TransactionTracker::new(0, 0, Some(blob_oracle_responses(blobs.iter()))),
        &mut controller,
    )
    .await
    .unwrap();
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

    /// Returns the [`UserApplicationDescription`] used to represent a sender that's an application.
    fn sender_application_description() -> UserApplicationDescription {
        let contract_id = Self::sender_application_contract_blob().id().hash;
        let service_id = Self::sender_application_service_blob().id().hash;
        let vm_runtime = VmRuntime::Wasm;

        UserApplicationDescription {
            module_id: ModuleId::new(contract_id, service_id, vm_runtime),
            creator_chain_id: ChainId::root(1000),
            block_height: BlockHeight(0),
            application_index: 0,
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
        ApplicationId::new(CryptoHash::test_hash("recipient application description"))
    }

    /// Returns a [`SystemExecutionState`] initialized with this transfer endpoint's account
    /// having `transfer_amount` tokens.
    ///
    /// The state is also configured so that authentication will succeed when this endpoint is used
    /// as a sender.
    pub fn create_system_state(&self, transfer_amount: Amount) -> SystemExecutionState {
        let (balance, balances, owner) = match self {
            TransferTestEndpoint::Chain => (transfer_amount, vec![], Some(Self::sender_owner())),
            TransferTestEndpoint::User => {
                let owner = Self::sender_owner();
                (
                    Amount::ZERO,
                    vec![(AccountOwner::User(owner), transfer_amount)],
                    Some(owner),
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
            super_owners: BTreeSet::from_iter(owner),
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

    /// Returns the [`AccountOwner`] to represent this transfer endpoint as an unauthorized sender.
    pub fn unauthorized_sender_account_owner(&self) -> Option<AccountOwner> {
        match self {
            TransferTestEndpoint::Chain => None,
            TransferTestEndpoint::User => {
                Some(AccountOwner::User(Owner(CryptoHash::test_hash("attacker"))))
            }
            TransferTestEndpoint::Application => {
                Some(AccountOwner::Application(Self::recipient_application_id()))
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

    /// Returns the [`Owner`] that should be used as the authenticated signer when testing an
    /// unauthorized transfer operation.
    pub fn unauthorized_signer(&self) -> Option<Owner> {
        match self {
            TransferTestEndpoint::Chain | TransferTestEndpoint::User => {
                Some(Self::recipient_owner())
            }
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

        let balances = system.balances.index_values().await?;

        assert_eq!(*system.balance.get(), expected_chain_balance);
        assert_eq!(balances, expected_balances);

        Ok(())
    }
}

/// Tests the contract system API to query an application service.
#[cfg(feature = "unstable-oracles")] // # TODO: Remove once #3524 lands
#[test_case(None => matches Ok(_); "when all authorized")]
#[test_case(Some(vec![()]) => matches Ok(_); "when single app authorized")]
#[test_case(Some(vec![]) => matches Err(ExecutionError::UnauthorizedApplication(_)); "when unauthorized")]
#[test_log::test(tokio::test)]
async fn test_query_service(authorized_apps: Option<Vec<()>>) -> Result<(), ExecutionError> {
    let mut view = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        ownership: ChainOwnership::default(),
        balance: Amount::ONE,
        balances: BTreeMap::new(),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    let contract_blob = TransferTestEndpoint::sender_application_contract_blob();
    let service_blob = TransferTestEndpoint::sender_application_service_blob();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let application_description = TransferTestEndpoint::sender_application_description();
    let application_description_blob = Blob::new_application_description(&application_description);
    let app_desc_blob_id = application_description_blob.id();

    let (application_id, application) = view
        .register_mock_application_with(application_description, contract_blob, service_blob)
        .await
        .expect("should register mock application");

    let call_service_as_oracle =
        authorized_apps.map(|apps| apps.into_iter().map(|()| application_id).collect());

    view.system
        .application_permissions
        .set(ApplicationPermissions {
            call_service_as_oracle,
            ..ApplicationPermissions::new_single(application_id)
        });

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            runtime.query_service(application_id, vec![])?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());
    application.expect_call(ExpectedCall::handle_query(|_service, _context, _query| {
        Ok(vec![])
    }));

    let context = create_dummy_operation_context();
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut TransactionTracker::new(
            0,
            0,
            Some(vec![
                OracleResponse::Blob(app_desc_blob_id),
                OracleResponse::Blob(contract_blob_id),
                OracleResponse::Blob(service_blob_id),
                OracleResponse::Service(vec![]),
            ]),
        ),
        &mut controller,
    )
    .await?;

    Ok(())
}

/// Tests the contract system API to make HTTP requests.
#[test_case(None => matches Ok(_); "when all authorized")]
#[test_case(Some(vec![()]) => matches Ok(_); "when single app authorized")]
#[test_case(Some(vec![]) => matches Err(ExecutionError::UnauthorizedApplication(_)); "when unauthorized")]
#[test_log::test(tokio::test)]
async fn test_perform_http_request(authorized_apps: Option<Vec<()>>) -> Result<(), ExecutionError> {
    let mut view = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        ownership: ChainOwnership::default(),
        balance: Amount::ONE,
        balances: BTreeMap::new(),
        ..SystemExecutionState::default()
    }
    .into_view()
    .await;

    let contract_blob = TransferTestEndpoint::sender_application_contract_blob();
    let service_blob = TransferTestEndpoint::sender_application_service_blob();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let application_description = TransferTestEndpoint::sender_application_description();
    let application_description_blob = Blob::new_application_description(&application_description);
    let app_desc_blob_id = application_description_blob.id();

    let (application_id, application) = view
        .register_mock_application_with(application_description, contract_blob, service_blob)
        .await
        .expect("should register mock application");

    let make_http_requests =
        authorized_apps.map(|apps| apps.into_iter().map(|()| application_id).collect());

    view.system
        .application_permissions
        .set(ApplicationPermissions {
            make_http_requests,
            ..ApplicationPermissions::new_single(application_id)
        });

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _context, _operation| {
            runtime.perform_http_request(http::Request::get("http://localhost"))?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context();
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        Timestamp::from(0),
        operation,
        &mut TransactionTracker::new(
            0,
            0,
            Some(vec![
                OracleResponse::Blob(app_desc_blob_id),
                OracleResponse::Blob(contract_blob_id),
                OracleResponse::Blob(service_blob_id),
                OracleResponse::Http(http::Response::ok(vec![])),
            ]),
        ),
        &mut controller,
    )
    .await?;

    Ok(())
}
