// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::field_reassign_with_default)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    vec,
};

use assert_matches::assert_matches;
use linera_base::{
    crypto::{AccountPublicKey, CryptoHash},
    data_types::{
        Amount, ApplicationDescription, ApplicationPermissions, Blob, BlockHeight, Bytecode,
        CompressedBytecode, OracleResponse,
    },
    http,
    identifiers::{Account, AccountOwner, ApplicationId, DataBlobHash, ModuleId},
    ownership::ChainOwnership,
    vm::VmRuntime,
};
use linera_execution::{
    test_utils::{
        create_dummy_message_context, create_dummy_operation_context, dummy_chain_description,
        dummy_chain_description_with_ownership_and_balance, test_accounts_strategy, ExpectedCall,
        RegisterMockApplication, SystemExecutionState,
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

    let state = sender.create_system_state(amount);
    let chain_id = state.description.unwrap().id();
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
        move |runtime, _operation| {
            runtime.transfer(
                sender.sender_account_owner(),
                Account {
                    owner: recipient.recipient_account_owner(),
                    chain_id: dummy_chain_description(0).id(),
                },
                amount,
            )?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = OperationContext {
        authenticated_signer: sender.signer(),
        ..create_dummy_operation_context(chain_id)
    };
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let mut tracker = TransactionTracker::new_replaying_blobs([
        app_desc_blob_id,
        contract_blob_id,
        service_blob_id,
    ]);
    view.execute_operation(context, operation, &mut tracker, &mut controller)
        .await?;

    let TransactionOutcome {
        outgoing_messages,
        oracle_responses,
        ..
    } = tracker.into_outcome()?;
    assert_eq!(outgoing_messages.len(), 1);
    assert_eq!(oracle_responses.len(), 3);
    assert!(matches!(outgoing_messages[0].message, Message::System(_)));

    view.execute_message(
        create_dummy_message_context(chain_id, None),
        outgoing_messages[0].message.clone(),
        None,
        &mut TransactionTracker::new_replaying(Vec::new()),
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

    let state = sender.create_system_state(amount);
    let chain_id = state.description.unwrap().id();
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
        move |runtime, _operation| {
            runtime.transfer(
                sender.unauthorized_sender_account_owner(),
                Account {
                    owner: recipient.recipient_account_owner(),
                    chain_id: dummy_chain_description(0).id(),
                },
                amount,
            )?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = OperationContext {
        authenticated_signer: sender.unauthorized_signer(),
        ..create_dummy_operation_context(chain_id)
    };
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let result = view
        .execute_operation(
            context,
            operation,
            &mut TransactionTracker::new_replaying_blobs([
                app_desc_blob_id,
                contract_blob_id,
                service_blob_id,
            ]),
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

    let claimer_chain_description = dummy_chain_description(1);
    let claimer_chain_id = claimer_chain_description.id();

    let source_state = sender.create_system_state(amount);
    let claimer_state = SystemExecutionState {
        description: Some(claimer_chain_description),
        ..SystemExecutionState::default()
    };

    let source_chain_id = source_state
        .description
        .as_ref()
        .expect("System state created by sender should have a `ChainDescription`")
        .id();

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
        move |runtime, _operation| {
            runtime.claim(
                Account {
                    owner: sender.sender_account_owner(),
                    chain_id: source_chain_id,
                },
                Account {
                    owner: recipient.recipient_account_owner(),
                    chain_id: claimer_chain_id,
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
        ..create_dummy_operation_context(claimer_chain_id)
    };
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let mut tracker = TransactionTracker::new_replaying_blobs([
        app_desc_blob_id,
        contract_blob_id,
        service_blob_id,
    ]);
    claimer_view
        .execute_operation(context, operation, &mut tracker, &mut controller)
        .await?;

    let TransactionOutcome {
        outgoing_messages,
        oracle_responses,
        ..
    } = tracker.into_outcome()?;
    assert_eq!(outgoing_messages.len(), 1);
    assert_eq!(oracle_responses.len(), 3);
    assert!(matches!(outgoing_messages[0].message, Message::System(_)));

    let mut tracker = TransactionTracker::new_replaying(Vec::new());
    source_view
        .execute_message(
            create_dummy_message_context(source_chain_id, None),
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
        ..
    } = tracker.into_outcome()?;
    assert_eq!(outgoing_messages.len(), 1);
    assert!(oracle_responses.is_empty());
    assert!(matches!(outgoing_messages[0].message, Message::System(_)));

    let mut tracker = TransactionTracker::new_replaying(Vec::new());
    let context = MessageContext {
        chain_id: claimer_chain_id,
        ..create_dummy_message_context(claimer_chain_id, None)
    };
    claimer_view
        .execute_message(
            context,
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

    let claimer_chain_description = dummy_chain_description(1);
    let claimer_chain_id = claimer_chain_description.id();

    let claimer_state = SystemExecutionState {
        description: Some(claimer_chain_description),
        ..SystemExecutionState::default()
    };

    let source_chain_id = dummy_chain_description(0).id();

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
        move |runtime, _operation| {
            runtime.claim(
                Account {
                    owner: sender.unauthorized_sender_account_owner(),
                    chain_id: source_chain_id,
                },
                Account {
                    owner: recipient.recipient_account_owner(),
                    chain_id: claimer_chain_id,
                },
                amount,
            )?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = OperationContext {
        authenticated_signer: sender.unauthorized_signer(),
        ..create_dummy_operation_context(claimer_chain_id)
    };
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };
    let mut tracker = TransactionTracker::new_replaying_blobs([
        app_desc_blob_id,
        contract_blob_id,
        service_blob_id,
    ]);
    let result = claimer_view
        .execute_operation(context, operation, &mut tracker, &mut controller)
        .await;

    assert_matches!(result, Err(ExecutionError::UnauthenticatedClaimOwner));

    Ok(())
}

/// Tests the contract system API to read the chain balance.
#[proptest(async = "tokio")]
async fn test_read_chain_balance_system_api(chain_balance: Amount) {
    let description = dummy_chain_description(0);
    let chain_id = description.id();
    let mut view = SystemExecutionState {
        balance: chain_balance,
        ..SystemExecutionState::new(description)
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
        move |runtime, _operation| {
            assert_eq!(runtime.read_chain_balance().unwrap(), chain_balance);
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        operation,
        &mut TransactionTracker::new_replaying_blobs([
            app_desc_blob_id,
            contract_blob_id,
            service_blob_id,
        ]),
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
    let description = dummy_chain_description(0);
    let chain_id = description.id();
    let mut view = SystemExecutionState {
        balances: accounts.clone(),
        ..SystemExecutionState::new(description)
    }
    .into_view()
    .await;

    let (application_id, application, blobs) = view.register_mock_application(0).await.unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            for (owner, balance) in accounts {
                assert_eq!(runtime.read_owner_balance(owner).unwrap(), balance);
            }
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        operation,
        &mut TransactionTracker::new_replaying_blobs(blobs),
        &mut controller,
    )
    .await
    .unwrap();
}

/// Tests if reading the balance of a missing account returns zero.
#[proptest(async = "tokio")]
async fn test_read_owner_balance_returns_zero_for_missing_accounts(missing_account: AccountOwner) {
    let description = dummy_chain_description(0);
    let chain_id = description.id();
    let mut view = SystemExecutionState::new(description).into_view().await;

    let (application_id, application, blobs) = view.register_mock_application(0).await.unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            assert_eq!(
                runtime.read_owner_balance(missing_account).unwrap(),
                Amount::ZERO
            );
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        operation,
        &mut TransactionTracker::new_replaying_blobs(blobs),
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
    let description = dummy_chain_description(0);
    let chain_id = description.id();
    let mut view = SystemExecutionState {
        balances: accounts.clone(),
        ..SystemExecutionState::new(description)
    }
    .into_view()
    .await;

    let (application_id, application, blobs) = view.register_mock_application(0).await.unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            assert_eq!(
                runtime.read_owner_balances().unwrap(),
                accounts.into_iter().collect::<Vec<_>>()
            );
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        operation,
        &mut TransactionTracker::new_replaying_blobs(blobs),
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
    let description = dummy_chain_description(0);
    let chain_id = description.id();
    let mut view = SystemExecutionState {
        balances: accounts.clone(),
        ..SystemExecutionState::new(description)
    }
    .into_view()
    .await;

    let (application_id, application, blobs) = view.register_mock_application(0).await.unwrap();

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            assert_eq!(
                runtime.read_balance_owners().unwrap(),
                accounts.keys().copied().collect::<Vec<_>>()
            );
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        operation,
        &mut TransactionTracker::new_replaying_blobs(blobs),
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
    /// Returns the [`AccountOwner`] used to represent a sender that's a user.
    fn sender_owner() -> AccountOwner {
        AccountOwner::from(CryptoHash::test_hash("sender"))
    }

    /// Returns the [`ApplicationId`] used to represent a sender that's an application.
    fn sender_application_id() -> ApplicationId {
        ApplicationId::from(&Self::sender_application_description())
    }

    /// Returns the [`ApplicationDescription`] used to represent a sender that's an application.
    fn sender_application_description() -> ApplicationDescription {
        let contract_id = Self::sender_application_contract_blob().id().hash;
        let service_id = Self::sender_application_service_blob().id().hash;
        let vm_runtime = VmRuntime::Wasm;

        ApplicationDescription {
            module_id: ModuleId::new(contract_id, service_id, vm_runtime),
            creator_chain_id: dummy_chain_description(1000).id(),
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
            compressed_bytes: Arc::new(b"sender contract".to_vec().into_boxed_slice()),
        })
    }

    /// Returns the [`Blob`] that represents the service bytecode used when representing the sender
    /// as an application.
    fn sender_application_service_blob() -> Blob {
        Blob::new_service_bytecode(CompressedBytecode {
            compressed_bytes: Arc::new(b"sender service".to_vec().into_boxed_slice()),
        })
    }

    /// Returns the [`Owner`] used to represent a recipient that's a user.
    fn recipient_owner() -> AccountOwner {
        AccountOwner::from(CryptoHash::test_hash("recipient"))
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
                (Amount::ZERO, vec![(owner, transfer_amount)], Some(owner))
            }
            TransferTestEndpoint::Application => (
                Amount::ZERO,
                vec![(Self::sender_application_id().into(), transfer_amount)],
                None,
            ),
        };

        let ownership = ChainOwnership {
            super_owners: BTreeSet::from_iter(owner),
            ..ChainOwnership::default()
        };

        let chain_description =
            dummy_chain_description_with_ownership_and_balance(0, ownership.clone(), balance);

        SystemExecutionState {
            description: Some(chain_description),
            ownership,
            balance,
            balances: BTreeMap::from_iter(balances),
            ..SystemExecutionState::default()
        }
    }

    /// Returns the [`AccountOwner`] to represent this transfer endpoint as a sender.
    pub fn sender_account_owner(&self) -> AccountOwner {
        match self {
            TransferTestEndpoint::Chain => AccountOwner::CHAIN,
            TransferTestEndpoint::User => Self::sender_owner(),
            TransferTestEndpoint::Application => Self::sender_application_id().into(),
        }
    }

    /// Returns the [`AccountOwner`] to represent this transfer endpoint as an unauthorized sender.
    pub fn unauthorized_sender_account_owner(&self) -> AccountOwner {
        match self {
            TransferTestEndpoint::Chain => AccountOwner::CHAIN,
            TransferTestEndpoint::User => AccountOwner::from(CryptoHash::test_hash("attacker")),
            TransferTestEndpoint::Application => Self::recipient_application_id().into(),
        }
    }

    /// Returns the [`AccountOwner`] that should be used as the authenticated signer in the transfer
    /// operation.
    pub fn signer(&self) -> Option<AccountOwner> {
        match self {
            TransferTestEndpoint::Chain | TransferTestEndpoint::User => Some(Self::sender_owner()),
            TransferTestEndpoint::Application => None,
        }
    }

    /// Returns the [`AccountOwner`] that should be used as the authenticated signer when testing an
    /// unauthorized transfer operation.
    pub fn unauthorized_signer(&self) -> Option<AccountOwner> {
        match self {
            TransferTestEndpoint::Chain | TransferTestEndpoint::User => {
                Some(Self::recipient_owner())
            }
            TransferTestEndpoint::Application => None,
        }
    }

    /// Returns the [`AccountOwner`] to represent this transfer endpoint as a recipient.
    pub fn recipient_account_owner(&self) -> AccountOwner {
        match self {
            TransferTestEndpoint::Chain => AccountOwner::CHAIN,
            TransferTestEndpoint::User => Self::recipient_owner(),
            TransferTestEndpoint::Application => Self::recipient_application_id().into(),
        }
    }

    /// Verifies that the [`SystemExecutionStateView`] has the expected `amount` in this transfer
    /// endpoint's account, and that all other accounts are empty.
    pub async fn verify_recipient(
        &self,
        system: &SystemExecutionStateView<MemoryContext<TestExecutionRuntimeContext>>,
        amount: Amount,
    ) -> anyhow::Result<()> {
        let account_owner = self.recipient_account_owner();
        let (expected_chain_balance, expected_balances) = if account_owner == AccountOwner::CHAIN {
            (amount, vec![])
        } else {
            (Amount::ZERO, vec![(account_owner, amount)])
        };

        let balances = system.balances.index_values().await?;

        assert_eq!(*system.balance.get(), expected_chain_balance);
        assert_eq!(balances, expected_balances);

        Ok(())
    }
}

/// Tests the contract system API to query an application service.
#[test_case(None => matches Ok(_); "when all authorized")]
#[test_case(Some(vec![()]) => matches Ok(_); "when single app authorized")]
#[test_case(Some(vec![]) => matches Err(ExecutionError::UnauthorizedApplication(_)); "when unauthorized")]
#[test_log::test(tokio::test)]
async fn test_query_service(authorized_apps: Option<Vec<()>>) -> Result<(), ExecutionError> {
    let description = dummy_chain_description(0);
    let chain_id = description.id();
    let mut view = SystemExecutionState {
        ownership: ChainOwnership::default(),
        balance: Amount::ONE,
        balances: BTreeMap::new(),
        ..SystemExecutionState::new(description)
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
        move |runtime, _operation| {
            runtime.query_service(application_id, vec![])?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());
    application.expect_call(ExpectedCall::handle_query(|_service, _query| Ok(vec![])));

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        operation,
        &mut TransactionTracker::new_replaying(vec![
            OracleResponse::Blob(app_desc_blob_id),
            OracleResponse::Blob(contract_blob_id),
            OracleResponse::Blob(service_blob_id),
            OracleResponse::Service(vec![]),
        ]),
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
    let description = dummy_chain_description(0);
    let chain_id = description.id();
    let mut view = SystemExecutionState {
        ownership: ChainOwnership::default(),
        balance: Amount::ONE,
        balances: BTreeMap::new(),
        ..SystemExecutionState::new(description)
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
        move |runtime, _operation| {
            runtime.perform_http_request(http::Request::get("http://localhost"))?;
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    view.execute_operation(
        context,
        operation,
        &mut TransactionTracker::new_replaying(vec![
            OracleResponse::Blob(app_desc_blob_id),
            OracleResponse::Blob(contract_blob_id),
            OracleResponse::Blob(service_blob_id),
            OracleResponse::Http(http::Response::ok(vec![])),
        ]),
        &mut controller,
    )
    .await?;

    Ok(())
}

/// Tests creating multiple data blobs in a single transaction.
#[test_log::test(tokio::test)]
async fn test_create_multiple_data_blobs() -> anyhow::Result<()> {
    let description = dummy_chain_description(0);
    let chain_id = description.id();
    let mut view = SystemExecutionState::new(description).into_view().await;
    let (application_id, application, blobs) = view.register_mock_application(0).await.unwrap();

    let test_data1 = b"First blob data";
    let test_data2 = &[];
    let expected_blob1 = Blob::new_data(test_data1.to_vec());
    let expected_blob2 = Blob::new_data(test_data2.to_vec());
    let expected_blob_hash1 = DataBlobHash(expected_blob1.id().hash);
    let expected_blob_hash2 = DataBlobHash(expected_blob2.id().hash);

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            let blob_hash1 = runtime.create_data_blob(test_data1.to_vec()).unwrap();
            let blob_hash2 = runtime.create_data_blob(test_data2.to_vec()).unwrap();

            assert_eq!(blob_hash1, expected_blob_hash1);
            assert_eq!(blob_hash2, expected_blob_hash2);
            assert_ne!(blob_hash1, blob_hash2); // Should be different blobs

            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    let mut tracker = TransactionTracker::new_replaying_blobs(blobs);
    let result = view
        .execute_operation(context, operation, &mut tracker, &mut controller)
        .await;

    assert!(result.is_ok());

    // Verify both blobs were created and tracked
    let created_blobs = tracker.created_blobs();
    assert!(created_blobs.contains_key(&expected_blob1.id()));
    assert!(created_blobs.contains_key(&expected_blob2.id()));

    let created_blob1 = created_blobs.get(&expected_blob1.id()).unwrap();
    let created_blob2 = created_blobs.get(&expected_blob2.id()).unwrap();
    assert_eq!(created_blob1.bytes(), test_data1);
    assert_eq!(created_blob2.bytes(), test_data2);

    Ok(())
}

/// Tests that publish_module with different bytecode creates different modules.
#[test_log::test(tokio::test)]
async fn test_publish_module_different_bytecode() -> anyhow::Result<()> {
    let description = dummy_chain_description(0);
    let chain_id = description.id();
    let mut view = SystemExecutionState::new(description).into_view().await;
    let (application_id, application, blobs) = view.register_mock_application(0).await.unwrap();

    let contract_bytes1 = b"contract bytecode 1".to_vec();
    let service_bytes1 = b"service bytecode 1".to_vec();
    let contract_bytes2 = b"contract bytecode 2".to_vec();
    let service_bytes2 = b"service bytecode 2".to_vec();

    let contract_bytecode1 = Bytecode::new(contract_bytes1);
    let service_bytecode1 = Bytecode::new(service_bytes1);
    let contract_bytecode2 = Bytecode::new(contract_bytes2);
    let service_bytecode2 = Bytecode::new(service_bytes2);
    let vm_runtime = VmRuntime::Wasm;

    application.expect_call(ExpectedCall::execute_operation(
        move |runtime, _operation| {
            let module_id1 = runtime
                .publish_module(contract_bytecode1, service_bytecode1, vm_runtime)
                .unwrap();
            let module_id2 = runtime
                .publish_module(contract_bytecode2, service_bytecode2, vm_runtime)
                .unwrap();

            // Different bytecode should produce different module IDs
            assert_ne!(module_id1, module_id2);
            Ok(vec![])
        },
    ));
    application.expect_call(ExpectedCall::default_finalize());

    let context = create_dummy_operation_context(chain_id);
    let mut controller = ResourceController::default();
    let operation = Operation::User {
        application_id,
        bytes: vec![],
    };

    let mut tracker = TransactionTracker::new_replaying_blobs(blobs);
    let result = view
        .execute_operation(context, operation, &mut tracker, &mut controller)
        .await;

    assert!(result.is_ok());

    // Should have created 4 blobs total (2 contract + 2 service for WASM)
    let created_blobs = tracker.created_blobs();
    assert_eq!(created_blobs.len(), 4);

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_callee_api_calls() -> anyhow::Result<()> {
    let (state, chain_id) = SystemExecutionState::dummy_chain_state(0);
    let mut view = state.into_view().await;

    let (caller_id, caller_application, caller_blobs) = view.register_mock_application(0).await?;
    let (target_id, target_application, target_blobs) = view.register_mock_application(1).await?;

    let owner = AccountOwner::from(AccountPublicKey::test_key(0));
    let dummy_operation = vec![1];

    caller_application.expect_call({
        let dummy_operation = dummy_operation.clone();
        ExpectedCall::execute_operation(move |runtime, operation| {
            assert_eq!(operation, dummy_operation);

            // Call the target application with authentication.
            let response =
                runtime.try_call_application(/* authenticated */ true, target_id, vec![])?;
            assert!(response.is_empty());

            // Call the target application without authentication.
            let response =
                runtime.try_call_application(/* authenticated */ false, target_id, vec![])?;
            assert!(response.is_empty());

            Ok(vec![])
        })
    });

    target_application.expect_call(ExpectedCall::execute_operation(move |runtime, argument| {
        assert!(argument.is_empty());
        assert_eq!(runtime.authenticated_signer().unwrap(), Some(owner));
        assert_eq!(runtime.authenticated_caller_id().unwrap(), Some(caller_id));
        Ok(vec![])
    }));
    target_application.expect_call(ExpectedCall::execute_operation(move |runtime, argument| {
        assert!(argument.is_empty());
        assert_eq!(runtime.authenticated_signer().unwrap(), None);
        assert_eq!(runtime.authenticated_caller_id().unwrap(), None);
        Ok(vec![])
    }));

    target_application.expect_call(ExpectedCall::default_finalize());
    caller_application.expect_call(ExpectedCall::default_finalize());

    let context = OperationContext {
        authenticated_signer: Some(owner),
        ..create_dummy_operation_context(chain_id)
    };
    let mut controller = ResourceController::default();
    let mut txn_tracker =
        TransactionTracker::new_replaying_blobs(caller_blobs.iter().chain(&target_blobs));
    view.execute_operation(
        context,
        Operation::User {
            application_id: caller_id,
            bytes: dummy_operation.clone(),
        },
        &mut txn_tracker,
        &mut controller,
    )
    .await
    .unwrap();
    let txn_outcome = txn_tracker.into_outcome().unwrap();
    assert!(txn_outcome.outgoing_messages.is_empty());
    Ok(())
}
