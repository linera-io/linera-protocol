// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// Some of these items are only used by some tests, but Rust will complain about unused
// items for the tests where they aren't used
#![allow(unused_imports)]

mod mock_application;
#[cfg(with_revm)]
pub mod solidity;
mod system_execution_state;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    thread, vec,
};

use linera_base::{
    crypto::{BcsSignable, CryptoHash},
    data_types::{Amount, Blob, BlockHeight, CompressedBytecode, OracleResponse, Timestamp},
    identifiers::{
        AccountOwner, ApplicationId, BlobId, BlobType, ChainDescription, ChainId, MessageId,
        ModuleId, Owner,
    },
    ownership::ChainOwnership,
    vm::VmRuntime,
};
use linera_views::{
    context::{Context, MemoryContext},
    views::{View, ViewError},
};
use proptest::{prelude::any, strategy::Strategy};
use serde::{Deserialize, Serialize};

pub use self::{
    mock_application::{ExpectedCall, MockApplication, MockApplicationInstance},
    system_execution_state::SystemExecutionState,
};
use crate::{
    ExecutionRequest, ExecutionRuntimeContext, ExecutionStateView, MessageContext,
    OperationContext, QueryContext, ServiceRuntimeEndpoint, ServiceRuntimeRequest,
    ServiceSyncRuntime, SystemExecutionStateView, TestExecutionRuntimeContext,
    UserApplicationDescription, UserApplicationId,
};

/// Creates a dummy [`UserApplicationDescription`] for use in tests.
pub fn create_dummy_user_application_description(
    index: u32,
) -> (UserApplicationDescription, Blob, Blob) {
    let chain_id = ChainId::root(1);
    let mut contract_bytes = b"contract".to_vec();
    let mut service_bytes = b"service".to_vec();
    contract_bytes.push(index as u8);
    service_bytes.push(index as u8);
    let contract_blob = Blob::new_contract_bytecode(CompressedBytecode {
        compressed_bytes: contract_bytes,
    });
    let service_blob = Blob::new_service_bytecode(CompressedBytecode {
        compressed_bytes: service_bytes,
    });

    let vm_runtime = VmRuntime::Wasm;
    (
        UserApplicationDescription {
            module_id: ModuleId::new(contract_blob.id().hash, service_blob.id().hash, vm_runtime),
            creator_chain_id: chain_id,
            block_height: 0.into(),
            application_index: index,
            required_application_ids: vec![],
            parameters: vec![],
        },
        contract_blob,
        service_blob,
    )
}

/// Creates a dummy [`OperationContext`] to use in tests.
pub fn create_dummy_operation_context() -> OperationContext {
    OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        round: Some(0),
        index: Some(0),
        authenticated_signer: None,
        authenticated_caller_id: None,
    }
}

/// Creates a dummy [`MessageContext`] to use in tests.
pub fn create_dummy_message_context(authenticated_signer: Option<Owner>) -> MessageContext {
    MessageContext {
        chain_id: ChainId::root(0),
        is_bouncing: false,
        authenticated_signer,
        refund_grant_to: None,
        height: BlockHeight(0),
        round: Some(0),
        certificate_hash: CryptoHash::test_hash("block receiving a message"),
        message_id: MessageId {
            chain_id: ChainId::root(0),
            height: BlockHeight(0),
            index: 0,
        },
    }
}

/// Creates a dummy [`QueryContext`] to use in tests.
pub fn create_dummy_query_context() -> QueryContext {
    QueryContext {
        chain_id: ChainId::root(0),
        next_block_height: BlockHeight(0),
        local_time: Timestamp::from(0),
    }
}

/// Registration of [`MockApplication`]s to use in tests.
#[allow(async_fn_in_trait)]
pub trait RegisterMockApplication {
    /// Returns the chain to use for the creation of the application.
    ///
    /// This is included in the mocked [`ApplicationId`].
    fn creator_chain_id(&self) -> ChainId;

    /// Registers a new [`MockApplication`] and returns it with the [`UserApplicationId`] that was
    /// used for it.
    async fn register_mock_application(
        &mut self,
        index: u32,
    ) -> anyhow::Result<(UserApplicationId, MockApplication, [BlobId; 3])> {
        let (description, contract, service) = create_dummy_user_application_description(index);
        let description_blob_id = Blob::new_application_description(&description).id();
        let contract_blob_id = contract.id();
        let service_blob_id = service.id();

        let (app_id, application) = self
            .register_mock_application_with(description, contract, service)
            .await?;
        Ok((
            app_id,
            application,
            [description_blob_id, contract_blob_id, service_blob_id],
        ))
    }

    /// Registers a new [`MockApplication`] associated with a [`UserApplicationDescription`] and
    /// its bytecode [`Blob`]s.
    async fn register_mock_application_with(
        &mut self,
        description: UserApplicationDescription,
        contract: Blob,
        service: Blob,
    ) -> anyhow::Result<(UserApplicationId, MockApplication)>;
}

impl<C> RegisterMockApplication for ExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    fn creator_chain_id(&self) -> ChainId {
        self.system.creator_chain_id()
    }

    async fn register_mock_application_with(
        &mut self,
        description: UserApplicationDescription,
        contract: Blob,
        service: Blob,
    ) -> anyhow::Result<(UserApplicationId, MockApplication)> {
        self.system
            .register_mock_application_with(description, contract, service)
            .await
    }
}

impl<C> RegisterMockApplication for SystemExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    fn creator_chain_id(&self) -> ChainId {
        self.description.get().expect(
            "Can't register applications on a system state with no associated `ChainDescription`",
        ).into()
    }

    async fn register_mock_application_with(
        &mut self,
        description: UserApplicationDescription,
        contract: Blob,
        service: Blob,
    ) -> anyhow::Result<(UserApplicationId, MockApplication)> {
        let id = From::from(&description);
        let extra = self.context().extra();
        let mock_application = MockApplication::default();

        extra
            .user_contracts()
            .insert(id, mock_application.clone().into());
        extra
            .user_services()
            .insert(id, mock_application.clone().into());
        extra
            .add_blobs([
                contract,
                service,
                Blob::new_application_description(&description),
            ])
            .await?;

        Ok((id, mock_application))
    }
}

pub async fn create_dummy_user_application_registrations(
    count: u32,
) -> anyhow::Result<Vec<(UserApplicationId, UserApplicationDescription, Blob, Blob)>> {
    let mut ids = Vec::with_capacity(count as usize);

    for index in 0..count {
        let (description, contract_blob, service_blob) =
            create_dummy_user_application_description(index);
        let id = From::from(&description);

        ids.push((id, description, contract_blob, service_blob));
    }

    Ok(ids)
}

impl QueryContext {
    /// Spawns a thread running the [`ServiceSyncRuntime`] actor.
    ///
    /// Returns the endpoints to communicate with the actor.
    pub fn spawn_service_runtime_actor(self) -> ServiceRuntimeEndpoint {
        let (execution_state_sender, incoming_execution_requests) =
            futures::channel::mpsc::unbounded();
        let (runtime_request_sender, runtime_request_receiver) = std::sync::mpsc::channel();

        thread::spawn(move || {
            ServiceSyncRuntime::new(execution_state_sender, self).run(runtime_request_receiver)
        });

        ServiceRuntimeEndpoint {
            incoming_execution_requests,
            runtime_request_sender,
        }
    }
}

/// Creates a [`Strategy`] for creating a [`BTreeMap`] of [`AccountOwner`]s with an initial
/// non-zero [`Amount`] of tokens.
pub fn test_accounts_strategy() -> impl Strategy<Value = BTreeMap<AccountOwner, Amount>> {
    proptest::collection::btree_map(
        any::<AccountOwner>(),
        (1_u128..).prop_map(Amount::from_tokens),
        0..5,
    )
}

/// Creates a vector of ['OracleResponse`]s for the supplied [`BlobId`]s.
pub fn blob_oracle_responses<'a>(blobs: impl Iterator<Item = &'a BlobId>) -> Vec<OracleResponse> {
    blobs
        .into_iter()
        .copied()
        .map(OracleResponse::Blob)
        .collect()
}

/// A test helper representing a transfer endpoint.
#[derive(Clone, Copy, Debug)]
pub enum TransferTestEndpoint {
    Chain,
    User,
    Application,
}

impl TransferTestEndpoint {
    /// Returns the [`Owner`] used to represent a sender that's a user.
    pub fn sender_owner() -> Owner {
        Owner(CryptoHash::test_hash("sender"))
    }

    /// Returns the [`ApplicationId`] used to represent a sender that's an application.
    pub fn sender_application_id() -> ApplicationId {
        ApplicationId::from(&Self::sender_application_description())
    }

    /// Returns the [`UserApplicationDescription`] used to represent a sender that's an application.
    pub fn sender_application_description() -> UserApplicationDescription {
        let contract_id = Self::sender_application_contract_blob().id().hash;
        let service_id = Self::sender_application_service_blob().id().hash;
        let vm_runtime = VmRuntime::Wasm;

        UserApplicationDescription {
            module_id: ModuleId::new(contract_id, service_id, vm_runtime),
            parameters: vec![],
            required_application_ids: vec![],
            creator_chain_id: ChainId::root(1000),
            block_height: BlockHeight(0),
            application_index: 0,
        }
    }

    /// Returns the [`Blob`] that represents the contract bytecode used when representing the
    /// sender as an application.
    pub fn sender_application_contract_blob() -> Blob {
        Blob::new_contract_bytecode(CompressedBytecode {
            compressed_bytes: b"sender contract".to_vec(),
        })
    }

    /// Returns the [`Blob`] that represents the service bytecode used when representing the sender
    /// as an application.
    pub fn sender_application_service_blob() -> Blob {
        Blob::new_service_bytecode(CompressedBytecode {
            compressed_bytes: b"sender service".to_vec(),
        })
    }

    /// Returns the [`Owner`] used to represent a recipient that's a user.
    pub fn recipient_owner() -> Owner {
        Owner(CryptoHash::test_hash("recipient"))
    }

    /// Returns the [`ApplicationId`] used to represent a recipient that's an application.
    fn recipient_application_id() -> ApplicationId {
        ApplicationId {
            module_id: ModuleId::new(
                CryptoHash::test_hash("recipient contract bytecode"),
                CryptoHash::test_hash("recipient service bytecode"),
                VmRuntime::Wasm,
            ),
            application_description_hash: CryptoHash::test_hash(
                "recipient application description",
            ),
        }
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
