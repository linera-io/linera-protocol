// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Not,
    sync::Arc,
};

use custom_debug_derive::Debug;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, ApplicationPermissions, Blob, ChainDescription, Epoch, Timestamp},
    identifiers::{AccountOwner, ApplicationId, BlobId, ChainId},
    ownership::ChainOwnership,
};
use linera_views::{
    context::{Context, MemoryContext},
    random::generate_test_namespace,
    views::{CryptoHashView, View, ViewError},
};

use super::{dummy_chain_description, MockApplication, RegisterMockApplication};
use crate::{
    committee::Committee, execution::UserAction, ApplicationDescription, ExecutionError,
    ExecutionRuntimeConfig, ExecutionRuntimeContext, ExecutionStateView, OperationContext,
    ResourceControlPolicy, ResourceController, ResourceTracker, TestExecutionRuntimeContext,
    UserContractCode,
};

/// A system execution state, not represented as a view but as a simple struct.
#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub struct SystemExecutionState {
    pub description: Option<ChainDescription>,
    pub epoch: Epoch,
    pub admin_id: Option<ChainId>,
    pub committees: BTreeMap<Epoch, Committee>,
    pub ownership: ChainOwnership,
    pub balance: Amount,
    #[debug(skip_if = BTreeMap::is_empty)]
    pub balances: BTreeMap<AccountOwner, Amount>,
    pub timestamp: Timestamp,
    pub used_blobs: BTreeSet<BlobId>,
    #[debug(skip_if = Not::not)]
    pub closed: bool,
    pub application_permissions: ApplicationPermissions,
    #[debug(skip_if = Vec::is_empty)]
    pub extra_blobs: Vec<Blob>,
    #[debug(skip_if = BTreeMap::is_empty)]
    pub mock_applications: BTreeMap<ApplicationId, MockApplication>,
}

impl SystemExecutionState {
    pub fn new(description: ChainDescription) -> Self {
        let ownership = description.config().ownership.clone();
        let balance = description.config().balance;
        let epoch = description.config().epoch;
        let admin_id = Some(description.config().admin_id.unwrap_or(description.id()));
        let committees = description
            .config()
            .committees
            .iter()
            .map(|(epoch, serialized_committee)| {
                (
                    *epoch,
                    bcs::from_bytes::<Committee>(serialized_committee)
                        .expect("should correctly deserialize a committee"),
                )
            })
            .collect();
        SystemExecutionState {
            epoch,
            description: Some(description),
            admin_id,
            ownership,
            balance,
            committees,
            ..SystemExecutionState::default()
        }
    }

    pub fn dummy_chain_state(index: u32) -> (Self, ChainId) {
        let description = dummy_chain_description(index);
        let chain_id = description.id();
        (Self::new(description), chain_id)
    }

    pub async fn into_hash(self) -> CryptoHash {
        let view = self.into_view().await;
        view.crypto_hash()
            .await
            .expect("hashing from memory should not fail")
    }

    pub async fn into_view(self) -> ExecutionStateView<MemoryContext<TestExecutionRuntimeContext>> {
        let chain_id = self
            .description
            .as_ref()
            .expect("Chain description should be set")
            .into();
        self.into_view_with(chain_id, ExecutionRuntimeConfig::default())
            .await
    }

    pub async fn into_view_with(
        self,
        chain_id: ChainId,
        execution_runtime_config: ExecutionRuntimeConfig,
    ) -> ExecutionStateView<MemoryContext<TestExecutionRuntimeContext>> {
        // Destructure, to make sure we don't miss any fields.
        let SystemExecutionState {
            description,
            epoch,
            admin_id,
            committees,
            ownership,
            balance,
            balances,
            timestamp,
            used_blobs,
            closed,
            application_permissions,
            extra_blobs,
            mock_applications,
        } = self;

        let extra = TestExecutionRuntimeContext::new(chain_id, execution_runtime_config);
        extra
            .add_blobs(extra_blobs)
            .await
            .expect("Adding blobs to the `TestExecutionRuntimeContext` should not fail");
        for (id, mock_application) in mock_applications {
            extra
                .user_contracts()
                .insert(id, mock_application.clone().into());
            extra.user_services().insert(id, mock_application.into());
        }

        let context = MemoryContext::new_for_testing(extra);
        let mut view = ExecutionStateView::load(context)
            .await
            .expect("Loading from memory should work");
        view.system.description.set(description);
        view.system.epoch.set(epoch);
        view.system.admin_id.set(admin_id);
        view.system.committees.set(committees);
        view.system.ownership.set(ownership);
        view.system.balance.set(balance);
        for (account_owner, balance) in balances {
            view.system
                .balances
                .insert(&account_owner, balance)
                .expect("insertion of balances should not fail");
        }
        view.system.timestamp.set(timestamp);
        for blob_id in used_blobs {
            view.system
                .used_blobs
                .insert(&blob_id)
                .expect("inserting blob IDs should not fail");
        }
        view.system.closed.set(closed);
        view.system
            .application_permissions
            .set(application_permissions);
        view
    }
}

impl RegisterMockApplication for SystemExecutionState {
    fn creator_chain_id(&self) -> ChainId {
        self.description.as_ref().expect(
            "Can't register applications on a system state with no associated `ChainDescription`",
        ).into()
    }

    async fn register_mock_application_with(
        &mut self,
        description: ApplicationDescription,
        contract: Blob,
        service: Blob,
    ) -> anyhow::Result<(ApplicationId, MockApplication)> {
        let id = ApplicationId::from(&description);
        let application = MockApplication::default();

        self.extra_blobs.extend([
            contract,
            service,
            Blob::new_application_description(&description),
        ]);
        self.mock_applications.insert(id, application.clone());

        Ok((id, application))
    }
}
