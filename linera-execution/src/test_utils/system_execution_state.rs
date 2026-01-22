// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Not,
};

use custom_debug_derive::Debug;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, ApplicationPermissions, Blob, ChainDescription, Epoch, Timestamp},
    identifiers::{AccountOwner, ApplicationId, BlobId, ChainId},
    ownership::ChainOwnership,
};
use linera_views::{context::MemoryContext, views::View};

use super::{dummy_chain_description, dummy_committees, MockApplication, RegisterMockApplication};
use crate::{
    committee::Committee, ApplicationDescription, ExecutionRuntimeConfig, ExecutionRuntimeContext,
    ExecutionStateView, TestExecutionRuntimeContext,
};

/// A system execution state, not represented as a view but as a simple struct.
#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub struct SystemExecutionState {
    pub description: Option<ChainDescription>,
    pub epoch: Epoch,
    pub admin_chain_id: Option<ChainId>,
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
        let admin_chain_id = Some(dummy_chain_description(0).id());
        SystemExecutionState {
            epoch,
            description: Some(description),
            admin_chain_id,
            ownership,
            balance,
            committees: dummy_committees(),
            ..SystemExecutionState::default()
        }
    }

    pub fn dummy_chain_state(index: u32) -> (Self, ChainId) {
        let description = dummy_chain_description(index);
        let chain_id = description.id();
        (Self::new(description), chain_id)
    }

    pub async fn into_hash(self) -> CryptoHash {
        let mut view = self.into_view().await;
        view.crypto_hash_mut()
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
            admin_chain_id,
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
                .pin()
                .insert(id, mock_application.clone().into());
            extra
                .user_services()
                .pin()
                .insert(id, mock_application.into());
        }

        let context = MemoryContext::new_for_testing(extra);
        let mut view = ExecutionStateView::load(context)
            .await
            .expect("Loading from memory should work");
        view.system.description.set(description);
        view.system.epoch.set(epoch);
        view.system.admin_chain_id.set(admin_chain_id);
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
