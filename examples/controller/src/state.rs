// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use linera_sdk::{
    abis::controller::{ManagedServiceId, Worker},
    linera_base_types::{AccountOwner, ChainId, MessagePolicy},
    views::{linera_views, SyncMapView, SyncRegisterView, SyncView, SyncSetView, ViewStorageContext},
};

/// The state of the service controller application.
#[derive(SyncView, async_graphql::SimpleObject)]
#[graphql(complex)]
#[view(context = ViewStorageContext)]
pub struct ControllerState {
    // -- Worker chain only --
    /// The description of this worker as we registered it.
    pub local_worker: SyncRegisterView<Option<Worker>>,
    /// The services currently running locally.
    pub local_services: SyncSetView<ManagedServiceId>,
    /// The chains currently followed locally (besides ours and the active service
    /// chains).
    pub local_chains: SyncSetView<ChainId>,
    /// The local message policy.
    pub local_message_policy: SyncMapView<ChainId, MessagePolicy>,

    // -- Controller chain only --
    /// The admin account owners (user or application) allowed to update services.
    pub admins: SyncRegisterView<Option<HashSet<AccountOwner>>>,
    /// All the workers declared in the network.
    pub workers: SyncMapView<ChainId, Worker>,
    /// All the services currently defined and where they run. If services run on several
    /// workers, the chain is configured so that workers can collaborate to produce blocks
    /// (e.g. only one worker is actively producing blocks while the other waits as a
    /// backup).
    // NOTE: Currently, services should run on a single worker at a time.
    pub services: SyncMapView<ManagedServiceId, HashSet<ChainId>>,
    /// All the chains currently being followed and by which workers.
    pub chains: SyncMapView<ChainId, HashSet<ChainId>>,
}
