// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use async_graphql::scalar;
use linera_sdk::{
    abis::controller::{ManagedServiceId, PendingService, Worker},
    linera_base_types::{AccountOwner, ChainId, MessagePolicy},
    views::{linera_views, MapView, RegisterView, RootView, SetView, ViewStorageContext},
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct ServicePendingHandoff {
    pub pending_workers: HashSet<ChainId>,
    pub owners_to_remove: HashSet<AccountOwner>,
}

scalar!(ServicePendingHandoff);

/// The state of the service controller application.
#[derive(RootView, async_graphql::SimpleObject)]
#[graphql(complex)]
#[view(context = ViewStorageContext)]
pub struct ControllerState {
    // -- Worker chain only --
    /// The description of this worker as we registered it.
    pub local_worker: RegisterView<Option<Worker>>,
    /// The services currently running locally.
    pub local_services: SetView<ManagedServiceId>,
    /// The services we were told to run locally, but we're still waiting to for the
    /// block heights at which we're supposed to start running them.
    pub local_pending_services: MapView<ManagedServiceId, PendingService>,
    /// The chains currently followed locally (besides ours and the active service
    /// chains).
    pub local_chains: SetView<ChainId>,
    /// The local message policy.
    pub local_message_policy: MapView<ChainId, MessagePolicy>,

    // -- Controller chain only --
    /// The admin account owners (user or application) allowed to update services.
    pub admins: RegisterView<Option<HashSet<AccountOwner>>>,
    /// All the workers declared in the network.
    pub workers: MapView<ChainId, Worker>,
    /// All the services currently defined and where they run. If services run on several
    /// workers, the chain is configured so that workers can collaborate to produce blocks
    /// (e.g. only one worker is actively producing blocks while the other waits as a
    /// backup).
    // NOTE: Currently, services should run on a single worker at a time.
    pub services: MapView<ManagedServiceId, HashSet<ChainId>>,
    /// Services that have been reassigned to a worker, but are awaiting confirmation that
    /// the previous worker has added the new one as an owner.
    pub pending_services: MapView<ManagedServiceId, ServicePendingHandoff>,
    /// All the chains currently being followed and by which workers.
    pub chains: MapView<ChainId, HashSet<ChainId>>,
}
