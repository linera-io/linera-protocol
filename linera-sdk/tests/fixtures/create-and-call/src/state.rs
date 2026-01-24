// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use counter_no_graphql::CounterNoGraphQlAbi;
use linera_sdk::{
    linera_base_types::ApplicationId,
    views::{linera_views, SyncRegisterView, SyncView, ViewStorageContext},
};

/// The application state.
#[derive(SyncView)]
#[view(context = ViewStorageContext)]
pub struct CreateAndCallState {
    pub value: SyncRegisterView<Option<ApplicationId<CounterNoGraphQlAbi>>>,
}
