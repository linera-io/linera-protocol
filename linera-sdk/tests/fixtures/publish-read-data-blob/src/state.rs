// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::{
    linera_base_types::DataBlobHash,
    views::{linera_views, RegisterView, SyncView, ViewStorageContext},
};

#[derive(SyncView)]
#[view(context = ViewStorageContext)]
pub struct PublishReadDataBlobState {
    pub hash: RegisterView<Option<DataBlobHash>>,
}
