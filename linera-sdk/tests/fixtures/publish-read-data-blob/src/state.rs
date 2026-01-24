// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::{
    linera_base_types::DataBlobHash,
    views::{linera_views, SyncRegisterView, SyncView, ViewStorageContext},
};

#[derive(SyncView)]
#[view(context = ViewStorageContext)]
pub struct PublishReadDataBlobState {
    pub hash: SyncRegisterView<Option<DataBlobHash>>,
}
