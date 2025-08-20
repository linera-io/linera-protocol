// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::{
    views::{linera_views, RegisterView, RootView, ViewStorageContext},
    linera_base_types::DataBlobHash,
};

#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct PublishReadDataBlobState {
    pub hash: RegisterView<Option<DataBlobHash>>,
}
