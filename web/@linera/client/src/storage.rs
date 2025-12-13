// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, future::Future, rc::Rc, sync::Arc};

use futures::{future::FutureExt as _, lock::Mutex as AsyncMutex, stream::StreamExt};
use linera_base::identifiers::{AccountOwner, ApplicationId};
use linera_client::{
    chain_listener::{ChainListener, ClientContext as _},
};
use linera_core::{
    data_types::ClientOutcome,
    node::{ValidatorNode as _, ValidatorNodeProvider as _},
};
use linera_views::store::WithError;
use serde::ser::Serialize as _;
use wasm_bindgen::prelude::*;
use web_sys::{js_sys, wasm_bindgen};

// TODO(#12): convert to IndexedDbStore once we refactor Context
pub type Storage =
    linera_storage::DbStorage<linera_views::memory::MemoryDatabase, linera_storage::WallClock>;

pub async fn get_storage(
) -> Result<Storage, linera_views::memory::MemoryStoreError> {
    linera_storage::DbStorage::maybe_create_and_connect(
        &linera_views::memory::MemoryStoreConfig {
            max_stream_queries: 1,
            kill_on_drop: false,
        },
        "linera",
        Some(linera_execution::WasmRuntime::Wasmer),
    )
        .await
}
