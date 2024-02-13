// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::db_storage::DbStorage;
use linera_storage_service::client::SharedStoreClient;

pub type ServiceStorage<C> = DbStorage<SharedStoreClient, C>;
