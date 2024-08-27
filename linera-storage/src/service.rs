// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_storage_service::client::ServiceStoreClient;

use crate::db_storage::DbStorage;

pub type ServiceStorage<C> = DbStorage<ServiceStoreClient, C>;
