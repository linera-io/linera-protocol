// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::scylla_db::ScyllaDbStore;

use crate::db_storage::DbStorage;

pub type ScyllaDbStorage<C> = DbStorage<ScyllaDbStore, C>;
