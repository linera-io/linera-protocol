// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::rocks_db::RocksDbStore;

use crate::db_storage::DbStorage;

pub type RocksDbStorage<C> = DbStorage<RocksDbStore, C>;
