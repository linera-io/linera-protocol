// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::memory::MemoryStore;

use crate::db_storage::DbStorage;

pub type MemoryStorage<C> = DbStorage<MemoryStore, C>;
