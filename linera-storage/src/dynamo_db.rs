// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::dynamo_db::DynamoDbStore;

use crate::db_storage::DbStorage;

pub type DynamoDbStorage<C> = DbStorage<DynamoDbStore, C>;
