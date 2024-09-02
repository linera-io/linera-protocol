// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::LazyLock;

use tokio::sync::Mutex;

use linera_service::cli_wrappers::ClientWrapper;
use linera_base::identifiers::AccountOwner;


/// A static lock to prevent integration tests from running in parallel.
pub static INTEGRATION_TEST_GUARD: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Gets the `AccountOwner` from the client.
pub fn get_fungible_account_owner(client: &ClientWrapper) -> AccountOwner {
    let owner = client.get_owner().unwrap();
    AccountOwner::User(owner)
}
