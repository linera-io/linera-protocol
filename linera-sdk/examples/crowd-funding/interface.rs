use fungible::SignedTransfer;
use serde::{Deserialize, Serialize};

/// A cross-application call.
#[derive(Deserialize, Serialize)]
pub enum ApplicationCall {
    Pledge,
    DelegatedPledge { transfer: SignedTransfer },
    Collect,
    Cancel,
}

// Work-around to pretend that `fungible` is an external crate, exposing the Fungible Token
// application's interface.
use crate::fungible;
