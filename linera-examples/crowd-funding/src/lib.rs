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
