use simple_fungible::SignedTransfer;
use serde::{Deserialize, Serialize};

/// A cross-application call.
#[derive(Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum ApplicationCall {
    Pledge,
    DelegatedPledge { transfer: SignedTransfer },
    Collect,
    Cancel,
}
