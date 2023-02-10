use serde::{Deserialize, Serialize};
use simple_fungible::SignedTransfer;

/// A cross-application call.
#[derive(Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum ApplicationCall {
    Pledge,
    DelegatedPledge { transfer: SignedTransfer },
    Collect,
    Cancel,
}
