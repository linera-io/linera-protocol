// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Pure routing helpers for the per-burn fallback path.
//!
//! `estimate_fits` turns a raw `eth_estimateGas` result into a fits /
//! doesn't-fit decision. The actual chunking algorithm is inlined in
//! `monitor::linera::process_pending_burns` to avoid an `AsyncFn`
//! predicate's HRTB Send issue when the future captures `&EvmClient`
//! across await points.

/// Turns a raw `eth_estimateGas` result into a fits/doesn't-fit decision.
/// `Ok(_)` means the node already accepted the estimate. A gas-exceeded
/// RPC error means the call wouldn't fit. Other errors bubble up.
pub fn estimate_fits(r: Result<u64, alloy::contract::Error>) -> anyhow::Result<bool> {
    use crate::relay::evm::is_gas_exceeded_error;
    match r {
        Ok(_) => Ok(true),
        Err(e) if is_gas_exceeded_error(&e) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimate_fits_ok_returns_true() {
        assert!(estimate_fits(Ok(123_456)).unwrap());
        assert!(estimate_fits(Ok(0)).unwrap());
    }

    #[test]
    fn estimate_fits_gas_exceeded_error_returns_false() {
        use alloy::transports::TransportErrorKind;
        let transport_err =
            TransportErrorKind::custom_str("gas required exceeds allowance (30000000)");
        let contract_err = alloy::contract::Error::TransportError(transport_err);
        assert!(!estimate_fits(Err(contract_err)).unwrap());
    }

    #[test]
    fn estimate_fits_other_error_bubbles_up() {
        use alloy::transports::TransportErrorKind;
        let transport_err = TransportErrorKind::custom_str("nonce too low");
        let contract_err = alloy::contract::Error::TransportError(transport_err);
        assert!(estimate_fits(Err(contract_err)).is_err());
    }
}
