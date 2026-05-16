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
    use alloy::{
        contract::Error as ContractError, rpc::json_rpc::ErrorPayload, transports::RpcError,
    };
    use serde_json::value::RawValue;

    use super::*;

    /// Builds an `Err(ContractError)` whose underlying transport carries a
    /// JSON-RPC `ErrorResp` — the same shape geth, reth, anvil, alchemy, and
    /// other nodes return for validation/estimate failures.
    fn rpc_error(message: &str, data: Option<Box<RawValue>>) -> ContractError {
        let payload: ErrorPayload = ErrorPayload {
            code: -32000,
            message: message.to_string().into(),
            data,
        };
        ContractError::TransportError(RpcError::ErrorResp(payload))
    }

    #[test]
    fn estimate_fits_ok_returns_true() {
        assert!(estimate_fits(Ok(123_456)).unwrap());
        assert!(estimate_fits(Ok(0)).unwrap());
    }

    #[test]
    fn estimate_fits_gas_required_exceeds_returns_false() {
        // Geth / Erigon / Alchemy wording for an estimate that would not fit.
        let err = rpc_error("gas required exceeds allowance (30000000)", None);
        assert!(!estimate_fits(Err(err)).unwrap());
    }

    #[test]
    fn estimate_fits_exceeds_block_gas_limit_returns_false() {
        // Reth / foundry forks wording.
        let err = rpc_error("call exceeds block gas limit", None);
        assert!(!estimate_fits(Err(err)).unwrap());
    }

    #[test]
    fn estimate_fits_anvil_out_of_gas_returns_false() {
        // Anvil 1.6's eth_estimateGas wording for a block-gas-limit hit
        // (verified empirically against both calldata-too-large and
        // infinite-loop constructors). Substring "gas required exceeds"
        // matches the geth/anvil/alchemy branch.
        let err = rpc_error("Out of gas: gas required exceeds allowance: 100000", None);
        assert!(!estimate_fits(Err(err)).unwrap());
    }

    #[test]
    fn estimate_fits_real_contract_revert_bubbles_up() {
        // `execution reverted` is a real on-chain revert (REVERT opcode),
        // not a block-gas-limit signal — verified on anvil 1.6, which
        // returns this message with `data: "0x"` (i.e. `Some`, not `None`).
        let revert_data: Box<RawValue> = serde_json::from_str(r#""0x""#).unwrap();
        let err = rpc_error("execution reverted", Some(revert_data));
        assert!(estimate_fits(Err(err)).is_err());
    }

    /// A bare "out of gas" message is NOT a fits/doesn't-fit signal — it can
    /// also be raised when the relayer's tx gas cap is too low or the
    /// contract state consumes more gas than estimated. Treating it as
    /// "doesn't fit" would mask those real misconfigurations behind retry
    /// churn down the chunking path.
    #[test]
    fn estimate_fits_bare_out_of_gas_bubbles_up() {
        let err = rpc_error("transaction reverted: out of gas", None);
        assert!(estimate_fits(Err(err)).is_err());
    }

    #[test]
    fn estimate_fits_unrelated_rpc_error_bubbles_up() {
        let err = rpc_error("nonce too low", None);
        assert!(estimate_fits(Err(err)).is_err());
    }

    #[test]
    fn estimate_fits_transport_layer_error_bubbles_up() {
        // HTTP-level / connection failure — not a JSON-RPC ErrorResp; must
        // not be classified as gas-exceeded.
        use alloy::transports::TransportErrorKind;
        let err = ContractError::TransportError(TransportErrorKind::custom_str(
            "connection reset by peer",
        ));
        assert!(estimate_fits(Err(err)).is_err());
    }
}
