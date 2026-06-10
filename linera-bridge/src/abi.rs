// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Canonical ABI types for the EVM→Linera bridge application.
//!
//! These types define the BCS-stable wire format used between:
//! - the on-chain `evm-bridge` Wasm contract (which deserializes operations),
//! - the off-chain relay (which serializes operations to submit them),
//! - and end-to-end tests (which exercise the full pipeline).
//!
//! Hosting them here ensures all three sites stay in lockstep without manual copying.

use linera_base::{
    data_types::U128,
    identifiers::{ApplicationId, ChainId},
};
use linera_sdk::formats::StableEnum;
use serde::{Deserialize, Serialize};

/// Parameters for a deployed bridge instance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BridgeParameters {
    /// EVM chain ID of the source chain (e.g. 8453 for Base).
    pub source_chain_id: u64,
    /// ERC-20 token address on the source EVM chain.
    pub token_address: [u8; 20],
    /// Linera chain ID of the bridge chain (the wrapped-fungible mint chain),
    /// where burns are driven and the `BurnEvent` is emitted. A user's local
    /// `Burn` operation routes the funding transfer and burn request here.
    pub bridge_chain_id: ChainId,
    /// The wrapped-fungible application this bridge mints and burns. The bridge
    /// calls `Mint`/`Burn` on it. Set at creation — the wrapped app must exist
    /// first (it learns its bridge afterwards via `RegisterAuthorizedCaller`). Being a
    /// parameter, it is available on every chain, so a user's local `Burn`
    /// operation never has to supply it.
    pub fungible_app_id: ApplicationId,
}

/// Operations accepted by the bridge contract.
#[derive(Debug, StableEnum)]
pub enum BridgeOperation {
    /// Verify a deposit proof and mint wrapped tokens.
    ProcessDeposit {
        block_header_rlp: Vec<u8>,
        receipt_rlp: Vec<u8>,
        proof_nodes: Vec<Vec<u8>>,
        tx_index: u64,
        log_index: u64,
    },
    /// Verify that an EVM block hash is authentic and finalized.
    ///
    /// Queries the EVM node to confirm the block exists and its number is at or below
    /// the latest finalized block. Caches the hash only when submitted by an
    /// authenticated signer (chain owner) to prevent state bloat.
    VerifyBlockHash { block_hash: [u8; 32] },
    /// Register the EVM FungibleBridge contract address.
    /// Can only be called once, by the chain owner.
    /// Must be set before ProcessDeposit can succeed.
    RegisterFungibleBridge { address: [u8; 20] },
    /// Replace the bridge's RPC endpoint. Chain-owner-only.
    /// Empty string disables finality verification.
    SetRpcEndpoint { rpc_endpoint: String },
    /// Burn wrapped tokens and signal an EVM release. Submitted by a user to
    /// the bridge instance on their *own* chain. The tokens are burned from the
    /// operation's authenticated signer; the wrapped-fungible application is
    /// taken from the bridge's own `fungible_app_id` parameter, never from the
    /// operation. Atomically (one outgoing bundle) the bridge moves `amount`
    /// from the signer into the signer's own escrow account on the bridge chain
    /// — a tracked transfer through that wrapped-fungible app — and sends a
    /// tracked [`BridgeMessage::Burn`] to the bridge chain. The signer is
    /// propagated to the bridge chain via the authenticated message, so the burn
    /// there can only ever drain the escrow that same signer funded. If the burn
    /// cannot complete, the whole bundle is rejected and the funding transfer
    /// bounces back to the signer.
    Burn {
        /// Amount to burn, in the source ERC-20's decimal scale.
        amount: U128,
        /// EVM address that will receive the released ERC-20 tokens.
        evm_target: [u8; 20],
    },
}

/// Cross-chain messages sent between bridge application instances.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BridgeMessage {
    /// Sent from the user's chain to the bridge chain to drive a burn. Paired,
    /// in the same outgoing bundle, with the funding wrapped-fungible transfer
    /// into the signer's escrow account on the bridge chain. The message is
    /// authenticated, so the bridge chain recovers the originating signer and
    /// burns that signer's escrowed `amount`, then emits the `BurnEvent`
    /// carrying `evm_target`. A bouncing delivery is a no-op (the funding
    /// transfer's own bounce refunds the user).
    Burn {
        /// Amount to burn, matching the funding transfer.
        amount: U128,
        /// EVM address that will receive the released ERC-20 tokens.
        evm_target: [u8; 20],
    },
}

/// Initial state passed at `create_application`.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct BridgeInstantiationArgument {
    /// JSON-RPC endpoint of the source EVM chain for finality verification.
    /// Empty string means "skip finality verification" (test / local-dev mode).
    pub rpc_endpoint: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instantiation_argument_json_roundtrip() {
        let arg = BridgeInstantiationArgument {
            rpc_endpoint: "https://example.com/rpc".to_string(),
        };
        let json = serde_json::to_string(&arg).unwrap();
        let back: BridgeInstantiationArgument = serde_json::from_str(&json).unwrap();
        assert_eq!(arg, back);

        // Empty string is a valid value (skip-finality mode).
        let empty = BridgeInstantiationArgument::default();
        let json = serde_json::to_string(&empty).unwrap();
        assert_eq!(json, r#"{"rpc_endpoint":""}"#);
    }
}
