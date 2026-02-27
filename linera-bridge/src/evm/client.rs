// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! EVM client for relaying committee changes to a LightClient contract.

use alloy::{
    network::{Ethereum, EthereumWallet},
    primitives::{keccak256, Address, Bytes, TxHash},
    providers::{
        fillers::{
            BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
            WalletFiller,
        },
        Provider, ProviderBuilder, RootProvider,
    },
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use alloy_sol_types::SolCall;
use linera_base::crypto::ValidatorPublicKey;
use linera_execution::committee::Committee;
use url::Url;

use super::light_client::addCommitteeCall;

/// Client for interacting with a deployed LightClient contract on an EVM chain.
#[allow(clippy::type_complexity)]
pub struct EvmLightClient {
    provider: FillProvider<
        JoinFill<
            JoinFill<
                alloy::providers::Identity,
                JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
            >,
            WalletFiller<EthereumWallet>,
        >,
        RootProvider<Ethereum>,
    >,
    contract_address: Address,
}

impl EvmLightClient {
    /// Creates a new EVM light client.
    ///
    /// - `endpoint`: HTTP JSON-RPC URL of the EVM node
    /// - `contract_address`: deployed LightClient contract address
    /// - `private_key`: hex-encoded private key for signing transactions
    pub fn new(
        endpoint: &str,
        contract_address: Address,
        private_key: &str,
    ) -> anyhow::Result<Self> {
        let rpc_url = Url::parse(endpoint)?;
        let signer: PrivateKeySigner = private_key.parse()?;
        let wallet = EthereumWallet::from(signer);
        let provider = ProviderBuilder::new().wallet(wallet).connect_http(rpc_url);

        Ok(Self {
            provider,
            contract_address,
        })
    }

    /// Calls `LightClient.addCommittee()` on the EVM chain.
    ///
    /// Extracts uncompressed validator keys from the committee blob internally,
    /// then submits the transaction to the LightClient contract.
    ///
    /// - `certificate_bytes`: BCS-serialized `ConfirmedBlockCertificate`
    /// - `committee_blob`: raw committee blob bytes (BCS-serialized `Committee`)
    pub async fn add_committee(
        &self,
        certificate_bytes: &[u8],
        committee_blob: &[u8],
    ) -> anyhow::Result<TxHash> {
        let validator_keys = extract_validator_keys(committee_blob)?;

        let call = addCommitteeCall {
            data: Bytes::copy_from_slice(certificate_bytes),
            committeeBlob: Bytes::copy_from_slice(committee_blob),
            validators: validator_keys.into_iter().map(Bytes::from).collect(),
        };

        let tx = TransactionRequest::default()
            .to(self.contract_address)
            .input(call.abi_encode().into());

        let receipt = self
            .provider
            .send_transaction(tx)
            .await?
            .get_receipt()
            .await?;

        Ok(receipt.transaction_hash)
    }
}

/// Extracts uncompressed validator public keys from a BCS-serialized committee blob.
///
/// Returns 64-byte uncompressed keys (without the 0x04 prefix) for each validator,
/// sorted by their compressed byte representation to match BCS canonical map ordering.
///
/// BCS serializes map entries sorted by serialized key bytes, which may differ from
/// Rust's `BTreeMap` iteration order (based on `Ord` for `VerifyingKey`).
pub fn extract_validator_keys(committee_blob: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
    let committee: Committee = bcs::from_bytes(committee_blob)?;
    let mut keys: Vec<ValidatorPublicKey> = committee.validators().keys().copied().collect();
    keys.sort_by_key(|a| a.as_bytes());
    Ok(keys.iter().map(validator_uncompressed_key).collect())
}

/// Derives the Ethereum address from a secp256k1 validator public key.
pub fn validator_evm_address(public: &ValidatorPublicKey) -> Address {
    let uncompressed = public.0.to_encoded_point(false);
    let hash = keccak256(&uncompressed.as_bytes()[1..]); // skip 0x04 prefix
    Address::from_slice(&hash[12..])
}

/// Returns the 64-byte uncompressed public key (without the 0x04 prefix).
pub fn validator_uncompressed_key(public: &ValidatorPublicKey) -> Vec<u8> {
    let uncompressed = public.0.to_encoded_point(false);
    uncompressed.as_bytes()[1..].to_vec()
}
