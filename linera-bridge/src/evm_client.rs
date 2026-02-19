// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! EVM client for relaying committee changes to a LightClient contract.

use alloy::{
    network::{Ethereum, EthereumWallet},
    primitives::{Address, Bytes, TxHash},
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
use k256::elliptic_curve::sec1::ToEncodedPoint;
use linera_base::crypto::ValidatorPublicKey;
use linera_execution::committee::Committee;
use url::Url;

use crate::light_client::addCommitteeCall;

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
    /// - `certificate_bytes`: BCS-serialized `ConfirmedBlockCertificate`
    /// - `committee_blob`: raw committee blob bytes (BCS-serialized `Committee`)
    /// - `validators`: 64-byte uncompressed public keys (no 0x04 prefix)
    pub async fn add_committee(
        &self,
        certificate_bytes: &[u8],
        committee_blob: &[u8],
        validators: Vec<Vec<u8>>,
    ) -> anyhow::Result<TxHash> {
        let call = addCommitteeCall {
            data: Bytes::copy_from_slice(certificate_bytes),
            committeeBlob: Bytes::copy_from_slice(committee_blob),
            validators: validators.into_iter().map(Bytes::from).collect(),
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
/// Returns 64-byte uncompressed keys (without the 0x04 prefix) for each validator.
pub fn extract_validator_keys(committee_blob: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
    let committee: Committee = bcs::from_bytes(committee_blob)?;
    let keys: Vec<Vec<u8>> = committee
        .validators()
        .keys()
        .map(|public_key| uncompressed_key(public_key))
        .collect();
    Ok(keys)
}

/// Returns the 64-byte uncompressed public key (without the 0x04 prefix).
fn uncompressed_key(public: &ValidatorPublicKey) -> Vec<u8> {
    let uncompressed = public.0.to_encoded_point(false);
    uncompressed.as_bytes()[1..].to_vec()
}
