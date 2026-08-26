// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generic EVM client for calling a LightClient contract's `addCommittee` method.

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
    sol,
};
use alloy_sol_types::SolCall;
use url::Url;

sol! {
    function addCommittee(
        bytes calldata data,
        bytes calldata committeeBlob,
        bytes[] calldata validators
    ) external;
}

/// Client for interacting with a deployed LightClient contract on an EVM chain.
#[allow(clippy::type_complexity)]
pub struct LightClient {
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

impl LightClient {
    /// Creates a new LightClient client.
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
    /// - `committee_blob`: raw committee blob bytes
    /// - `validator_keys`: uncompressed 64-byte public keys (without 0x04 prefix)
    pub async fn add_committee(
        &self,
        certificate_bytes: &[u8],
        committee_blob: &[u8],
        validator_keys: Vec<Vec<u8>>,
    ) -> anyhow::Result<TxHash> {
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
