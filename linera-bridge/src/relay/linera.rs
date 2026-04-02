// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Centralized Linera client for all bridge chain interactions.

use anyhow::Result;
use linera_base::{
    crypto::CryptoHash,
    data_types::Amount,
    identifiers::{AccountOwner, ApplicationId},
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_core::client::ChainClient;
use tokio::sync::{mpsc, oneshot};

use crate::proof::DepositKey;

/// A write operation to be executed on the bridge chain.
/// Sent to the main loop which serializes all chain mutations.
pub(crate) enum ChainOperation {
    ProcessInbox {
        response: oneshot::Sender<Result<Vec<ConfirmedBlockCertificate>, String>>,
    },
    ProcessDeposit {
        proof: crate::proof::gen::DepositProof,
        response: oneshot::Sender<Result<(), String>>,
    },
    Burn {
        owner: AccountOwner,
        amount: Amount,
        response: oneshot::Sender<Result<ConfirmedBlockCertificate, String>>,
    },
}

/// Centralized client for Linera chain interactions.
///
/// Read operations use the `ChainClient` directly (safe on clones).
/// Write operations (block proposals) go through a channel to the main loop.
pub struct LineraClient<E: linera_core::environment::Environment> {
    chain_client: ChainClient<E>,
    op_tx: mpsc::Sender<ChainOperation>,
    bridge_app_id: ApplicationId,
    fungible_app_id: ApplicationId,
}

impl<E: linera_core::environment::Environment> LineraClient<E> {
    pub(crate) fn new(
        chain_client: ChainClient<E>,
        op_tx: mpsc::Sender<ChainOperation>,
        bridge_app_id: ApplicationId,
        fungible_app_id: ApplicationId,
    ) -> Self {
        Self {
            chain_client,
            op_tx,
            bridge_app_id,
            fungible_app_id,
        }
    }

    pub fn bridge_app_id(&self) -> ApplicationId {
        self.bridge_app_id
    }

    pub fn fungible_app_id(&self) -> ApplicationId {
        self.fungible_app_id
    }

    // ── Read operations (safe on cloned chain_client) ──

    pub async fn sync(&self) -> Result<()> {
        self.chain_client
            .synchronize_from_validators()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    pub async fn chain_info(&self) -> Result<Box<linera_core::data_types::ChainInfo>> {
        self.chain_client
            .chain_info()
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    /// Returns the chain's current balance.
    pub async fn chain_balance(&self) -> Result<Amount> {
        let info = self.chain_info().await?;
        Ok(info.chain_balance)
    }

    pub async fn read_confirmed_block(
        &self,
        hash: CryptoHash,
    ) -> Result<linera_chain::block::ConfirmedBlock> {
        self.chain_client
            .read_confirmed_block(hash)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn read_certificate(&self, hash: CryptoHash) -> Result<ConfirmedBlockCertificate> {
        self.chain_client
            .read_certificate(hash)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn query_deposit_processed(&self, deposit_key: &DepositKey) -> Result<bool> {
        crate::monitor::query_deposit_processed(&self.chain_client, self.bridge_app_id, deposit_key)
            .await
    }

    // ── Write operations (sent to main loop via channel) ──

    pub async fn process_deposit(&self, proof: crate::proof::gen::DepositProof) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.op_tx
            .send(ChainOperation::ProcessDeposit {
                proof,
                response: resp_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("Chain operation channel closed"))?;
        resp_rx
            .await
            .map_err(|_| anyhow::anyhow!("Response channel closed"))?
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn burn(
        &self,
        owner: AccountOwner,
        amount: Amount,
    ) -> Result<ConfirmedBlockCertificate> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.op_tx
            .send(ChainOperation::Burn {
                owner,
                amount,
                response: resp_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("Chain operation channel closed"))?;
        resp_rx
            .await
            .map_err(|_| anyhow::anyhow!("Response channel closed"))?
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn process_inbox(&self) -> Result<Vec<ConfirmedBlockCertificate>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.op_tx
            .send(ChainOperation::ProcessInbox { response: resp_tx })
            .await
            .map_err(|_| anyhow::anyhow!("Chain operation channel closed"))?;
        resp_rx
            .await
            .map_err(|_| anyhow::anyhow!("Response channel closed"))?
            .map_err(|e| anyhow::anyhow!(e))
    }
}

impl<E: linera_core::environment::Environment> Clone for LineraClient<E> {
    fn clone(&self) -> Self {
        Self {
            chain_client: self.chain_client.clone(),
            op_tx: self.op_tx.clone(),
            bridge_app_id: self.bridge_app_id,
            fungible_app_id: self.fungible_app_id,
        }
    }
}

/// Find all Credit-to-Address20 messages in a block's transactions for a given app.
pub(crate) fn find_address20_credits(
    transactions: &[linera_chain::data_types::Transaction],
    fungible_app_id: ApplicationId,
) -> Vec<(AccountOwner, Amount)> {
    let mut credits = Vec::new();
    for txn in transactions {
        if let linera_chain::data_types::Transaction::ReceiveMessages(bundle) = txn {
            for posted in &bundle.bundle.messages {
                if let linera_execution::Message::User {
                    application_id,
                    bytes,
                } = &posted.message
                {
                    if *application_id == fungible_app_id {
                        if let Some(credit) = try_parse_credit_to_address20(bytes.as_slice()) {
                            credits.push(credit);
                        }
                    }
                }
            }
        }
    }
    credits
}

/// BCS-serialize a Burn operation.
pub(crate) fn serialize_burn_operation(owner: &AccountOwner, amount: &Amount) -> Vec<u8> {
    bcs::to_bytes(&wrapped_fungible::WrappedFungibleOperation::Burn {
        owner: *owner,
        amount: *amount,
    })
    .expect("failed to BCS-serialize Burn operation")
}

fn try_parse_credit_to_address20(bytes: &[u8]) -> Option<(AccountOwner, Amount)> {
    if let Ok(fungible::Message::Credit { target, amount, .. }) = bcs::from_bytes(bytes) {
        matches!(target, AccountOwner::Address20(_)).then_some((target, amount))
    } else {
        None
    }
}
