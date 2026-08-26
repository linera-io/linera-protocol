// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Centralized Linera client for all bridge chain interactions.

use std::sync::Arc;

use anyhow::{Context as _, Result};
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, Event},
    identifiers::{ApplicationId, GenericApplicationId},
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_core::client::ChainClient;
use tokio::sync::{mpsc, oneshot};

use crate::proof::DepositKey;

/// A write operation to be executed on the bridge chain.
/// Sent to the main loop which serializes all chain mutations.
#[derive(Debug)]
pub(crate) enum ChainOperation {
    ProcessInbox {
        response: oneshot::Sender<Result<Vec<ConfirmedBlockCertificate>>>,
    },
    ProcessDeposit {
        proof: crate::proof::gen::DepositProof,
        response: oneshot::Sender<Result<()>>,
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

    /// Returns the bridge application ID.
    pub fn bridge_app_id(&self) -> ApplicationId {
        self.bridge_app_id
    }

    /// Returns the wrapped-fungible application ID.
    pub fn fungible_app_id(&self) -> ApplicationId {
        self.fungible_app_id
    }

    // ── Read operations (safe on cloned chain_client) ──

    /// Synchronizes the chain client from the validators.
    pub async fn sync(&self) -> Result<()> {
        self.chain_client
            .synchronize_from_validators()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    /// Returns the chain's current info.
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

    /// Reads the confirmed block with the given hash.
    pub async fn read_confirmed_block(
        &self,
        hash: CryptoHash,
    ) -> Result<Arc<linera_chain::block::ConfirmedBlock>> {
        self.chain_client
            .read_confirmed_block(hash)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    /// Reads the confirmed block certificate with the given hash.
    pub async fn read_certificate(
        &self,
        hash: CryptoHash,
    ) -> Result<Arc<ConfirmedBlockCertificate>> {
        self.chain_client
            .read_certificate(hash)
            .await
            .map(|c| c.into_std())
            .map_err(|e| anyhow::anyhow!(e))
    }

    /// Returns whether the deposit with the given key has already been processed.
    pub async fn query_deposit_processed(&self, deposit_key: &DepositKey) -> Result<bool> {
        crate::monitor::query_deposit_processed(&self.chain_client, self.bridge_app_id, deposit_key)
            .await
    }

    // ── Write operations (sent to main loop via channel) ──

    /// Submits a deposit proof to the bridge chain via the main loop.
    pub async fn process_deposit(&self, proof: crate::proof::gen::DepositProof) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.op_tx
            .send(ChainOperation::ProcessDeposit {
                proof,
                response: resp_tx,
            })
            .await
            .with_context(|| "Chain operation channel closed")?;
        resp_rx.await.with_context(|| "Response channel closed")?
    }

    /// Processes the bridge chain's inbox via the main loop.
    pub async fn process_inbox(&self) -> Result<Vec<ConfirmedBlockCertificate>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.op_tx
            .send(ChainOperation::ProcessInbox { response: resp_tx })
            .await
            .with_context(|| "Chain operation channel closed")?;
        resp_rx.await.with_context(|| "Response channel closed")?
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

/// Finds all `BurnEvent`s emitted by the bridge application on its "burns"
/// stream within a block's event streams.
///
/// Returns `(tx_index, event_pos_in_tx, event_index, BurnEvent)` for each burn:
/// - `tx_index`: position of the transaction within `body.events` (outer index)
/// - `event_pos_in_tx`: position of the event within `body.events[tx_index]` (inner index)
/// - `event_index`: `Event.index` — the stream index, used as the on-chain dedup key
pub(crate) fn find_burn_events(
    events: &[Vec<Event>],
    bridge_app_id: ApplicationId,
) -> Vec<(u32, u32, u32, wrapped_fungible::BurnEvent)> {
    let mut result = Vec::new();
    for (tx_index, tx_events) in (0u32..).zip(events) {
        for (event_pos, event) in (0u32..).zip(tx_events) {
            if event.stream_id.application_id != GenericApplicationId::User(bridge_app_id) {
                continue;
            }
            if event.stream_id.stream_name.0 != b"burns" {
                continue;
            }
            if let Ok(burn) = bcs::from_bytes::<wrapped_fungible::BurnEvent>(&event.value) {
                result.push((tx_index, event_pos, event.index, burn));
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::{CryptoHash, TestString},
        data_types::{Event, U128},
        identifiers::{ApplicationId, GenericApplicationId, StreamId, StreamName},
    };

    use super::find_burn_events;

    fn app_id(tag: &str) -> ApplicationId {
        ApplicationId::new(CryptoHash::new(&TestString::new(tag)))
    }

    fn encoded_burn(target: [u8; 20], amount: u128) -> Vec<u8> {
        bcs::to_bytes(&wrapped_fungible::BurnEvent {
            target,
            amount: U128(amount),
        })
        .unwrap()
    }

    fn event(app: ApplicationId, stream: &str, value: Vec<u8>, index: u32) -> Event {
        Event {
            stream_id: StreamId {
                application_id: GenericApplicationId::User(app),
                stream_name: StreamName::from(stream),
            },
            index,
            value,
        }
    }

    /// The relayer must match burns by the *bridge* application's "burns"
    /// stream — not by any other application or stream.
    #[test]
    fn matches_only_the_given_app_burns_stream() {
        let bridge_app_id = app_id("bridge");
        let other = app_id("other");
        let burn_target = [0xE7; 20];
        let burn_amount = 123;
        let value = encoded_burn(burn_target, burn_amount);

        let events = vec![
            // tx 0: a burn emitted by the bridge app.
            vec![event(bridge_app_id, "burns", value.clone(), 7)],
            // tx 1: a burn from a different app, and a non-"burns" stream event
            // from the bridge app — neither should match when keyed on `bridge_app_id`.
            vec![
                event(other, "burns", value.clone(), 0),
                event(bridge_app_id, "transfers", value.clone(), 1),
            ],
        ];

        let found = find_burn_events(&events, bridge_app_id);
        assert_eq!(
            found.len(),
            1,
            "only the bridge app's burns-stream event must match"
        );
        let (tx_index, event_pos, event_index, decoded) = &found[0];
        assert_eq!((*tx_index, *event_pos, *event_index), (0, 0, 7));
        assert_eq!(decoded.target, burn_target);
        assert_eq!(decoded.amount, U128(burn_amount));

        // Keyed on a different application, the bridge app's burn is ignored.
        let found_other = find_burn_events(&events, other);
        assert_eq!(found_other.len(), 1);
        assert_eq!(
            found_other[0].2, 0,
            "matches the other app's burn at index 0"
        );
    }
}
