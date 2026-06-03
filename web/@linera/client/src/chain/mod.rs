// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use futures::stream::StreamExt;
use linera_base::{data_types::Round, identifiers::AccountOwner};
use linera_client::chain_listener::ClientContext as _;
use linera_core::{
    client::ChainClient,
    node::{ValidatorNode as _, ValidatorNodeProvider as _},
};
use serde::ser::Serialize as _;
use wasm_bindgen::prelude::*;
use web_sys::{js_sys, wasm_bindgen};

use crate::{Environment, JsResult};

pub mod application;
pub use application::Application;

#[wasm_bindgen]
pub struct Chain {
    pub(crate) client: crate::Client,
    pub(crate) chain_client: ChainClient<Environment>,
}

#[derive(serde::Deserialize, tsify::Tsify)]
#[tsify(from_wasm_abi)]
pub struct TransferParams {
    #[serde(default)]
    pub donor: Option<AccountOwner>,
    pub amount: u64,
    pub recipient: linera_base::identifiers::Account,
}

#[derive(Default, serde::Deserialize, tsify::Tsify)]
#[tsify(from_wasm_abi)]
pub struct AddOwnerOptions {
    #[serde(default)]
    pub weight: u64,
}

/// Information about the round in which a block would currently be proposed on a chain.
#[derive(serde::Serialize, tsify::Tsify)]
#[tsify(into_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct RoundInfo {
    /// The category of the round: `"fast"`, `"multiLeader"`, `"singleLeader"` or
    /// `"validator"`.
    pub kind: String,
    /// The index of the round within its category (always `0` for the fast round).
    pub number: u32,
    /// The owner currently allowed to propose, or `undefined` if any eligible owner
    /// may propose (the fast and multi-leader rounds).
    pub leader: Option<AccountOwner>,
    /// Whether this client's current identity may propose a block in this round.
    pub can_propose: bool,
}

#[wasm_bindgen]
impl Chain {
    /// Sets a callback to be called when a notification is received
    /// from the network.
    ///
    /// # Errors
    /// If we fail to subscribe to the notification stream.
    ///
    /// # Panics
    /// If the handler function fails.
    #[wasm_bindgen(js_name = onNotification)]
    pub fn on_notification(&self, handler: js_sys::Function) -> JsResult<()> {
        let mut notifications = self.chain_client.subscribe()?;
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(notification) = notifications.next().await {
                tracing::debug!("received notification: {notification:?}");
                handler
                    .call1(
                        &JsValue::null(),
                        &serde_wasm_bindgen::to_value(&notification).unwrap(),
                    )
                    .unwrap_throw();
            }
        });
        Ok(())
    }

    /// Transfers funds from one account to another.
    ///
    /// `options` should be an options object of the form `{ donor,
    /// recipient, amount }`; omitting `donor` will cause the funds to
    /// come from the chain balance.
    ///
    /// # Errors
    /// - if the options object is of the wrong form
    /// - if the transfer fails
    #[wasm_bindgen]
    pub async fn transfer(&self, params: TransferParams) -> JsResult<()> {
        let _hash = self
            .client
            .context
            .lock()
            .await
            .apply_client_command(&self.chain_client, |_chain_client| {
                self.chain_client.transfer(
                    params.donor.unwrap_or(AccountOwner::CHAIN),
                    linera_base::data_types::Amount::from_tokens(params.amount.into()),
                    params.recipient,
                )
            })
            .await?;

        Ok(())
    }

    /// Gets the balance of the default chain.
    ///
    /// # Errors
    /// If the chain couldn't be established.
    pub async fn balance(&self) -> JsResult<String> {
        Ok(self.chain_client.query_balance().await?.to_string())
    }

    /// Gets the identity of the default chain.
    ///
    /// # Errors
    /// If the chain couldn't be established.
    pub async fn identity(&self) -> JsResult<AccountOwner> {
        Ok(self.chain_client.identity().await?)
    }

    /// Adds a new owner to the default chain.
    ///
    /// # Errors
    ///
    /// If the owner is in the wrong format, or the chain client can't be instantiated.
    #[wasm_bindgen(js_name = addOwner)]
    pub async fn add_owner(
        &self,
        owner: AccountOwner,
        options: Option<AddOwnerOptions>,
    ) -> JsResult<()> {
        let AddOwnerOptions { weight } = options.unwrap_or_default();
        self.client
            .context
            .lock()
            .await
            .apply_client_command(&self.chain_client, |_chain_client| {
                self.chain_client.share_ownership(owner, weight)
            })
            .await?;
        Ok(())
    }

    /// Synchronizes this chain with the validators, downloading any blocks and state that
    /// the local node is missing.
    ///
    /// Reads such as [`balance`](Self::balance), [`nextRoundInfo`](Self::next_round_info),
    /// [`isOwner`](Self::is_owner) and [`ownerWeight`](Self::owner_weight) operate on the
    /// local node, so call this after first connecting to a chain (for example one just
    /// claimed from the faucet) to make sure they observe the chain's current state.
    ///
    /// # Errors
    /// If synchronization fails, e.g. because validators are unreachable.
    #[wasm_bindgen]
    pub async fn synchronize(&self) -> JsResult<()> {
        self.chain_client.synchronize_from_validators().await?;
        self.client
            .context
            .lock()
            .await
            .update_wallet(&self.chain_client)
            .await?;
        Ok(())
    }

    /// Returns whether `owner` is currently an owner of this chain, either as a regular
    /// owner or a super owner.
    ///
    /// Useful before calling [`addOwner`](Self::add_owner), which silently overwrites the
    /// weight of an existing owner rather than failing.
    ///
    /// # Errors
    /// If the chain ownership cannot be retrieved.
    #[wasm_bindgen(js_name = isOwner)]
    pub async fn is_owner(&self, owner: AccountOwner) -> JsResult<bool> {
        let ownership = self.chain_client.query_chain_ownership().await?;
        Ok(ownership.verify_owner(&owner))
    }

    /// Returns the weight of a regular owner of this chain, or `undefined` if `owner` is
    /// not a regular owner (it may be a super owner, which has no weight, or not an owner
    /// at all).
    ///
    /// The weight determines how often the owner is selected as the leader in
    /// single-leader and validator rounds.
    ///
    /// # Errors
    /// If the chain ownership cannot be retrieved.
    #[wasm_bindgen(js_name = ownerWeight)]
    pub async fn owner_weight(&self, owner: AccountOwner) -> JsResult<Option<f64>> {
        let ownership = self.chain_client.query_chain_ownership().await?;
        let weight = ownership.owners.get(&owner).copied();
        #[expect(
            clippy::cast_precision_loss,
            reason = "owner weights are small relative values"
        )]
        let weight = weight.map(|weight| weight as f64);
        Ok(weight)
    }

    /// Discards any pending block proposal on this chain.
    ///
    /// When a proposal fails to reach a quorum (for example because the client went
    /// offline mid-round) it stays queued and is retried before any new block. Call this
    /// to drop it so a fresh block can be proposed instead.
    ///
    /// Importantly, this must never be used to clear a proposal already submitted in the
    /// fast round: fast-round proposals are final, so clearing one is rejected with an
    /// error.
    ///
    /// # Errors
    /// If the chain is currently in the fast round, or the wallet fails to persist the
    /// cleared state.
    #[wasm_bindgen(js_name = clearPendingProposal)]
    pub async fn clear_pending_proposal(&self) -> JsResult<()> {
        let info = self.chain_client.chain_info().await?;
        if info.manager.current_round == Round::Fast {
            return Err(JsError::new(
                "cannot clear a pending proposal in the fast round",
            ));
        }
        self.chain_client.clear_pending_proposal().await;
        // Of all proposals, only fast-round ones are persisted in the wallet across
        // sessions. The guard above forbids clearing one while the chain is still in the
        // fast round, but a stuck fast-round proposal can outlive the fast round itself;
        // refresh the persisted copy so that clearing it here is not undone on reload.
        self.client
            .context
            .lock()
            .await
            .update_wallet(&self.chain_client)
            .await?;
        Ok(())
    }

    /// Returns information about the round in which a block would currently be proposed on
    /// this chain: its category, its index, the current leader (if the round restricts
    /// proposals to a single owner) and whether this client's identity may propose.
    ///
    /// `leader` is `undefined` in the fast and multi-leader rounds, where any eligible
    /// owner may propose; in the single-leader and validator rounds it is the owner
    /// currently allowed to propose.
    ///
    /// # Errors
    /// If the chain information cannot be retrieved.
    #[wasm_bindgen(js_name = nextRoundInfo)]
    pub async fn next_round_info(&self) -> JsResult<RoundInfo> {
        let info = self.chain_client.chain_info().await?;
        let manager = &info.manager;
        let identity = self.chain_client.identity().await?;
        let can_propose = manager.can_propose(&identity);
        let (kind, number) = match manager.current_round {
            Round::Fast => ("fast", 0),
            Round::MultiLeader(number) => ("multiLeader", number),
            Round::SingleLeader(number) => ("singleLeader", number),
            Round::Validator(number) => ("validator", number),
        };
        Ok(RoundInfo {
            kind: kind.to_owned(),
            number,
            leader: manager.leader,
            can_propose,
        })
    }

    /// Sets the number of multi-leader rounds for this chain, leaving the rest of the
    /// ownership configuration (owners, super owners, timeouts) unchanged.
    ///
    /// In multi-leader rounds every eligible owner may propose a block concurrently;
    /// afterwards the chain falls back to single-leader rounds. A larger number favors
    /// liveness under contention, while `0` makes the chain reach single-leader rounds
    /// immediately.
    ///
    /// # Errors
    /// If the chain is inactive, or the ownership change fails to commit.
    #[wasm_bindgen(js_name = setMultiLeaderRounds)]
    pub async fn set_multi_leader_rounds(&self, rounds: u32) -> JsResult<()> {
        self.client
            .context
            .lock()
            .await
            .apply_client_command(&self.chain_client, |_chain_client| async {
                let mut ownership = self.chain_client.query_chain_ownership().await?;
                ownership.multi_leader_rounds = rounds;
                self.chain_client.change_ownership(ownership).await
            })
            .await?;
        Ok(())
    }

    /// Gets the version information of the validators of the current network.
    ///
    /// # Errors
    /// If a validator is unreachable.
    #[wasm_bindgen(js_name = validatorVersionInfo)]
    pub async fn validator_version_info(&self) -> JsResult<JsValue> {
        self.chain_client.synchronize_from_validators().await?;
        let result = self.chain_client.local_committee().await;
        let mut client = self.client.context.lock().await;
        client.update_wallet(&self.chain_client).await?;
        let committee = result?;
        let node_provider = client.make_node_provider();

        let mut validator_versions = HashMap::new();

        for (name, state) in committee.validators() {
            match node_provider
                .make_node(&state.network_address)?
                .get_version_info()
                .await
            {
                Ok(version_info) => {
                    if validator_versions
                        .insert(name, version_info.clone())
                        .is_some()
                    {
                        tracing::warn!("duplicate validator entry for validator {name:?}");
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "failed to get version information for validator {name:?}:\n{e:?}"
                    );
                }
            }
        }

        Ok(validator_versions.serialize(
            &serde_wasm_bindgen::Serializer::new()
                .serialize_large_number_types_as_bigints(true)
                .serialize_maps_as_objects(true),
        )?)
    }

    /// Retrieves an application for querying.
    ///
    /// # Errors
    /// If the application ID is invalid.
    #[wasm_bindgen]
    pub async fn application(&self, id: &str) -> JsResult<Application> {
        web_sys::console::debug_1(&format!("connecting to Linera application {id}").into());
        Ok(Application {
            client: self.client.clone(),
            chain_client: self.chain_client.clone(),
            id: id.parse()?,
        })
    }
}
