// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use futures::stream::StreamExt;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, BlockHeight, Round, Timestamp},
    identifiers::{AccountOwner, ApplicationId, BlobId, ChainId, ModuleId, StreamId},
};
use linera_client::chain_listener::ClientContext as _;
use linera_core::{
    client::ChainClient,
    node::{ValidatorNode as _, ValidatorNodeProvider as _},
};
use linera_storage::Storage as _;
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

/// TypeScript names for the `linera-base` types used in the interfaces below. They
/// serialize as these primitives but carry no TypeScript mapping of their own.
#[wasm_bindgen(typescript_custom_section)]
const _: &str = r"
export type Amount = string;
export type BlockHeight = number;
export type ModuleId = string;
export type Timestamp = number;
export type StreamId = string;
export type BlobId = string;
";

/// How many blocks [`Chain::blocks`] returns when the caller doesn't say.
const DEFAULT_BLOCKS_LIMIT: u32 = 10;

/// The largest integer a JavaScript number represents exactly.
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// The local node's view of a chain: where its block log ends, and the state at that
/// point.
#[derive(serde::Serialize, tsify::Tsify)]
#[tsify(into_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct ChainSummary {
    /// The chain this describes.
    pub chain_id: ChainId,
    /// The epoch, i.e. the committee configuration, the chain is currently on.
    pub epoch: u32,
    /// The balance of the chain account.
    pub balance: Amount,
    /// The hash of the chain's last block, or `undefined` if it has none yet.
    pub block_hash: Option<CryptoHash>,
    /// The height the next block will have, i.e. the number of blocks in the chain.
    pub next_block_height: BlockHeight,
    /// The hash of the execution state after the last block.
    pub state_hash: Option<CryptoHash>,
    /// The earliest timestamp the next block may carry.
    pub timestamp: Timestamp,
}

/// An application registered on a chain.
#[derive(serde::Serialize, tsify::Tsify)]
#[tsify(into_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationOverview {
    /// The application's ID, in the form [`Chain::application`] accepts.
    pub id: ApplicationId,
    /// The module, i.e. the contract and service bytecode, the application runs.
    pub module_id: ModuleId,
    /// The chain that created the application.
    pub creator_chain_id: ChainId,
    /// The height of the block that created the application.
    pub block_height: BlockHeight,
    /// The index of the application among those created in that block.
    pub application_index: u32,
    /// The application's instantiation parameters, hex-encoded.
    pub parameters: String,
    /// The applications this one depends on.
    pub required_application_ids: Vec<ApplicationId>,
    /// The ID of the blob holding this application's published `Formats`, in the form
    /// [`Chain::read_blob`] takes, or `undefined` if the module published none. Read it
    /// and pass the bytes to `Formats` to decode this application's payloads.
    #[tsify(optional, type = "BlobId")]
    pub formats_blob_id: Option<String>,
}

/// Header-level information about one confirmed block: enough to list a chain's blocks
/// without loading their bodies.
#[derive(serde::Serialize, tsify::Tsify)]
#[tsify(into_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct BlockSummary {
    /// The hash of this block, as accepted by [`Chain::block`].
    pub hash: CryptoHash,
    /// The block's height in its chain.
    pub height: BlockHeight,
    /// The epoch the block belongs to.
    pub epoch: u32,
    /// When the block was created.
    pub timestamp: Timestamp,
    /// The hash of the preceding block, or `undefined` for the first block.
    pub previous_block_hash: Option<CryptoHash>,
    /// The owner who signed for the block's operations, or `undefined` if the chain
    /// account signed.
    pub authenticated_owner: Option<AccountOwner>,
    /// The hash of the execution state after this block.
    pub state_hash: CryptoHash,
    /// How many operations the block executes.
    pub operation_count: usize,
    /// How many incoming message bundles the block receives.
    pub incoming_bundle_count: usize,
    /// How many messages the block sends.
    pub outgoing_message_count: usize,
    /// How many events the block emits.
    pub event_count: usize,
}

/// The applications returned by [`Chain::applications`].
///
/// `wasm-bindgen` can't return a bare `Vec` of a serialized type, so the list travels
/// as a newtype that TypeScript sees as a plain array.
#[derive(serde::Serialize, tsify::Tsify)]
#[tsify(into_wasm_abi)]
#[serde(transparent)]
pub struct Applications(pub Vec<ApplicationOverview>);

/// The block summaries returned by [`Chain::blocks`], as an array in TypeScript.
#[derive(serde::Serialize, tsify::Tsify)]
#[tsify(into_wasm_abi)]
#[serde(transparent)]
pub struct BlockSummaries(pub Vec<BlockSummary>);

/// One event on a chain's event stream.
#[derive(serde::Serialize, tsify::Tsify)]
#[tsify(into_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct EventEntry {
    /// The event's index within its stream.
    pub index: u32,
    /// The event's payload. Like operations and messages, it is the publishing
    /// application's own bytes; decode it with `Formats`.
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub event: Vec<u8>,
}

/// The events returned by [`Chain::events`], as an array in TypeScript.
#[derive(serde::Serialize, tsify::Tsify)]
#[tsify(into_wasm_abi)]
#[serde(transparent)]
pub struct EventEntries(pub Vec<EventEntry>);

/// Which events [`Chain::events`] should return.
#[derive(serde::Deserialize, tsify::Tsify)]
#[tsify(from_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct EventsQuery {
    /// The stream to read, in the form blocks print it: `"System:<name>"` or
    /// `"User:<application>:<name>"`, with names hex-encoded.
    #[tsify(type = "StreamId")]
    pub stream_id: String,
    /// The index to start at. Defaults to the start of the stream.
    #[serde(default)]
    pub start_index: Option<u32>,
}

/// Which blocks [`Chain::blocks`] should return.
#[derive(Default, serde::Deserialize, tsify::Tsify)]
#[tsify(from_wasm_abi)]
#[serde(default, rename_all = "camelCase")]
pub struct BlocksQuery {
    /// The hash of the block to start from. Defaults to the chain's last block.
    #[tsify(optional, type = "CryptoHash")]
    pub from: Option<String>,
    /// How many blocks to return at most. Defaults to 10.
    pub limit: Option<u32>,
}

/// A block together with its hash, which the block itself doesn't carry.
#[derive(serde::Serialize)]
struct HashedBlock<'a> {
    hash: CryptoHash,
    block: &'a linera_chain::block::Block,
}

/// Parses a block hash as JavaScript passes it: a string.
///
/// Identifiers are parsed here, in the body of the method that takes them, rather than
/// by `tsify` while it converts arguments: a failure there escapes an `async` method as
/// an unhandled exception, which no caller can catch, instead of rejecting its promise.
///
/// # Errors
/// If the string is not a valid hash.
fn block_hash_from_string(hash: &str) -> JsResult<CryptoHash> {
    Ok(hash.parse()?)
}

/// Converts a block height as JavaScript passes it — a number — into a [`BlockHeight`].
///
/// # Errors
/// If the number is not an exactly-representable non-negative integer.
fn block_height_from_number(height: f64) -> JsResult<BlockHeight> {
    if !height.is_finite() || height < 0.0 || height.fract() != 0.0 || height > MAX_SAFE_INTEGER {
        return Err(JsError::new("block height must be a non-negative integer"));
    }
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "checked above to be a non-negative integer below 2^53"
    )]
    let height = height as u64;
    Ok(BlockHeight(height))
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

    /// Gets the balance of `owner`'s account on this chain.
    ///
    /// This is the account a cross-chain transfer addressed to
    /// `Account { chain_id, owner }` credits, and is distinct from
    /// [`balance`](Self::balance), which reports the chain account. Block fees
    /// are paid from the chain account plus the account of whoever signed the
    /// block, so the two together are what a signer can actually spend.
    ///
    /// # Errors
    /// If the chain couldn't be established.
    #[wasm_bindgen(js_name = ownerBalance)]
    pub async fn owner_balance(&self, owner: AccountOwner) -> JsResult<String> {
        Ok(self
            .chain_client
            .query_owner_balance(owner)
            .await?
            .to_string())
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
        Ok(ownership.is_owner(&owner))
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

    /// Returns the local node's view of this chain: its epoch and balance, and the hash
    /// and height at which its block log currently ends.
    ///
    /// This reads the local node, so a chain that has never been synchronized reports an
    /// empty log. Call [`synchronize`](Self::synchronize) first to see the chain as the
    /// validators see it.
    ///
    /// # Errors
    /// If the chain information cannot be retrieved.
    #[wasm_bindgen(js_name = chainInfo)]
    pub async fn chain_info(&self) -> JsResult<ChainSummary> {
        let info = self.chain_client.chain_info().await?;
        Ok(ChainSummary {
            chain_id: info.chain_id,
            epoch: info.epoch.0,
            balance: info.chain_balance,
            block_hash: info.block_hash,
            next_block_height: info.next_block_height,
            state_hash: info.state_hash,
            timestamp: info.timestamp,
        })
    }

    /// Lists the applications registered on this chain.
    ///
    /// # Errors
    /// If the chain's state cannot be read.
    #[wasm_bindgen]
    pub async fn applications(&self) -> JsResult<Applications> {
        let applications = self
            .chain_client
            .chain_state_view()
            .await?
            .execution_state
            .list_applications()
            .await?;

        Ok(Applications(
            applications
                .into_iter()
                .map(|(id, description)| ApplicationOverview {
                    id,
                    module_id: description.module_id,
                    creator_chain_id: description.creator_chain_id,
                    block_height: description.block_height,
                    application_index: description.application_index,
                    parameters: hex::encode(&description.parameters),
                    required_application_ids: description.required_application_ids,
                    formats_blob_id: description
                        .module_id
                        .formats_blob_id()
                        .map(|blob_id| blob_id.to_string()),
                })
                .collect(),
        ))
    }

    /// Summarizes up to `limit` blocks, starting at `from` and walking back towards the
    /// first block of the chain. Both default to the chain's last block and 10
    /// respectively.
    ///
    /// The walk follows each block's predecessor, so paging through a chain means
    /// passing the last returned block's `previousBlockHash` as the next `from` — there
    /// is no offset to skip by.
    ///
    /// # Errors
    /// If a block cannot be read from the local node.
    #[wasm_bindgen]
    pub async fn blocks(&self, query: Option<BlocksQuery>) -> JsResult<BlockSummaries> {
        let BlocksQuery { from, limit } = query.unwrap_or_default();
        let limit = limit.unwrap_or(DEFAULT_BLOCKS_LIMIT);

        let mut next = match from {
            Some(hash) => Some(block_hash_from_string(&hash)?),
            None => self.chain_client.chain_info().await?.block_hash,
        };

        let mut summaries = Vec::new();
        for _ in 0..limit {
            let Some(hash) = next else {
                break;
            };
            let confirmed_block = self.chain_client.read_confirmed_block(hash).await?;
            let block = confirmed_block.block();
            next = block.header.previous_block_hash;
            summaries.push(BlockSummary {
                hash,
                height: block.header.height,
                epoch: block.header.epoch.0,
                timestamp: block.header.timestamp,
                previous_block_hash: block.header.previous_block_hash,
                authenticated_owner: block.header.authenticated_owner,
                state_hash: block.header.state_hash,
                operation_count: block.body.operations().count(),
                incoming_bundle_count: block.body.incoming_bundles().count(),
                outgoing_message_count: block.body.messages.iter().map(Vec::len).sum(),
                event_count: block.body.events.iter().map(Vec::len).sum(),
            });
        }

        Ok(BlockSummaries(summaries))
    }

    /// Returns the block with the given hash, or the chain's last block if no hash is
    /// given; `undefined` if the chain has no blocks.
    ///
    /// The result is `{ hash, block: { header, body } }`. Unlike the summary types
    /// above, the block is the protocol's own serialization, so its fields keep their
    /// Rust names — `chain_id`, `previous_block_hash`, `oracle_responses` — rather than
    /// being camel-cased. The header carries only the canonical fields; the content
    /// hashes it also holds in memory are recomputed from the body and are not part of
    /// this form. Operation, message and event payloads are the applications' own bytes,
    /// returned undecoded.
    ///
    /// # Errors
    /// If the block cannot be read from the local node or cannot be serialized.
    #[wasm_bindgen]
    pub async fn block(&self, hash: Option<String>) -> JsResult<JsValue> {
        let hash = match hash {
            Some(hash) => Some(block_hash_from_string(&hash)?),
            None => self.chain_client.chain_info().await?.block_hash,
        };
        let Some(hash) = hash else {
            return Ok(JsValue::UNDEFINED);
        };
        let confirmed_block = self.chain_client.read_confirmed_block(hash).await?;
        Ok(serde_wasm_bindgen::to_value(&HashedBlock {
            hash,
            block: confirmed_block.block(),
        })?)
    }

    /// Returns the hash of this chain's block at `height`, or `undefined` if the local
    /// node has no block at that height.
    ///
    /// Use it to jump to a known height, then walk back from there with
    /// [`blocks`](Self::blocks).
    ///
    /// # Errors
    /// If `height` is not a non-negative integer, or storage cannot be read.
    #[wasm_bindgen(js_name = blockHashAtHeight)]
    pub async fn block_hash_at_height(&self, height: f64) -> JsResult<Option<CryptoHash>> {
        let height = block_height_from_number(height)?;
        let hashes = self
            .chain_client
            .storage_client()
            .read_certificate_hashes_by_heights(self.chain_client.chain_id(), &[height])
            .await?;
        Ok(hashes.into_iter().next().flatten())
    }

    /// Returns the events this chain published on `streamId`, starting at `startIndex`.
    ///
    /// Events are also carried by the block that emitted them; this reads them by stream
    /// instead, which is how you follow one application's stream across blocks.
    ///
    /// # Errors
    /// If the stream ID is malformed, or the events cannot be read.
    #[wasm_bindgen]
    pub async fn events(&self, query: EventsQuery) -> JsResult<EventEntries> {
        let EventsQuery {
            stream_id,
            start_index,
        } = query;
        let stream_id: StreamId = stream_id
            .parse()
            .map_err(|error| JsError::new(&format!("invalid stream ID: {error}")))?;
        let events = self
            .chain_client
            .events_from_index(stream_id, start_index.unwrap_or(0))
            .await?;

        Ok(EventEntries(
            events
                .into_iter()
                .map(|event| EventEntry {
                    index: event.index,
                    event: event.event,
                })
                .collect(),
        ))
    }

    /// Returns the contents of the blob with the given ID, or `undefined` if the local
    /// node doesn't have it.
    ///
    /// `blobId` is the string form blocks print, `"<type>:<hash>"` — for example
    /// `"Data:0x…"` or `"ApplicationDescription:0x…"`. Blobs a block publishes are
    /// already inside its body; this reads the ones it only refers to, such as bytecode
    /// and application descriptions.
    ///
    /// # Errors
    /// If the blob ID is malformed, or storage cannot be read.
    #[wasm_bindgen(js_name = readBlob)]
    pub async fn read_blob(&self, id: &str) -> JsResult<Option<Vec<u8>>> {
        // `BlobId::from_str` fails with an `anyhow::Error`, which is not a `std::error::Error`
        // and so has no blanket conversion into `JsError`.
        let blob_id: BlobId = id
            .parse()
            .map_err(|error| JsError::new(&format!("invalid blob ID: {error}")))?;
        let blob = self
            .chain_client
            .storage_client()
            .read_blob(blob_id)
            .await?;
        Ok(blob.map(|blob| blob.bytes().to_vec()))
    }
}
