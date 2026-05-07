// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Linera-side monitoring: scans for BurnEvent stream events (auto-burns),
//! forwards certificates to EVM, checks EVM for already-processed burns via
//! `FungibleBridge.processedBurns`, and retries unforwarded burns.

use std::{sync::Arc, time::Duration};

use alloy::{primitives::Address, providers::Provider};
use tokio::sync::{Notify, RwLock};

use super::{MonitorState, PendingBurn};
use crate::relay::{
    self,
    evm::EvmClient,
    linera::{find_burn_events, LineraClient},
};

/// Background task that scans Linera block history for BurnEvent stream
/// events and checks EVM for completion. Newly-discovered burns are written
/// to `MonitorState` (and SQLite) and the consumer is woken via `notify`.
pub async fn linera_scan_loop<E: linera_core::environment::Environment + 'static>(
    monitor: Arc<RwLock<MonitorState>>,
    evm_client: Arc<EvmClient<impl Provider + 'static>>,
    linera_client: Arc<LineraClient<E>>,
    burn_notify: Arc<Notify>,
    scan_interval: Duration,
) {
    loop {
        let (scan_result, completion_result) = tokio::join!(
            linera_scan_iteration(&monitor, &linera_client, &burn_notify),
            check_burn_completion(&monitor, &evm_client),
        );

        if let Err(error) = scan_result {
            tracing::warn!(?error, "Linera scan iteration failed");
        }
        if let Err(error) = completion_result {
            tracing::warn!(?error, "Burn completion check failed");
        }

        let summary = monitor.read().await.status_summary();
        tracing::trace!(
            pending = summary.burns_pending,
            completed = summary.burns_forwarded,
            last_height = %summary.last_scanned_linera_height,
            "Linera burn scan complete"
        );

        tokio::time::sleep(scan_interval).await;
    }
}

/// Drains `MonitorState.burns` for items ready for retry, processing one at a
/// time. Sleeps on `notify` (woken by the scanner) or on `poll_interval`
/// (whichever comes first) when nothing is ready.
pub(crate) async fn process_pending_burns<E: linera_core::environment::Environment + 'static>(
    monitor: &RwLock<MonitorState>,
    evm_client: &EvmClient<impl Provider>,
    linera_client: &LineraClient<E>,
    notify: &Notify,
    poll_interval: Duration,
    max_retries: u32,
) -> anyhow::Result<()> {
    loop {
        let pending = monitor.read().await.next_burn_for_retry(max_retries);
        let Some(pending) = pending else {
            tracing::trace!(
                ?poll_interval,
                "Linera burns processor sleeping until notified or poll interval elapses"
            );
            tokio::select! {
                _ = notify.notified() => {
                    tracing::trace!("Linera burns processor notified about new pending item");
                }
                _ = tokio::time::sleep(poll_interval) => {}
            }
            continue;
        };

        let credit_height = pending.height;
        let burn_index = pending.burn_index;
        tracing::info!(?credit_height, burn_index, "Processing burn...");

        // Read the certificate at the burn's block height (already contains the auto-burn).
        let cert = match async {
            linera_client.sync().await?;
            let info = linera_client.chain_info().await?;
            let mut hash = info.block_hash;
            loop {
                let Some(h) = hash else {
                    anyhow::bail!("Block at height {} not found", credit_height);
                };
                let c = linera_client.read_certificate(h).await?;
                if c.block().header.height == credit_height {
                    break Ok(c);
                }
                hash = c.block().header.previous_block_hash;
            }
        }
        .await
        {
            Ok(cert) => cert,
            Err(e) => {
                tracing::warn!(
                    ?credit_height,
                    burn_index,
                    "Failed to read certificate: {e:#}"
                );
                monitor
                    .write()
                    .await
                    .mark_burn_retried(credit_height, burn_index);
                continue;
            }
        };

        // Persist raw BCS cert bytes so burns can be replayed without the relayer.
        let cert_bytes =
            bcs::to_bytes(&cert).expect("failed to BCS-serialize ConfirmedBlockCertificate");
        if let Some(db) = monitor.read().await.db() {
            if let Err(e) = db
                .store_burn_raw(credit_height, burn_index, &cert_bytes)
                .await
            {
                tracing::warn!(
                    ?credit_height,
                    burn_index,
                    "Failed to store burn raw bytes: {e:#}"
                );
            }
        }

        // Forward cert to EVM.
        let completed = match evm_client.forward_cert(&cert).await {
            Ok(()) => {
                tracing::info!(?credit_height, burn_index, "Burn forwarded to EVM");
                true
            }
            Err(e) => {
                let msg = format!("{e:#}");
                if msg.contains("already verified") {
                    tracing::trace!(?credit_height, burn_index, "Block already verified on EVM");
                    true
                } else {
                    tracing::warn!(?credit_height, burn_index, "EVM forwarding failed: {e:#}");
                    monitor
                        .write()
                        .await
                        .mark_burn_retried(credit_height, burn_index);
                    false
                }
            }
        };

        if completed {
            monitor
                .write()
                .await
                .complete_burn(credit_height, burn_index)
                .await;
            relay::update_balance_metrics(evm_client, linera_client).await;
        }
    }
}

async fn linera_scan_iteration<E: linera_core::environment::Environment>(
    monitor: &RwLock<MonitorState>,
    linera_client: &LineraClient<E>,
    notify: &Notify,
) -> anyhow::Result<()> {
    let last_height = monitor.read().await.last_scanned_linera_height;

    linera_client.sync().await?;
    let info = linera_client.chain_info().await?;
    let current_height = info.next_block_height;
    if current_height.0 == 0 || current_height <= last_height {
        return Ok(());
    }

    let fungible_app_id = linera_client.fungible_app_id();

    let mut blocks = Vec::new();
    let mut hash = info.block_hash;
    while let Some(h) = hash {
        let block = linera_client.read_confirmed_block(h).await?;
        let height = block.block().header.height;
        if height < last_height {
            break;
        }
        hash = block.block().header.previous_block_hash;
        blocks.push(block);
    }
    blocks.reverse();

    let mut new_burns = Vec::new();
    for block in &blocks {
        let height = block.block().header.height;
        let burn_events = find_burn_events(&block.block().body.events, fungible_app_id);
        for (burn_index, burn_event) in burn_events.into_iter().enumerate() {
            new_burns.push((
                height,
                burn_index,
                Address::from(burn_event.target),
                burn_event.amount,
            ));
        }
    }

    let mut tracked_any = false;
    for (height, burn_index, recipient, amount) in &new_burns {
        tracing::info!(?height, burn_index, %recipient, %amount, "Discovered burn");
        let was_new = monitor
            .write()
            .await
            .track_burn(PendingBurn {
                height: *height,
                burn_index: *burn_index,
                evm_recipient: *recipient,
                amount: *amount,
            })
            .await;
        tracked_any |= was_new;
    }

    {
        let mut state = monitor.write().await;
        state.last_scanned_linera_height = current_height;
    }
    crate::relay::metrics::set_last_scanned_linera_height(current_height.0);

    if tracked_any {
        notify.notify_one();
    }
    Ok(())
}

/// Detects burns that were already released on EVM (by a prior `addBlock`
/// from us before a crash, or by another relayer). Queries the per-burn
/// `processedBurns` mapping on FungibleBridge so the answer is intrinsic to
/// the burn and survives any future change to the contract's per-block
/// processing semantics. Marks each such burn complete *without* sending an
/// EVM transaction, so `process_pending_burns` doesn't waste gas re-submitting
/// blocks that would just revert with "block already verified".
async fn check_burn_completion(
    monitor: &RwLock<MonitorState>,
    evm_client: &EvmClient<impl Provider>,
) -> anyhow::Result<()> {
    let pending: Vec<(linera_base::data_types::BlockHeight, usize)> = {
        let state = monitor.read().await;
        state
            .pending_burns()
            .into_iter()
            .map(|b| (b.value.height, b.value.burn_index))
            .collect()
    };

    if pending.is_empty() {
        return Ok(());
    }

    for (height, burn_index) in pending {
        match evm_client.is_burn_processed(height, burn_index).await {
            Ok(true) => {
                monitor
                    .write()
                    .await
                    .complete_burn(height, burn_index)
                    .await;
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    ?height,
                    burn_index,
                    "Failed to query processedBurns for completion check: {e:#}"
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;
    use alloy_sol_types::sol;
    use linera_base::{
        crypto::{CryptoHash, TestString, ValidatorSecretKey},
        data_types::{Amount, BlockHeight},
    };
    use revm::{database::CacheDB, primitives::Address};

    use crate::{evm::microchain::addBlockCall, test_helpers::*};

    sol! {
        function isBurnProcessed(uint64 height, uint256 burnIndex) external view returns (bool);
        function transfer(address to, uint256 amount) external returns (bool);
    }

    /// End-to-end check that `_onBlock` records each released burn under a
    /// key that `isBurnProcessed(height, burnIndex)` can find. Submits a
    /// cert with two burn events in one transaction — the multi-burn-in-
    /// one-block case that block-level dedup couldn't distinguish — and
    /// verifies both `(height, 0)` and `(height, 1)` flip to true.
    #[test]
    fn is_burn_processed_returns_true_after_release() {
        let mut db = CacheDB::default();
        let deployer = Address::ZERO;
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        let validator_addr = validator_evm_address(&public);
        let admin_chain_id = test_admin_chain_id();
        let light_client = deploy_light_client(
            &mut db,
            deployer,
            &[validator_addr],
            &[1],
            admin_chain_id,
            0,
        );

        // Fund the bridge so `token.transfer` inside `_onBlock` succeeds —
        // a failed transfer reverts the whole `addBlock` and `processedBurns`
        // would never be written, masking the property under test.
        let initial_supply = U256::from(1_000_000_000_000_000_000_000_000u128);
        let token = deploy_mock_erc20(&mut db, deployer, initial_supply);

        let chain_id = CryptoHash::new(&TestString::new("test_chain"));
        let app_id = CryptoHash::new(&TestString::new("fungible_app"));
        let bridge =
            deploy_fungible_bridge(&mut db, deployer, light_client, chain_id, token, app_id);
        call_contract(
            &mut db,
            deployer,
            token,
            &transferCall {
                to: bridge,
                amount: initial_supply,
            },
        );

        let height = BlockHeight(1);
        let events = vec![vec![
            burn_event(app_id, [0xAA; 20], Amount::from_attos(100), 0),
            burn_event(app_id, [0xBB; 20], Amount::from_attos(200), 1),
        ]];

        // Both burns are unset before the block is processed.
        for burn_index in [0u64, 1] {
            let (set, _, _) = call_contract(
                &mut db,
                deployer,
                bridge,
                &isBurnProcessedCall {
                    height: height.0,
                    burnIndex: U256::from(burn_index),
                },
            );
            assert!(
                !set,
                "isBurnProcessed(height, {burn_index}) should be false before addBlock"
            );
        }

        let cert = create_certificate_with_events(&secret, &public, chain_id, height, events);
        call_contract(
            &mut db,
            deployer,
            bridge,
            &addBlockCall {
                data: bcs::to_bytes(&cert).unwrap().into(),
            },
        );

        // Both burns now report processed.
        for burn_index in [0u64, 1] {
            let (set, _, _) = call_contract(
                &mut db,
                deployer,
                bridge,
                &isBurnProcessedCall {
                    height: height.0,
                    burnIndex: U256::from(burn_index),
                },
            );
            assert!(
                set,
                "isBurnProcessed(height, {burn_index}) must be true after addBlock"
            );
        }

        // A burn at a height that wasn't processed must remain unset.
        let (other, _, _) = call_contract(
            &mut db,
            deployer,
            bridge,
            &isBurnProcessedCall {
                height: 2,
                burnIndex: U256::ZERO,
            },
        );
        assert!(
            !other,
            "isBurnProcessed for an unrelated (height, burnIndex) must be false"
        );
    }
}
