// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, num::ParseIntError, str::FromStr, time::Duration};

use anyhow::Result;
use futures::future;
use linera_base::{
    crypto::CryptoError,
    data_types::{TimeDelta, Timestamp},
    identifiers::ChainId,
};
use linera_core::{data_types::RoundTimeout, node::NotificationStream, worker::Reason};
use tokio_stream::StreamExt as _;

pub fn parse_millis(s: &str) -> Result<Duration, ParseIntError> {
    Ok(Duration::from_millis(s.parse()?))
}

pub fn parse_millis_delta(s: &str) -> Result<TimeDelta, ParseIntError> {
    Ok(TimeDelta::from_millis(s.parse()?))
}

pub fn parse_chain_set(s: &str) -> Result<HashSet<ChainId>, CryptoError> {
    match s.trim() {
        "" => Ok(HashSet::new()),
        s => s.split(",").map(ChainId::from_str).collect(),
    }
}

/// Returns after the specified time or if we receive a notification that a new round has started.
pub async fn wait_for_next_round(stream: &mut NotificationStream, timeout: RoundTimeout) {
    let mut stream = stream.filter(|notification| match &notification.reason {
        Reason::NewBlock { height, .. } => *height >= timeout.next_block_height,
        Reason::NewRound { round, .. } => *round > timeout.current_round,
        Reason::NewIncomingMessage { .. } => false,
    });
    future::select(
        Box::pin(stream.next()),
        Box::pin(tokio::time::sleep(
            timeout.timestamp.duration_since(Timestamp::now()),
        )),
    )
    .await;
}
