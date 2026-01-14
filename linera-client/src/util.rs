// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, num::ParseIntError, str::FromStr};

use futures::future;
use linera_base::{
    crypto::CryptoError,
    data_types::{TimeDelta, Timestamp},
    identifiers::{ApplicationId, ChainId, GenericApplicationId},
    time::Duration,
};
use linera_core::{data_types::RoundTimeout, node::NotificationStream, worker::Reason};
use tokio_stream::StreamExt as _;

pub fn parse_json<T: serde::de::DeserializeOwned>(s: &str) -> anyhow::Result<T> {
    Ok(serde_json::from_str(s.trim())?)
}

pub fn parse_millis(s: &str) -> Result<Duration, ParseIntError> {
    Ok(Duration::from_millis(s.parse()?))
}

pub fn parse_secs(s: &str) -> Result<Duration, ParseIntError> {
    Ok(Duration::from_secs(s.parse()?))
}

pub fn parse_millis_delta(s: &str) -> Result<TimeDelta, ParseIntError> {
    Ok(TimeDelta::from_millis(s.parse()?))
}

pub fn parse_json_optional_millis_delta(s: &str) -> anyhow::Result<Option<TimeDelta>> {
    Ok(parse_json::<Option<u64>>(s)?.map(TimeDelta::from_millis))
}

pub fn parse_chain_set(s: &str) -> Result<HashSet<ChainId>, CryptoError> {
    match s.trim() {
        "" => Ok(HashSet::new()),
        s => s.split(",").map(ChainId::from_str).collect(),
    }
}

pub fn parse_app_set(s: &str) -> anyhow::Result<HashSet<GenericApplicationId>> {
    s.trim()
        .split(",")
        .map(|app_str| {
            GenericApplicationId::from_str(app_str)
                .or_else(|_| Ok(ApplicationId::from_str(app_str)?.into()))
        })
        .collect()
}

/// Returns after the specified time or if we receive a notification that a new round has started.
pub async fn wait_for_next_round(stream: &mut NotificationStream, timeout: RoundTimeout) {
    let mut stream = stream.filter(|notification| match &notification.reason {
        Reason::NewBlock { height, .. } => *height >= timeout.next_block_height,
        Reason::NewRound { round, .. } => *round > timeout.current_round,
        Reason::NewIncomingBundle { .. } | Reason::BlockExecuted { .. } => false,
    });
    future::select(
        Box::pin(stream.next()),
        Box::pin(linera_base::time::timer::sleep(
            timeout.timestamp.duration_since(Timestamp::now()),
        )),
    )
    .await;
}

macro_rules! impl_from_infallible {
    ($target:path) => {
        impl From<::std::convert::Infallible> for $target {
            fn from(infallible: ::std::convert::Infallible) -> Self {
                match infallible {}
            }
        }
    };
}

pub(crate) use impl_from_infallible;
