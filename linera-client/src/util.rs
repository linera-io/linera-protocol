// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, num::ParseIntError, str::FromStr, time::Duration};

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
        Reason::NewIncomingBundle { .. } => false,
    });
    future::select(
        Box::pin(stream.next()),
        Box::pin(tokio::time::sleep(
            timeout.timestamp.duration_since(Timestamp::now()),
        )),
    )
    .await;
}

macro_rules! impl_from_dynamic {
    ($target:ty : $variant:ident, $source:ty) => {
        impl From<$source> for $target {
            fn from(error: $source) -> Self {
                <$target>::$variant(Box::new(error))
            }
        }
    };
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

pub(crate) use impl_from_dynamic;
pub(crate) use impl_from_infallible;

pub mod serde_btreemap_keys_as_strings {
    use std::{collections::BTreeMap, fmt::Display, str::FromStr};

    use serde::{self, Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<K, V, S>(map: &BTreeMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        K: Display,
        V: Serialize,
        S: Serializer,
    {
        let string_map: BTreeMap<String, &V> =
            map.iter().map(|(k, v)| (k.to_string(), v)).collect();
        string_map.serialize(serializer)
    }

    pub fn deserialize<'de, K, V, D>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error>
    where
        K: FromStr + Ord,
        <K as FromStr>::Err: Display,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        let string_map: BTreeMap<String, V> = BTreeMap::deserialize(deserializer)?;
        string_map
            .into_iter()
            .map(|(k, v)| {
                k.parse()
                    .map(|key| (key, v))
                    .map_err(serde::de::Error::custom)
            })
            .collect()
    }
}
