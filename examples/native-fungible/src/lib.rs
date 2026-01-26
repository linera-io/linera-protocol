// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Native Fungible Token Example Application */

use async_graphql::SimpleObject;
use linera_sdk::linera_base_types::{AccountOwner, Amount};
use serde::{Deserialize, Serialize};

pub const TICKER_SYMBOL: &str = "NAT";

#[derive(Deserialize, SimpleObject)]
pub struct AccountEntry {
    pub key: AccountOwner,
    pub value: Amount,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    Notify,
}

#[cfg(not(target_arch = "wasm32"))]
pub mod formats {
    use linera_sdk::{
        abis::fungible::{FungibleResponse, NativeFungibleOperation, NativeFungibleTokenAbi},
        formats::{BcsApplication, Formats},
        linera_base_types::AccountOwner,
    };
    use serde_reflection::{Samples, Tracer, TracerConfig};

    use super::{AccountEntry, Message};

    /// The NativeFungible application.
    pub struct NativeFungibleApplication;

    impl BcsApplication for NativeFungibleApplication {
        type Abi = NativeFungibleTokenAbi;

        fn formats() -> serde_reflection::Result<Formats> {
            let mut tracer = Tracer::new(
                TracerConfig::default()
                    .record_samples_for_newtype_structs(true)
                    .record_samples_for_tuple_structs(true),
            );
            let samples = Samples::new();

            // Trace the ABI types
            let (operation, _) = tracer.trace_type::<NativeFungibleOperation>(&samples)?;
            let (response, _) = tracer.trace_type::<FungibleResponse>(&samples)?;
            let (message, _) = tracer.trace_type::<Message>(&samples)?;
            let (event_value, _) = tracer.trace_type::<()>(&samples)?;

            // Trace additional supporting types (notably all enums) to populate the registry
            tracer.trace_type::<AccountEntry>(&samples)?;
            tracer.trace_type::<AccountOwner>(&samples)?;

            let registry = tracer.registry()?;

            Ok(Formats {
                registry,
                operation,
                response,
                message,
                event_value,
            })
        }
    }
}
