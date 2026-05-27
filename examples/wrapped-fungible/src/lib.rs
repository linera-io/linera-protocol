// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI and types for a wrapped (bridged) fungible token with Mint/Burn support.

pub use linera_sdk::abis::wrapped_fungible::*;

#[cfg(not(target_arch = "wasm32"))]
pub mod formats {
    use linera_sdk::{
        abis::wrapped_fungible::{
            BurnEvent, FungibleResponse, WrappedFungibleOperation, WrappedFungibleTokenAbi,
            WrappedParameters,
        },
        formats::{BcsApplication, Formats, TracerExt},
        linera_base_types::{Account, AccountOwner},
    };
    use serde_reflection::{Samples, Tracer, TracerConfig};

    use super::Message;

    /// The WrappedFungible application.
    pub struct WrappedFungibleApplication;

    impl BcsApplication for WrappedFungibleApplication {
        type Abi = WrappedFungibleTokenAbi;

        fn formats() -> serde_reflection::Result<Formats> {
            let mut tracer = Tracer::new(
                TracerConfig::default()
                    .record_samples_for_newtype_structs(true)
                    .record_samples_for_tuple_structs(true),
            );
            let samples = Samples::new();

            // Trace the ABI types
            let operation = tracer.trace_stable_enum_type::<WrappedFungibleOperation>(&samples)?;
            let response = tracer.trace_stable_enum_type::<FungibleResponse>(&samples)?;
            let (message, _) = tracer.trace_type::<Message>(&samples)?;
            let (event_value, _) = tracer.trace_type::<BurnEvent>(&samples)?;

            // Trace additional supporting types (notably all enums) to populate the registry
            tracer.trace_type::<WrappedParameters>(&samples)?;
            tracer.trace_type::<Account>(&samples)?;
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
