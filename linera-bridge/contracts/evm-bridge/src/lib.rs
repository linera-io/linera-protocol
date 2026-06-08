// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI and types for the EVM→Linera bridge application.
//!
//! This bridge verifies MPT inclusion proofs for `DepositInitiated` events
//! on EVM and mints wrapped tokens on Linera via the wrapped-fungible app.

use async_graphql::{Request, Response};
pub use linera_bridge::{
    abi::{BridgeInstantiationArgument, BridgeMessage, BridgeOperation, BridgeParameters},
    proof::DepositKey,
};
use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};

pub struct EvmBridgeAbi;

impl ContractAbi for EvmBridgeAbi {
    type Operation = BridgeOperation;
    type Response = ();
}

impl ServiceAbi for EvmBridgeAbi {
    type Query = Request;
    type QueryResponse = Response;
}

#[cfg(not(target_arch = "wasm32"))]
pub mod formats {
    use linera_sdk::formats::{BcsApplication, Formats, TracerExt};
    use serde_reflection::{Samples, Tracer, TracerConfig};

    use super::{BridgeOperation, BridgeParameters, EvmBridgeAbi};

    /// The EvmBridge application.
    pub struct EvmBridgeApplication;

    impl BcsApplication for EvmBridgeApplication {
        type Abi = EvmBridgeAbi;

        fn formats() -> serde_reflection::Result<Formats> {
            let mut tracer = Tracer::new(
                TracerConfig::default()
                    .record_samples_for_newtype_structs(true)
                    .record_samples_for_tuple_structs(true),
            );
            let samples = Samples::new();

            // Trace the ABI types
            let operation = tracer.trace_stable_enum_type::<BridgeOperation>(&samples)?;
            let (response, _) = tracer.trace_type::<()>(&samples)?;
            let (message, _) = tracer.trace_type::<()>(&samples)?;
            let (event_value, _) = tracer.trace_type::<()>(&samples)?;

            // Trace additional supporting types (notably all enums) to populate the registry
            tracer.trace_type::<BridgeParameters>(&samples)?;

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
