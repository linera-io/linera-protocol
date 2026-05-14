// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Counter Example Application */

use async_graphql::{Request, Response};
use linera_sdk::{
    formats::{StableEnumDeserialize, StableEnumSerialize, StableEnumTrace},
    linera_base_types::{ContractAbi, ServiceAbi},
};

pub struct CounterAbi;

#[derive(Debug, StableEnumSerialize, StableEnumDeserialize, StableEnumTrace)]
pub enum CounterOperation {
    /// Increment the counter by the given value
    Increment { value: u64 },
}

impl ContractAbi for CounterAbi {
    type Operation = CounterOperation;
    type Response = u64;
}

impl ServiceAbi for CounterAbi {
    type Query = Request;
    type QueryResponse = Response;
}

#[cfg(not(target_arch = "wasm32"))]
pub mod formats {
    use linera_sdk::formats::{BcsApplication, Formats, TracerExt};
    use serde_reflection::{Samples, Tracer, TracerConfig};

    use super::{CounterAbi, CounterOperation};

    /// The Counter application.
    pub struct CounterApplication;

    impl BcsApplication for CounterApplication {
        type Abi = CounterAbi;

        fn formats() -> serde_reflection::Result<Formats> {
            let mut tracer = Tracer::new(
                TracerConfig::default()
                    .record_samples_for_newtype_structs(true)
                    .record_samples_for_tuple_structs(true),
            );
            let samples = Samples::new();

            // Trace the ABI types
            let operation = tracer.trace_stable_enum_type::<CounterOperation>(&samples)?;
            let (response, _) = tracer.trace_type::<u64>(&samples)?;
            let (message, _) = tracer.trace_type::<()>(&samples)?;
            let (event_value, _) = tracer.trace_type::<()>(&samples)?;

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
