// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Counter Example Application that does not use GraphQL */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct CounterNoGraphQlAbi;

impl ContractAbi for CounterNoGraphQlAbi {
    type Operation = CounterOperation;
    type Response = u64;
}

impl ServiceAbi for CounterNoGraphQlAbi {
    type Query = CounterRequest;
    type QueryResponse = u64;
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CounterRequest {
    Query,
    Increment(u64),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CounterOperation {
    Increment(u64),
}

#[cfg(not(target_arch = "wasm32"))]
pub mod formats {
    use linera_sdk::formats::{BcsApplication, Formats};
    use serde_reflection::{Samples, Tracer, TracerConfig};

    use super::{CounterNoGraphQlAbi, CounterOperation};

    /// The CounterNoGraphQl application.
    pub struct CounterApplication;

    impl BcsApplication for CounterApplication {
        type Abi = CounterNoGraphQlAbi;

        fn formats() -> serde_reflection::Result<Formats> {
            let mut tracer = Tracer::new(
                TracerConfig::default()
                    .record_samples_for_newtype_structs(true)
                    .record_samples_for_tuple_structs(true),
            );
            let samples = Samples::new();

            // Trace the ABI types
            let (operation, _) = tracer.trace_type::<CounterOperation>(&samples)?;
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
