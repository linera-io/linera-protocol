// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Counter Example Application that does not use GraphQL */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct CallCounterAbi;

impl ContractAbi for CallCounterAbi {
    type Operation = CallCounterOperation;
    type Response = u64;
}

impl ServiceAbi for CallCounterAbi {
    type Query = CallCounterRequest;
    type QueryResponse = u64;
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CallCounterRequest {
    Query,
    TestCallAddress,
    ContractTestCallAddress,
    Increment(u64),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CallCounterOperation {
    TestCallAddress,
    Increment(u64),
}

#[cfg(not(target_arch = "wasm32"))]
pub mod formats {
    use linera_sdk::formats::{BcsApplication, Formats};
    use serde_reflection::{Samples, Tracer, TracerConfig};

    use super::{CallCounterAbi, CallCounterOperation, CallCounterRequest};

    /// The CallEvmCounter application.
    pub struct CallEvmCounterApplication;

    impl BcsApplication for CallEvmCounterApplication {
        type Abi = CallCounterAbi;

        fn formats() -> serde_reflection::Result<Formats> {
            let mut tracer = Tracer::new(
                TracerConfig::default()
                    .record_samples_for_newtype_structs(true)
                    .record_samples_for_tuple_structs(true),
            );
            let samples = Samples::new();

            // Trace the ABI types
            let (operation, _) = tracer.trace_type::<CallCounterOperation>(&samples)?;
            let (response, _) = tracer.trace_type::<u64>(&samples)?;
            let (message, _) = tracer.trace_type::<()>(&samples)?;
            let (event_value, _) = tracer.trace_type::<()>(&samples)?;

            // Trace additional supporting types
            tracer.trace_type::<CallCounterRequest>(&samples)?;

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
