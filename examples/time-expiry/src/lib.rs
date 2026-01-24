// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Time Expiry Test Application */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi, TimeDelta};
use serde::{Deserialize, Serialize};

pub struct TimeExpiryAbi;

#[derive(Debug, Deserialize, Serialize)]
pub enum TimeExpiryOperation {
    /// Expire the operation after the given time delta from block timestamp.
    ExpireAfter(TimeDelta),
}

impl ContractAbi for TimeExpiryAbi {
    type Operation = TimeExpiryOperation;
    type Response = ();
}

impl ServiceAbi for TimeExpiryAbi {
    type Query = ();
    type QueryResponse = ();
}

#[cfg(not(target_arch = "wasm32"))]
pub mod formats {
    use linera_sdk::abis::formats::{BcsApplication, Formats};
    use serde_reflection::{Samples, Tracer, TracerConfig};

    use super::{TimeExpiryAbi, TimeExpiryOperation};

    /// The TimeExpiry application.
    pub struct TimeExpiryApplication;

    impl BcsApplication for TimeExpiryApplication {
        type Abi = TimeExpiryAbi;

        fn formats() -> serde_reflection::Result<Formats> {
            let mut tracer = Tracer::new(
                TracerConfig::default()
                    .record_samples_for_newtype_structs(true)
                    .record_samples_for_tuple_structs(true),
            );
            let samples = Samples::new();

            // Trace the ABI types
            let (operation, _) = tracer.trace_type::<TimeExpiryOperation>(&samples)?;
            let (response, _) = tracer.trace_type::<()>(&samples)?;
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
