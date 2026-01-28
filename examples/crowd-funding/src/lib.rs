// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Crowd-funding Example Application */

use async_graphql::{Request, Response, SimpleObject};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{AccountOwner, Amount, ContractAbi, ServiceAbi, Timestamp},
};
use serde::{Deserialize, Serialize};

pub struct CrowdFundingAbi;

impl ContractAbi for CrowdFundingAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for CrowdFundingAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// The instantiation data required to create a crowd-funding campaign.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, SimpleObject)]
pub struct InstantiationArgument {
    /// The receiver of the pledges of a successful campaign.
    pub owner: AccountOwner,
    /// The deadline of the campaign, after which it can be cancelled if it hasn't met its target.
    pub deadline: Timestamp,
    /// The funding target of the campaign.
    pub target: Amount,
}

impl std::fmt::Display for InstantiationArgument {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(self).expect("Serialization failed")
        )
    }
}

/// Operations that can be executed by the application.
#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum Operation {
    /// Pledge some tokens to the campaign (from an account on the current chain to the campaign chain).
    Pledge { owner: AccountOwner, amount: Amount },
    /// Collect the pledges after the campaign has reached its target (campaign chain only).
    Collect,
    /// Cancel the campaign and refund all pledges after the campaign has reached its deadline (campaign chain only).
    Cancel,
}

/// Messages that can be exchanged across chains from the same application instance.
#[derive(Debug, Deserialize, Serialize)]
#[doc(hidden)]
pub enum Message {
    /// Pledge some tokens to the campaign (from an account on the receiver chain).
    PledgeWithAccount { owner: AccountOwner, amount: Amount },
}

#[cfg(not(target_arch = "wasm32"))]
pub mod formats {
    use linera_sdk::{
        formats::{BcsApplication, Formats},
        linera_base_types::AccountOwner,
    };
    use serde_reflection::{Samples, Tracer, TracerConfig};

    use super::{CrowdFundingAbi, InstantiationArgument, Message, Operation};

    /// The CrowdFunding application.
    pub struct CrowdFundingApplication;

    impl BcsApplication for CrowdFundingApplication {
        type Abi = CrowdFundingAbi;

        fn formats() -> serde_reflection::Result<Formats> {
            let mut tracer = Tracer::new(
                TracerConfig::default()
                    .record_samples_for_newtype_structs(true)
                    .record_samples_for_tuple_structs(true),
            );
            let samples = Samples::new();

            // Trace the ABI types
            let (operation, _) = tracer.trace_type::<Operation>(&samples)?;
            let (response, _) = tracer.trace_type::<()>(&samples)?;
            let (message, _) = tracer.trace_type::<Message>(&samples)?;
            let (event_value, _) = tracer.trace_type::<()>(&samples)?;

            // Trace additional supporting types (notably all enums) to populate the registry
            tracer.trace_type::<InstantiationArgument>(&samples)?;
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
