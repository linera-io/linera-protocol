use crate::{
    committee::{Committee, ValidatorState},
    crypto::CryptoHash,
    data_types::{BlockHeight, ChainDescription, ChainId, Epoch, Owner, ValidatorName},
};
use async_graphql::{scalar, Object};
use std::collections::BTreeMap;

scalar!(BlockHeight);
scalar!(ChainDescription);
scalar!(ChainId);
scalar!(Epoch);
scalar!(CryptoHash);
scalar!(Owner);
scalar!(ValidatorName);

#[Object]
impl Committee {
    #[graphql(derived(name = "validators"))]
    async fn _validators(&self) -> &BTreeMap<ValidatorName, ValidatorState> {
        &self.validators
    }

    #[graphql(derived(name = "total_votes"))]
    async fn _total_votes(&self) -> u64 {
        self.total_votes
    }

    #[graphql(derived(name = "quorum_threshold"))]
    async fn _quorum_threshold(&self) -> u64 {
        self.quorum_threshold
    }

    #[graphql(derived(name = "validity_threshold"))]
    async fn _validity_threshold(&self) -> u64 {
        self.validity_threshold
    }
}
