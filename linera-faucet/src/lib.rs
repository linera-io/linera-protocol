use linera_base::{
    crypto::CryptoHash,
    identifiers::{ChainId, MessageId},
};

/// The result of a successful `claim` mutation.
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
pub struct ClaimOutcome {
    /// The ID of the message that created the new chain.
    pub message_id: MessageId,
    /// The ID of the new chain.
    pub chain_id: ChainId,
    /// The hash of the parent chain's certificate containing the `OpenChain` operation.
    pub certificate_hash: CryptoHash,
}
