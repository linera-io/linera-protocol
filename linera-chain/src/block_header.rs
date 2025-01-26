
use linera_base::{codec::{self, read_next, write_next, Codec}, crypto::CryptoHash, data_types::{BlockHeight, Timestamp}, identifiers::ChainId, version::Protocol};
use linera_execution::committee::Epoch;

use crate::{data_types::Block, ChainError};


/// A block-header
/// is to be derived from the block proposal
/// doesnt need to be propogated
#[derive(Clone, Debug, Hash)]
pub struct BlockHeader {
    /// Protocol mode
    pub version: Protocol,
    /// The chain to which this block belongs.
    pub chain_id: ChainId,
    /// The number identifying the current configuration.
    pub epoch: Epoch,
    /// The block height.
    pub height: BlockHeight,
    /// The timestamp when this block was created. This must be later than all messages received
    /// in this block, but no later than the current time.
    pub timestamp: Timestamp,

    /// Merkle tree root of Messages.
    pub messages_root: CryptoHash,
    // Merkle tree root of operations.
    pub operations_root: CryptoHash,

    /// Merkle root of Outcomes, Logs and Emissions after the execution of the transactions.
    pub outcomes_root: CryptoHash,
    /// Hash of the current execution state after the execution of this block.
    pub execution_state_hash: CryptoHash,

    /// Parallel block header hash.
    pub parent_block_header_hash: CryptoHash,
    /// Ancestor state index root
    /// Derived from merklized skip list
    /// Canonical block header hash.
    pub parent_block_header_root: CryptoHash,
}


impl Codec for BlockHeader {
    fn consensus_serialize<W: std::io::Write>(&self, fd: &mut W) -> Result<(), codec::Error> {
        write_next(fd, &(self.version as u8))?;
        write_next(fd, &self.chain_id)?;
        write_next(fd, &self.epoch)?;
        write_next(fd, &self.height)?;
        write_next(fd, &self.timestamp)?;
        write_next(fd, &self.messages_root)?;
        write_next(fd, &self.operations_root)?;
        write_next(fd, &self.outcomes_root)?;
        write_next(fd, &self.execution_state_hash)?;
        write_next(fd, &self.parent_block_header_hash)?;
        write_next(fd, &self.parent_block_header_root)?;
        Ok(())
    }

    fn consensus_deserialize<R: std::io::Read>(fd: &mut R) -> Result<Self, codec::Error> {
        let version: Protocol = read_next::<u8, R>(fd)?.try_into()?;
        let chain_id: ChainId = read_next(fd)?;
        let epoch: Epoch = read_next(fd)?;
        let height: BlockHeight = read_next(fd)?;
        let timestamp: Timestamp = read_next(fd)?;

        let messages_root: CryptoHash = read_next(fd)?;
        let operations_root: CryptoHash = read_next(fd)?;

        let outcomes_root: CryptoHash = read_next(fd)?;
        let execution_state_hash: CryptoHash = read_next(fd)?;
        let parent_block_header_hash: CryptoHash = read_next(fd)?;
        let parent_block_header_root: CryptoHash = read_next(fd)?;

        Ok(Self {
            version,
            chain_id,
            epoch,
            height,
            timestamp,
            messages_root,
            operations_root,
            outcomes_root,
            execution_state_hash,
            parent_block_header_hash,
            parent_block_header_root,
        })
    }
}

impl BlockHeader {
    pub fn try_append_to_chain<'a>(chain: impl Iterator<Item = &'a BlockHeader>, block: &Block) -> Result<Self, ChainError> {
        todo!()
    }
}
