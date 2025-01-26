use linera_base::{codec::{read_next, write_next, Codec}, crypto::CryptoHash, data_types::{BlockHeight, OracleResponse}, identifiers::ChainId, version::Protocol};
use serde::{Deserialize, Serialize};
use linera_base::bcs;
use crate::data_types::{EventRecord, IncomingBundle, OutgoingMessage};

type TransactionVersion = Protocol;

#[derive(Debug, Serialize, Deserialize)]
pub enum TransactionPayload {
    Message(Message),
    Operation(Operation),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub index: u32, // within block message index
    pub nonce: u64, // between blocks message index
    pub message: IncomingBundle,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Operation {
    pub index: u32,
    pub nonce: u64,
    pub operation: linera_execution::Operation,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Transaction {
    pub version: TransactionVersion,
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub index: u32, // local index
    pub nonce: u64, // global index
    pub payload: TransactionPayload,
}

impl Codec for Transaction {
    fn consensus_serialize<W: std::io::Write>(&self, fd: &mut W) -> Result<(), linera_base::codec::Error>
    where
        Self: Sized {
        write_next(fd, &(self.version as u8))?;
        write_next(fd, &self.chain_id)?;
        write_next(fd, &self.height)?;
        write_next(fd, &self.index)?;
        write_next(fd, &self.nonce)?;
        write_next(fd, &self.payload)?;
        Ok(())
    }

    fn consensus_deserialize<R: std::io::Read>(fd: &mut R) -> Result<Self, linera_base::codec::Error>
    where
        Self: Sized {
        let version: Protocol = read_next::<u8, R>(fd)?.try_into()?;
        let chain_id: ChainId = read_next(fd)?;
        let height: BlockHeight = read_next(fd)?;
        let index: u32 = read_next(fd)?;
        let nonce: u64 = read_next(fd)?;
        let payload: TransactionPayload = read_next(fd)?;

        Ok(Self {
            version,
            chain_id,
            height,
            index,
            nonce,
            payload,
        })
    }
}

impl Codec for TransactionPayload {
    fn consensus_serialize<W: std::io::Write>(&self, fd: &mut W) -> Result<(), linera_base::codec::Error>
    where
        Self: Sized
    {
        match self {
            TransactionPayload::Message(message) => {
                write_next(fd, &0u8)?;
                write_next(fd, &message.index)?;
                write_next(fd, &message.nonce)?;
                write_next(fd, &message.message)?;
            },

            TransactionPayload::Operation(operation) => {
                write_next(fd, &1u8)?;
                write_next(fd, &operation.index)?;
                write_next(fd, &operation.nonce)?;
                write_next(fd, &operation.operation)?;
            },
        }

        Ok(())
    }

    fn consensus_deserialize<R: std::io::Read>(fd: &mut R) -> Result<Self, linera_base::codec::Error>
    where
        Self: Sized
    {
        let variant: u8 = read_next(fd)?;
        let index: u32 = read_next(fd)?;
        let nonce: u64 = read_next(fd)?;

        if 0 == variant {
            Ok(Self::Message(
                Message { index, nonce, message: read_next::<IncomingBundle, R>(fd)? }
            ))
        } else if 1 == variant {
            Ok(Self::Operation(
                Operation { index, nonce, operation: read_next::<linera_execution::Operation, R>(fd)? }
            ))
        } else {
            panic!("not implemented")
        }
    }
}

impl Codec for IncomingBundle {
    fn consensus_serialize<W: std::io::Write>(&self, fd: &mut W) -> Result<(), linera_base::codec::Error>
    where
        Self: Sized
    {
        bcs::serialize_into(fd, self)
            .map_err(|e| e.to_string())
            .map_err(linera_base::codec::Error::GenericError)
    }

    fn consensus_deserialize<R: std::io::Read>(fd: &mut R) -> Result<Self, linera_base::codec::Error>
    where
        Self: Sized
    {
        bcs::from_reader(fd)
            .map_err(|e| e.to_string())
            .map_err(linera_base::codec::Error::GenericError)
    }
}

#[derive(Debug, Hash, Serialize, Deserialize)]
pub struct TransactionOutcome {
    pub version: Protocol,
    pub chain_id: ChainId,
    pub block_height: u64,
    pub transaction_hash: CryptoHash,
    pub transaction_index: u32,
    pub kind: OutcomeKind,
}

#[derive(Debug, Hash, Serialize, Deserialize)]
pub enum OutcomeKind {
    OracleResponse(OracleResponse),
    Message(OutgoingMessage),
    EventRecord(EventRecord),
}

impl Codec for OutcomeKind {
    fn consensus_serialize<W: std::io::Write>(&self, fd: &mut W) -> Result<(), linera_base::codec::Error>
    where
        Self: Sized {
        bcs::serialize_into(fd, self)
            .map_err(|e| e.to_string())
            .map_err(linera_base::codec::Error::GenericError)
    }

    fn consensus_deserialize<R: std::io::Read>(fd: &mut R) -> Result<Self, linera_base::codec::Error>
    where
        Self: Sized {
        bcs::from_reader(fd)
            .map_err(|e| e.to_string())
            .map_err(linera_base::codec::Error::GenericError)
    }
}

impl Codec for TransactionOutcome {
    fn consensus_serialize<W: std::io::Write>(&self, fd: &mut W) -> Result<(), linera_base::codec::Error>
    where
        Self: Sized {
        write_next(fd, &(self.version as u8))?;
        write_next(fd, &self.chain_id)?;
        write_next(fd, &self.block_height)?;
        write_next(fd, &self.transaction_hash)?;
        write_next(fd, &self.transaction_index)?;
        write_next(fd, &self.kind)?;
        Ok(())
    }

    fn consensus_deserialize<R: std::io::Read>(fd: &mut R) -> Result<Self, linera_base::codec::Error>
    where
        Self: Sized {
        let version: Protocol = read_next::<u8, R>(fd)?.try_into()?;
        let chain_id: ChainId = read_next(fd)?;
        let block_height: u64 = read_next(fd)?;
        let transaction_hash: CryptoHash = read_next(fd)?;
        let transaction_index: u32 = read_next(fd)?;
        let kind: OutcomeKind = read_next(fd)?;

        Ok(Self {
            version,
            chain_id,
            block_height,
            transaction_hash,
            transaction_index,
            kind,
        })
    }
}

