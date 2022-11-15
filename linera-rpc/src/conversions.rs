use thiserror::Error;
use linera_base::crypto::Signature;

use crate::grpc_network::grpc_network::{CrossChainRequest as CrossChainRequestRpc, medium, NameSignaturePair};
use linera_core::messages::CrossChainRequest;

use crate::grpc_network::grpc_network::BlockProposal as BlockProposalRpc;
use linera_chain::messages::BlockProposal;

use crate::grpc_network::grpc_network::Certificate as CertificateRpc;
use linera_chain::messages::Certificate;

use crate::grpc_network::grpc_network::ChainInfoQuery as ChainInfoQueryRpc;
use linera_core::messages::ChainInfoQuery;

use crate::grpc_network::grpc_network::Origin as OriginRpc;
use linera_base::messages::{Origin, ValidatorName};

use crate::grpc_network::grpc_network::Medium as MediumRpc;
use linera_base::messages::Medium;

use crate::grpc_network::grpc_network::BlockHeightRange as BlockHeaightRangeRPC;
use linera_core::messages::BlockHeightRange;

use crate::grpc_network::grpc_network::ApplicationId as ApplicationIdRPC;
use linera_base::messages::ApplicationId;

use crate::grpc_network::grpc_network::ChainId as ChainIdRPC;
use linera_base::messages::ChainId;

#[derive(Error, Debug)]
enum ProtoConversionError {
    #[error("BCS serialization / deserialization error.")]
    BcsError(#[from] bcs::Error),
}

impl TryFrom<BlockProposal> for BlockProposalRpc {
    type Error = ProtoConversionError;

    fn try_from(block_proposal: BlockProposal) -> Result<Self, Self::Error> {
        Ok(Self {
            content: bcs::to_bytes(&block_proposal.content)?,
            owner: Some(block_proposal.owner.into()),
            signature: Some(block_proposal.signature.into()),
        })
    }
}

impl TryFrom<Certificate> for CertificateRpc {
    type Error = ProtoConversionError;

    fn try_from(certificate: Certificate) -> Result<Self, Self::Error> {
        Ok(Self {
            value: bcs::to_bytes(&certificate.value)?,
            signatures: certificate.signatures.into()
        })
    }
}

impl TryFrom<ChainInfoQuery> for ChainInfoQueryRpc {
    type Error = ProtoConversionError;

    fn try_from(chain_info_query: ChainInfoQuery) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_id: Some(chain_info_query.chain_id.into()),
            block_height: Some(chain_info_query.test_next_block_height.into()),
            request_committees: chain_info_query.request_committees,
            request_pending_messages: chain_info_query.request_pending_messages,
            request_sent_certificates_in_range: Some(chain_info_query.request_sent_certificates_in_range.into()),
            request_received_certificates_excluding_first_nth: Some(chain_info_query.request_received_certificates_excluding_first_nth.into())
        })
    }
}

impl TryFrom<Origin> for OriginRpc {
    type Error = ProtoConversionError;

    fn try_from(origin: Origin) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_id: Some(origin.chain_id.into()),
            medium: Some(origin.medium.into())
        })
    }
}

impl From<Medium> for MediumRpc {
    fn from(medium: Medium) -> Self {
        match medium {
            Medium::Direct => MediumRpc {
                inner: Some(medium::Inner::Direct(false))
            },
            Medium::Channel(channel) => MediumRpc {
                inner: Some(medium::Inner::Channel(channel))
            }
        }
    }
}

impl From<BlockHeightRange> for BlockHeaightRangeRPC {
    fn from(block_height_range: BlockHeightRange) -> Self {
        Self {
            start: Some(block_height_range.start.into()),
            limit: block_height_range.limit
        }
    }
}

impl From<ApplicationId> for ApplicationIdRPC {
    fn from(application_id: ApplicationId) -> Self {
        Self {
            inner: application_id.0
        }
    }
}

impl From<ChainId> for ChainIdRPC {
    fn from(application_id: ChainId) -> Self {
        Self  {
            bytes: application_id.0.as_bytes()
        }
    }
}

// todo
// 1. Public Key
// 2. Signature
// 3. ChainInfoResponse
// 4. BlockHeight