use ed25519::signature::Signature as edSignature;
use thiserror::Error;

use crate::grpc_network::grpc_network::{
    medium, CrossChainRequest as CrossChainRequestRpc, NameSignaturePair,
};
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

use crate::grpc_network::grpc_network::BlockHeightRange as BlockHeightRangeRPC;
use linera_core::messages::BlockHeightRange;

use crate::grpc_network::grpc_network::ApplicationId as ApplicationIdRPC;
use linera_base::messages::ApplicationId;

use crate::grpc_network::grpc_network::ChainId as ChainIdRPC;
use linera_base::messages::ChainId;

use crate::grpc_network::grpc_network::PublicKey as PublicKeyRPC;
use linera_base::crypto::PublicKey;

use crate::grpc_network::grpc_network::Signature as SignatureRPC;
use linera_base::crypto::Signature;

use crate::grpc_network::grpc_network::ChainInfoResponse as ChainInfoResponseRPC;
use linera_core::messages::ChainInfoResponse;

use crate::grpc_network::grpc_network::BlockHeight as BlockHeightRPC;
use linera_base::messages::BlockHeight;

use crate::grpc_network::grpc_network::Owner as OwnerRPC;
use linera_base::messages::Owner;

#[derive(Error, Debug)]
pub enum ProtoConversionError {
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
        let signatures = certificate
            .signatures
            .into_iter()
            .map(|(validator_name, signature)| NameSignaturePair {
                validator_name: Some(validator_name.into()),
                signature: Some(signature.into()),
            })
            .collect();

        Ok(Self {
            value: bcs::to_bytes(&certificate.value)?,
            signatures,
        })
    }
}

impl TryFrom<ChainInfoQuery> for ChainInfoQueryRpc {
    type Error = ProtoConversionError;

    fn try_from(chain_info_query: ChainInfoQuery) -> Result<Self, Self::Error> {
        let test_next_block_height = chain_info_query.test_next_block_height.map(|t| t.into());

        let request_sent_certificates_in_range = chain_info_query
            .request_sent_certificates_in_range
            .map(|r| r.into());

        let request_received_certificates_excluding_first_nth = chain_info_query
            .request_received_certificates_excluding_first_nth
            .map(|n| n as u64);

        Ok(Self {
            chain_id: Some(chain_info_query.chain_id.into()),
            test_next_block_height,
            request_committees: chain_info_query.request_committees,
            request_pending_messages: chain_info_query.request_pending_messages,
            request_sent_certificates_in_range,
            request_received_certificates_excluding_first_nth,
        })
    }
}

impl TryFrom<Origin> for OriginRpc {
    type Error = ProtoConversionError;

    fn try_from(origin: Origin) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_id: Some(origin.chain_id.into()),
            medium: Some(origin.medium.into()),
        })
    }
}

impl From<Medium> for MediumRpc {
    fn from(medium: Medium) -> Self {
        match medium {
            Medium::Direct => MediumRpc {
                inner: Some(medium::Inner::Direct(false)),
            },
            Medium::Channel(channel) => MediumRpc {
                inner: Some(medium::Inner::Channel(channel)),
            },
        }
    }
}

impl From<BlockHeightRange> for BlockHeightRangeRPC {
    fn from(block_height_range: BlockHeightRange) -> Self {
        Self {
            start: Some(block_height_range.start.into()),
            limit: block_height_range.limit.map(|l| l as u64),
        }
    }
}

impl From<ApplicationId> for ApplicationIdRPC {
    fn from(application_id: ApplicationId) -> Self {
        Self {
            inner: application_id.0,
        }
    }
}

impl From<ChainId> for ChainIdRPC {
    fn from(application_id: ChainId) -> Self {
        Self {
            bytes: application_id.0.as_bytes().to_vec(),
        }
    }
}

impl From<PublicKey> for PublicKeyRPC {
    fn from(public_key: PublicKey) -> Self {
        Self {
            bytes: public_key.0.to_vec(),
        }
    }
}

impl From<ValidatorName> for PublicKeyRPC {
    fn from(validator_name: ValidatorName) -> Self {
        Self {
            bytes: validator_name.0 .0.to_vec(),
        }
    }
}

impl From<Signature> for SignatureRPC {
    fn from(signature: Signature) -> Self {
        Self {
            bytes: signature.0.as_bytes().to_vec(),
        }
    }
}

impl TryFrom<ChainInfoResponse> for ChainInfoResponseRPC {
    type Error = ProtoConversionError;

    fn try_from(chain_info_response: ChainInfoResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_info: bcs::to_bytes(&chain_info_response.info)?,
            signature: chain_info_response.signature.map(|s| s.into()),
        })
    }
}

impl From<BlockHeight> for BlockHeightRPC {
    fn from(block_height: BlockHeight) -> Self {
        Self {
            height: block_height.0,
        }
    }
}

impl From<Owner> for OwnerRPC {
    fn from(owner: Owner) -> Self {
        Self {
            inner: Some(owner.0.into()),
        }
    }
}
