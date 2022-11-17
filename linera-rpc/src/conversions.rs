use ed25519::signature::Signature as edSignature;
use thiserror::Error;
use tonic::{Code, Response, Status};

use crate::grpc_network::grpc_network::{
    chain_info_result, medium, ChainInfoResult, ConfirmUpdateRecipient,
    CrossChainRequest as CrossChainRequestRpc, NameSignaturePair, UpdateRecipient,
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
use linera_base::crypto::{CryptoError, HashValue, PublicKey, PublicKeyFromStrError};

use crate::grpc_network::grpc_network::Signature as SignatureRPC;
use linera_base::crypto::Signature;

use crate::grpc_network::grpc_network::ChainInfoResponse as ChainInfoResponseRPC;
use linera_core::messages::ChainInfoResponse;

use crate::grpc_network::grpc_network::BlockHeight as BlockHeightRPC;
use linera_base::messages::BlockHeight;

use crate::grpc_network::grpc_network::{
    chain_info_result::Inner::ChainInfoResponse as ChainInfoResponseRpc,
    cross_chain_request::Inner, Owner as OwnerRPC,
};
use linera_base::messages::Owner;

#[derive(Error, Debug)]
pub enum ProtoConversionError {
    #[error("BCS serialization / deserialization error.")]
    BcsError(#[from] bcs::Error),
    #[error("Conversion failed due to missing field")]
    MissingField,
    #[error("Signature error: {0}")]
    SignatureError(#[from] ed25519_dalek::SignatureError),
    #[error("Public key error: {0}")]
    PublicKeyError(#[from] PublicKeyFromStrError),
    // todo do we want this?
    #[error("Cryptographic error: {0}")]
    CryptoError(#[from] CryptoError),
}

/// Extract an optional field from a Proto type and map it.
macro_rules! proto_convert {
    ($expr:expr) => {
        $expr.ok_or(ProtoConversionError::MissingField)?.into()
    };
}

/// Extract an optional field from a Proto type and try to map it.
macro_rules! try_proto_convert {
    ($expr:expr) => {
        $expr
            .ok_or(ProtoConversionError::MissingField)?
            .try_into()?
    };
}

/// Try to map an iterable collection into a vector.
macro_rules! try_proto_convert_vec {
    ($expr:expr, $ty:ty) => {
        $expr
            .into_iter()
            .map(|c| c.try_into())
            .collect::<Result<Vec<$ty>, ProtoConversionError>>()?
    };
}

/// Map a type into another type.
macro_rules! map_into {
    ($expr:expr) => {
        $expr.map(|x| x.into())
    };
}

/// Try to map a type into another type.
macro_rules! map_try_into {
    ($expr:expr) => {
        $expr.map(|x| x.try_into())
    };
}

/// Cast a type to another type via a map.
macro_rules! map_as {
    ($expr:expr, $ty:ty) => {
        $expr.map(|x| x as $ty)
    };
}

/// Maps from Result<Option<T>,E> to Option<Result<T,E>>.
macro_rules! map_invert {
    ($expr:expr) => {
        $expr
            .map(|x| x.try_into())
            .map_or(Ok(None), |v| v.map(Some))?
    };
}

#[macro_export]
macro_rules! convert_response {
    ($self:ident, $handler:ident, $req:ident) => {
        Ok(Response::new(match $self.state.clone().$handler($req.into_inner().try_into()?).await {
            Ok(chain_info_response) => ChainInfoResult {
                inner: Some(
                    crate::grpc_network::grpc_network::chain_info_result::Inner::ChainInfoResponse(
                        chain_info_response.try_into()?,
                    ),
                ),
            },
            Err(error) => ChainInfoResult {
                // todo we need to serialize this properly
                inner: Some(
                    crate::grpc_network::grpc_network::chain_info_result::Inner::Error(
                        NodeError::from(error).to_string(),
                    ),
                ),
            },
        }))
    };
}

impl From<ProtoConversionError> for Status {
    fn from(error: ProtoConversionError) -> Self {
        Status::new(Code::InvalidArgument, error.to_string())
    }
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

impl TryFrom<BlockProposalRpc> for BlockProposal {
    type Error = ProtoConversionError;

    fn try_from(block_proposal: BlockProposalRpc) -> Result<Self, Self::Error> {
        Ok(Self {
            content: bcs::from_bytes(&block_proposal.content)?,
            owner: try_proto_convert!(block_proposal.owner),
            signature: try_proto_convert!(block_proposal.signature),
        })
    }
}

impl TryFrom<CrossChainRequestRpc> for CrossChainRequest {
    type Error = ProtoConversionError;

    fn try_from(cross_chain_request: CrossChainRequestRpc) -> Result<Self, Self::Error> {
        let ccr = match cross_chain_request
            .inner
            .ok_or(ProtoConversionError::MissingField)?
        {
            Inner::UpdateRecipient(UpdateRecipient {
                application_id,
                origin,
                recipient,
                certificates,
            }) => CrossChainRequest::UpdateRecipient {
                application_id: proto_convert!(application_id),
                origin: try_proto_convert!(origin),
                recipient: try_proto_convert!(recipient),
                certificates: try_proto_convert_vec!(certificates, Certificate),
            },
            Inner::ConfirmUpdateRecipient(ConfirmUpdateRecipient {
                application_id,
                origin,
                recipient,
                height,
            }) => CrossChainRequest::ConfirmUpdatedRecipient {
                application_id: proto_convert!(application_id),
                origin: try_proto_convert!(origin),
                recipient: try_proto_convert!(recipient),
                height: proto_convert!(height),
            },
        };
        Ok(ccr)
    }
}

impl TryFrom<CrossChainRequest> for CrossChainRequestRpc {
    type Error = ProtoConversionError;

    fn try_from(cross_chain_request: CrossChainRequest) -> Result<Self, Self::Error> {
        let inner = match cross_chain_request {
            CrossChainRequest::UpdateRecipient {
                application_id,
                origin,
                recipient,
                certificates,
            } => Inner::UpdateRecipient(UpdateRecipient {
                application_id: Some(application_id.into()),
                origin: Some(origin.into()),
                recipient: Some(recipient.into()),
                certificates: try_proto_convert_vec!(certificates, CertificateRpc),
            }),
            CrossChainRequest::ConfirmUpdatedRecipient {
                application_id,
                origin,
                recipient,
                height,
            } => Inner::ConfirmUpdateRecipient(ConfirmUpdateRecipient {
                application_id: Some(application_id.into()),
                origin: Some(origin.into()),
                recipient: Some(recipient.into()),
                height: Some(height.into()),
            }),
        };
        Ok(Self { inner: Some(inner) })
    }
}

impl TryFrom<CertificateRpc> for Certificate {
    type Error = ProtoConversionError;

    fn try_from(certificate: CertificateRpc) -> Result<Self, Self::Error> {
        let mut signatures = Vec::with_capacity(certificate.signatures.len());

        for name_signature_pair in certificate.signatures {
            let validator_name: ValidatorName =
                try_proto_convert!(name_signature_pair.validator_name);
            let signature: Signature = try_proto_convert!(name_signature_pair.signature);
            signatures.push((validator_name, signature));
        }

        unimplemented!();

        Ok(Self {
            value: bcs::from_bytes(certificate.value.as_slice())?,
            signatures,
            hash: todo!(),
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

impl TryFrom<ChainInfoQueryRpc> for ChainInfoQuery {
    type Error = ProtoConversionError;

    fn try_from(chain_info_query: ChainInfoQueryRpc) -> Result<Self, Self::Error> {
        Ok(Self {
            request_committees: chain_info_query.request_committees,
            request_pending_messages: chain_info_query.request_pending_messages,
            chain_id: try_proto_convert!(chain_info_query.chain_id),
            request_sent_certificates_in_range: map_invert!(
                chain_info_query.request_sent_certificates_in_range
            ),
            request_received_certificates_excluding_first_nth: map_as!(
                chain_info_query.request_received_certificates_excluding_first_nth,
                usize
            ),
            test_next_block_height: map_into!(chain_info_query.test_next_block_height),
        })
    }
}

impl TryFrom<ChainInfoQuery> for ChainInfoQueryRpc {
    type Error = ProtoConversionError;

    fn try_from(chain_info_query: ChainInfoQuery) -> Result<Self, Self::Error> {
        let test_next_block_height = map_into!(chain_info_query.test_next_block_height);

        let request_sent_certificates_in_range =
            map_into!(chain_info_query.request_sent_certificates_in_range);

        let request_received_certificates_excluding_first_nth = map_as!(
            chain_info_query.request_received_certificates_excluding_first_nth,
            u64
        );

        Ok(Self {
            chain_id: Some(chain_info_query.chain_id.into()),
            request_committees: chain_info_query.request_committees,
            request_pending_messages: chain_info_query.request_pending_messages,
            test_next_block_height,
            request_sent_certificates_in_range,
            request_received_certificates_excluding_first_nth,
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

impl TryFrom<MediumRpc> for Medium {
    type Error = ProtoConversionError;

    fn try_from(medium: MediumRpc) -> Result<Self, Self::Error> {
        match medium.inner.ok_or(ProtoConversionError::MissingField)? {
            medium::Inner::Direct(_) => Ok(Medium::Direct),
            medium::Inner::Channel(channel) => Ok(Medium::Channel(channel)),
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

impl TryFrom<BlockHeightRangeRPC> for BlockHeightRange {
    type Error = ProtoConversionError;

    fn try_from(block_height_range: BlockHeightRangeRPC) -> Result<Self, Self::Error> {
        Ok(Self {
            start: proto_convert!(block_height_range.start),
            limit: map_as!(block_height_range.limit, usize),
        })
    }
}

impl From<ApplicationId> for ApplicationIdRPC {
    fn from(application_id: ApplicationId) -> Self {
        Self {
            inner: application_id.0,
        }
    }
}

impl From<ApplicationIdRPC> for ApplicationId {
    fn from(application_id: ApplicationIdRPC) -> Self {
        ApplicationId(application_id.inner)
    }
}

impl From<ChainId> for ChainIdRPC {
    fn from(application_id: ChainId) -> Self {
        Self {
            bytes: application_id.0.as_bytes().to_vec(),
        }
    }
}

impl TryFrom<ChainIdRPC> for ChainId {
    type Error = ProtoConversionError;

    fn try_from(chain_id: ChainIdRPC) -> Result<Self, Self::Error> {
        Ok(ChainId::try_from(chain_id.bytes.as_slice())?)
    }
}

impl From<PublicKey> for PublicKeyRPC {
    fn from(public_key: PublicKey) -> Self {
        Self {
            bytes: public_key.0.to_vec(),
        }
    }
}

impl TryFrom<PublicKeyRPC> for PublicKey {
    type Error = ProtoConversionError;

    fn try_from(public_key: PublicKeyRPC) -> Result<Self, Self::Error> {
        Ok(PublicKey::try_from(public_key.bytes.as_slice())?)
    }
}

impl From<Origin> for OriginRpc {
    fn from(origin: Origin) -> Self {
        Self {
            chain_id: Some(origin.chain_id.into()),
            medium: Some(origin.medium.into()),
        }
    }
}

impl TryFrom<OriginRpc> for Origin {
    type Error = ProtoConversionError;

    fn try_from(origin: OriginRpc) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_id: try_proto_convert!(origin.chain_id),
            medium: Medium::Direct,
        })
    }
}

impl From<ValidatorName> for PublicKeyRPC {
    fn from(validator_name: ValidatorName) -> Self {
        Self {
            bytes: validator_name.0 .0.to_vec(),
        }
    }
}

impl TryFrom<PublicKeyRPC> for ValidatorName {
    type Error = ProtoConversionError;

    fn try_from(public_key: PublicKeyRPC) -> Result<Self, Self::Error> {
        public_key.try_into()
    }
}

impl From<Signature> for SignatureRPC {
    fn from(signature: Signature) -> Self {
        Self {
            bytes: signature.0.as_bytes().to_vec(),
        }
    }
}

impl TryFrom<SignatureRPC> for Signature {
    type Error = ProtoConversionError;

    fn try_from(signature: SignatureRPC) -> Result<Self, Self::Error> {
        Ok(Self {
            0: ed25519_dalek::Signature::from_bytes(&signature.bytes)?,
        })
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

impl From<BlockHeightRPC> for BlockHeight {
    fn from(block_height: BlockHeightRPC) -> Self {
        Self {
            0: block_height.height,
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

impl TryFrom<OwnerRPC> for Owner {
    type Error = ProtoConversionError;

    fn try_from(owner: OwnerRPC) -> Result<Self, Self::Error> {
        Ok(Self {
            0: try_proto_convert!(owner.inner),
        })
    }
}
