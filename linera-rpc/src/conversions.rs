// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::grpc_network::{
    grpc,
    grpc::{ChainInfoResult, NameSignaturePair, NewBlock, NewMessage},
};
use ed25519::signature::Signature as edSignature;
use linera_base::{
    crypto::{CryptoError, CryptoHash, PublicKey, Signature},
    data_types::BlockHeight,
    ensure,
    identifiers::{BytecodeId, ChainId, EffectId, Owner},
};
use linera_chain::data_types::{
    BlockAndRound, BlockProposal, Certificate, CertificateWithDependencies, ChannelFullName,
    HashedValue, LiteCertificate, LiteValue, Medium, Origin,
};
use linera_core::{
    data_types::{
        BlockHeightRange, ChainInfoQuery, ChainInfoResponse, CrossChainRequest,
        CrossChainRequest::{ConfirmUpdatedRecipient, UpdateRecipient},
    },
    node::NodeError,
    worker::{Notification, Reason},
};
use linera_execution::{committee::ValidatorName, ApplicationId, UserApplicationId};
use thiserror::Error;
use tonic::{Code, Status};

#[derive(Error, Debug)]
pub enum ProtoConversionError {
    #[error("BCS serialization / deserialization error.")]
    BcsError(#[from] bcs::Error),
    #[error("Conversion failed due to missing field")]
    MissingField,
    #[error("Signature error: {0}")]
    SignatureError(#[from] ed25519_dalek::SignatureError),
    #[error("Cryptographic error: {0}")]
    CryptoError(#[from] CryptoError),
    #[error("Inconsistent outer/inner chain ids")]
    InconsistentChainId,
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

impl From<ProtoConversionError> for Status {
    fn from(error: ProtoConversionError) -> Self {
        Status::new(Code::InvalidArgument, error.to_string())
    }
}

impl From<Notification> for grpc::Notification {
    fn from(notification: Notification) -> Self {
        Self {
            chain_id: Some(notification.chain_id.into()),
            reason: Some(notification.reason.into()),
        }
    }
}

impl TryFrom<grpc::Notification> for Notification {
    type Error = ProtoConversionError;

    fn try_from(notification: grpc::Notification) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_id: try_proto_convert!(notification.chain_id),
            reason: try_proto_convert!(notification.reason),
        })
    }
}

impl From<Reason> for grpc::Reason {
    fn from(reason: Reason) -> Self {
        Self {
            inner: Some(match reason {
                Reason::NewBlock { height } => grpc::reason::Inner::NewBlock(NewBlock {
                    height: Some(height.into()),
                }),
                Reason::NewMessage { origin, height } => {
                    grpc::reason::Inner::NewMessage(NewMessage {
                        origin: Some(origin.into()),
                        height: Some(height.into()),
                    })
                }
            }),
        }
    }
}

impl TryFrom<grpc::Reason> for Reason {
    type Error = ProtoConversionError;

    fn try_from(reason: grpc::Reason) -> Result<Self, Self::Error> {
        Ok(
            match reason.inner.ok_or(ProtoConversionError::MissingField)? {
                grpc::reason::Inner::NewBlock(new_block) => Reason::NewBlock {
                    height: proto_convert!(new_block.height),
                },
                grpc::reason::Inner::NewMessage(new_message) => Reason::NewMessage {
                    origin: try_proto_convert!(new_message.origin),
                    height: proto_convert!(new_message.height),
                },
            },
        )
    }
}

impl TryFrom<ChainInfoResponse> for ChainInfoResult {
    type Error = ProtoConversionError;

    fn try_from(chain_info_response: ChainInfoResponse) -> Result<Self, Self::Error> {
        Ok(ChainInfoResult {
            inner: Some(grpc::chain_info_result::Inner::ChainInfoResponse(
                chain_info_response.try_into()?,
            )),
        })
    }
}

impl TryFrom<NodeError> for ChainInfoResult {
    type Error = ProtoConversionError;

    fn try_from(node_error: NodeError) -> Result<Self, Self::Error> {
        Ok(ChainInfoResult {
            inner: Some(grpc::chain_info_result::Inner::Error(
                bcs::to_bytes(&node_error).expect("bcs::MAX_CONTAINER_DEPTH is never exceeded"),
            )),
        })
    }
}

impl TryFrom<BlockProposal> for grpc::BlockProposal {
    type Error = ProtoConversionError;

    fn try_from(block_proposal: BlockProposal) -> Result<Self, Self::Error> {
        let blobs = block_proposal
            .blobs
            .iter()
            .map(bcs::to_bytes)
            .collect::<Result<_, bcs::Error>>()?;
        Ok(Self {
            chain_id: Some(block_proposal.content.block.chain_id.into()),
            content: bcs::to_bytes(&block_proposal.content)?,
            owner: Some(block_proposal.owner.into()),
            signature: Some(block_proposal.signature.into()),
            blobs,
        })
    }
}

impl TryFrom<grpc::BlockProposal> for BlockProposal {
    type Error = ProtoConversionError;

    fn try_from(block_proposal: grpc::BlockProposal) -> Result<Self, Self::Error> {
        let content: BlockAndRound = bcs::from_bytes(&block_proposal.content)?;
        let inner_chain_id = content.block.chain_id;
        ensure!(
            Some(inner_chain_id.into()) == block_proposal.chain_id,
            ProtoConversionError::InconsistentChainId
        );
        let blobs = block_proposal
            .blobs
            .iter()
            .map(|bytes| bcs::from_bytes(bytes))
            .collect::<Result<_, bcs::Error>>()?;
        Ok(Self {
            content,
            owner: try_proto_convert!(block_proposal.owner),
            signature: try_proto_convert!(block_proposal.signature),
            blobs,
        })
    }
}

impl TryFrom<grpc::CrossChainRequest> for CrossChainRequest {
    type Error = ProtoConversionError;

    fn try_from(cross_chain_request: grpc::CrossChainRequest) -> Result<Self, Self::Error> {
        use grpc::cross_chain_request::Inner;

        let ccr = match cross_chain_request
            .inner
            .ok_or(ProtoConversionError::MissingField)?
        {
            Inner::UpdateRecipient(grpc::UpdateRecipient {
                height_map: grpc_height_map,
                sender,
                recipient,
                certificates,
            }) => {
                let mut height_map = Vec::new();
                for grpc::UpdateRecipientEntry { medium, heights } in grpc_height_map {
                    height_map.push((
                        try_proto_convert!(medium),
                        heights.into_iter().map(BlockHeight::from).collect(),
                    ));
                }
                UpdateRecipient {
                    height_map,
                    sender: try_proto_convert!(sender),
                    recipient: try_proto_convert!(recipient),
                    certificates: try_proto_convert_vec!(certificates, Certificate),
                }
            }
            Inner::ConfirmUpdatedRecipient(grpc::ConfirmUpdatedRecipient {
                origin,
                recipient,
                height,
            }) => ConfirmUpdatedRecipient {
                origin: try_proto_convert!(origin),
                recipient: try_proto_convert!(recipient),
                height: proto_convert!(height),
            },
        };
        Ok(ccr)
    }
}

impl TryFrom<CrossChainRequest> for grpc::CrossChainRequest {
    type Error = ProtoConversionError;

    fn try_from(cross_chain_request: CrossChainRequest) -> Result<Self, Self::Error> {
        use grpc::cross_chain_request::Inner;

        let inner = match cross_chain_request {
            UpdateRecipient {
                height_map,
                sender,
                recipient,
                certificates,
            } => Inner::UpdateRecipient(grpc::UpdateRecipient {
                height_map: height_map
                    .into_iter()
                    .map(|(medium, heights)| grpc::UpdateRecipientEntry {
                        medium: Some(medium.into()),
                        heights: heights.into_iter().map(|height| height.into()).collect(),
                    })
                    .collect(),
                sender: Some(sender.into()),
                recipient: Some(recipient.into()),
                certificates: try_proto_convert_vec!(certificates, grpc::Certificate),
            }),
            ConfirmUpdatedRecipient {
                origin,
                recipient,
                height,
            } => Inner::ConfirmUpdatedRecipient(grpc::ConfirmUpdatedRecipient {
                origin: Some(origin.into()),
                recipient: Some(recipient.into()),
                height: Some(height.into()),
            }),
        };
        Ok(Self { inner: Some(inner) })
    }
}

impl TryFrom<grpc::LiteCertificate> for LiteCertificate {
    type Error = ProtoConversionError;

    fn try_from(certificate: grpc::LiteCertificate) -> Result<Self, Self::Error> {
        let mut signatures = Vec::with_capacity(certificate.signatures.len());

        for name_signature_pair in certificate.signatures {
            let validator_name: ValidatorName =
                try_proto_convert!(name_signature_pair.validator_name);
            let signature: Signature = try_proto_convert!(name_signature_pair.signature);
            signatures.push((validator_name, signature));
        }

        let chain_id = try_proto_convert!(certificate.chain_id);
        Ok(LiteCertificate::new(
            LiteValue {
                value_hash: CryptoHash::try_from(certificate.hash.as_slice())?,
                chain_id,
            },
            signatures,
        ))
    }
}

impl TryFrom<LiteCertificate> for grpc::LiteCertificate {
    type Error = ProtoConversionError;

    fn try_from(certificate: LiteCertificate) -> Result<Self, Self::Error> {
        let signatures = certificate
            .signatures
            .into_iter()
            .map(|(validator_name, signature)| NameSignaturePair {
                validator_name: Some(validator_name.into()),
                signature: Some(signature.into()),
            })
            .collect();

        Ok(Self {
            hash: certificate.value.value_hash.as_bytes().to_vec(),
            chain_id: Some(certificate.value.chain_id.into()),
            signatures,
        })
    }
}

impl TryFrom<grpc::Certificate> for Certificate {
    type Error = ProtoConversionError;

    fn try_from(certificate: grpc::Certificate) -> Result<Self, Self::Error> {
        let value: HashedValue = bcs::from_bytes(certificate.value.as_slice())?;
        let inner_chain_id = value.chain_id();
        ensure!(
            Some(inner_chain_id.into()) == certificate.chain_id,
            ProtoConversionError::InconsistentChainId
        );

        let mut signatures = Vec::with_capacity(certificate.signatures.len());
        for name_signature_pair in certificate.signatures {
            let validator_name: ValidatorName =
                try_proto_convert!(name_signature_pair.validator_name);
            let signature: Signature = try_proto_convert!(name_signature_pair.signature);
            signatures.push((validator_name, signature));
        }

        Ok(Certificate::new(value, signatures))
    }
}

impl TryFrom<Certificate> for grpc::Certificate {
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
            chain_id: Some(certificate.value.chain_id().into()),
            value: bcs::to_bytes(&certificate.value)?,
            signatures,
        })
    }
}

impl TryFrom<grpc::CertificateWithDependencies> for CertificateWithDependencies {
    type Error = ProtoConversionError;

    fn try_from(cert_with_deps: grpc::CertificateWithDependencies) -> Result<Self, Self::Error> {
        let blobs = cert_with_deps
            .blobs
            .into_iter()
            .map(|value| bcs::from_bytes(value.as_slice()))
            .collect::<Result<_, bcs::Error>>()?;
        let certificate: Certificate = try_proto_convert!(cert_with_deps.certificate);
        let inner_chain_id = certificate.value.chain_id();
        ensure!(
            Some(inner_chain_id.into()) == cert_with_deps.chain_id,
            ProtoConversionError::InconsistentChainId
        );
        Ok(CertificateWithDependencies { certificate, blobs })
    }
}

impl TryFrom<CertificateWithDependencies> for grpc::CertificateWithDependencies {
    type Error = ProtoConversionError;

    fn try_from(cert_with_deps: CertificateWithDependencies) -> Result<Self, Self::Error> {
        let blobs = cert_with_deps
            .blobs
            .into_iter()
            .map(|value| bcs::to_bytes(&value))
            .collect::<Result<_, _>>()?;
        let chain_id = Some(cert_with_deps.certificate.value.chain_id().into());
        let certificate = Some(cert_with_deps.certificate.try_into()?);
        Ok(Self {
            chain_id,
            certificate,
            blobs,
        })
    }
}

impl TryFrom<grpc::ChainInfoQuery> for ChainInfoQuery {
    type Error = ProtoConversionError;

    fn try_from(chain_info_query: grpc::ChainInfoQuery) -> Result<Self, Self::Error> {
        Ok(Self {
            request_committees: chain_info_query.request_committees,
            request_pending_messages: chain_info_query.request_pending_messages,
            chain_id: try_proto_convert!(chain_info_query.chain_id),
            request_sent_certificates_in_range: map_invert!(
                chain_info_query.request_sent_certificates_in_range
            ),
            request_received_log_excluding_first_nth: map_as!(
                chain_info_query.request_received_log_excluding_first_nth,
                usize
            ),
            test_next_block_height: map_into!(chain_info_query.test_next_block_height),
            request_manager_values: chain_info_query.request_manager_values,
            request_blob: chain_info_query
                .request_blob
                .map(|bytes| bcs::from_bytes(&bytes))
                .transpose()?,
        })
    }
}

impl TryFrom<ChainInfoQuery> for grpc::ChainInfoQuery {
    type Error = ProtoConversionError;

    fn try_from(chain_info_query: ChainInfoQuery) -> Result<Self, Self::Error> {
        let test_next_block_height = map_into!(chain_info_query.test_next_block_height);

        let request_sent_certificates_in_range =
            map_into!(chain_info_query.request_sent_certificates_in_range);

        let request_received_log_excluding_first_nth = map_as!(
            chain_info_query.request_received_log_excluding_first_nth,
            u64
        );

        Ok(Self {
            chain_id: Some(chain_info_query.chain_id.into()),
            request_committees: chain_info_query.request_committees,
            request_pending_messages: chain_info_query.request_pending_messages,
            test_next_block_height,
            request_sent_certificates_in_range,
            request_received_log_excluding_first_nth,
            request_manager_values: chain_info_query.request_manager_values,
            request_blob: chain_info_query
                .request_blob
                .map(|hash| bcs::to_bytes(&hash))
                .transpose()?,
        })
    }
}

impl From<ChannelFullName> for grpc::ChannelFullName {
    fn from(full_name: ChannelFullName) -> Self {
        Self {
            application_id: Some(full_name.application_id.into()),
            name: full_name.name.as_ref().to_owned(),
        }
    }
}

impl TryFrom<grpc::ChannelFullName> for ChannelFullName {
    type Error = ProtoConversionError;

    fn try_from(full_name: grpc::ChannelFullName) -> Result<Self, Self::Error> {
        Ok(Self {
            application_id: try_proto_convert!(full_name.application_id),
            name: full_name.name.into(),
        })
    }
}

impl From<BlockHeightRange> for grpc::BlockHeightRange {
    fn from(block_height_range: BlockHeightRange) -> Self {
        Self {
            start: Some(block_height_range.start.into()),
            limit: map_as!(block_height_range.limit, u64),
        }
    }
}

impl TryFrom<grpc::BlockHeightRange> for BlockHeightRange {
    type Error = ProtoConversionError;

    fn try_from(block_height_range: grpc::BlockHeightRange) -> Result<Self, Self::Error> {
        Ok(Self {
            start: proto_convert!(block_height_range.start),
            limit: block_height_range.limit,
        })
    }
}

impl From<ApplicationId> for grpc::ApplicationId {
    fn from(application_id: ApplicationId) -> Self {
        match application_id {
            ApplicationId::System => grpc::ApplicationId {
                inner: Some(grpc::application_id::Inner::System(())),
            },
            ApplicationId::User(UserApplicationId {
                bytecode_id,
                creation,
            }) => grpc::ApplicationId {
                inner: Some(grpc::application_id::Inner::User(grpc::UserApplicationId {
                    bytecode_id: Some(bytecode_id.into()),
                    creation: Some(creation.into()),
                })),
            },
        }
    }
}

impl TryFrom<grpc::ApplicationId> for ApplicationId {
    type Error = ProtoConversionError;

    fn try_from(application_id: grpc::ApplicationId) -> Result<Self, Self::Error> {
        Ok(
            match application_id
                .inner
                .ok_or(ProtoConversionError::MissingField)?
            {
                grpc::application_id::Inner::System(_) => ApplicationId::System,
                grpc::application_id::Inner::User(user_application_id) => {
                    ApplicationId::User(UserApplicationId {
                        bytecode_id: try_proto_convert!(user_application_id.bytecode_id),
                        creation: try_proto_convert!(user_application_id.creation),
                    })
                }
            },
        )
    }
}

impl From<Medium> for grpc::Medium {
    fn from(medium: Medium) -> Self {
        match medium {
            Medium::Direct => grpc::Medium {
                inner: Some(grpc::medium::Inner::Direct(())),
            },
            Medium::Channel(full_name) => grpc::Medium {
                inner: Some(grpc::medium::Inner::Channel(full_name.into())),
            },
        }
    }
}

impl TryFrom<grpc::Medium> for Medium {
    type Error = ProtoConversionError;

    fn try_from(medium: grpc::Medium) -> Result<Self, Self::Error> {
        Ok(
            match medium.inner.ok_or(ProtoConversionError::MissingField)? {
                grpc::medium::Inner::Direct(_) => Medium::Direct,
                grpc::medium::Inner::Channel(full_name) => Medium::Channel(ChannelFullName {
                    application_id: try_proto_convert!(full_name.application_id),
                    name: full_name.name.into(),
                }),
            },
        )
    }
}

impl From<BytecodeId> for grpc::BytecodeId {
    fn from(bytecode_id: BytecodeId) -> Self {
        Self {
            publish_effect: Some(bytecode_id.0.into()),
        }
    }
}

impl TryFrom<grpc::BytecodeId> for BytecodeId {
    type Error = ProtoConversionError;

    fn try_from(bytecode_id: grpc::BytecodeId) -> Result<Self, Self::Error> {
        Ok(Self(try_proto_convert!(bytecode_id.publish_effect)))
    }
}

impl From<EffectId> for grpc::EffectId {
    fn from(effect_id: EffectId) -> Self {
        Self {
            chain_id: Some(effect_id.chain_id.into()),
            height: Some(effect_id.height.into()),
            index: effect_id.index,
        }
    }
}

impl TryFrom<grpc::EffectId> for EffectId {
    type Error = ProtoConversionError;

    fn try_from(effect_id: grpc::EffectId) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_id: try_proto_convert!(effect_id.chain_id),
            height: proto_convert!(effect_id.height),
            index: effect_id.index,
        })
    }
}

impl From<ChainId> for grpc::ChainId {
    fn from(chain_id: ChainId) -> Self {
        Self {
            bytes: chain_id.0.as_bytes().to_vec(),
        }
    }
}

impl TryFrom<grpc::ChainId> for ChainId {
    type Error = ProtoConversionError;

    fn try_from(chain_id: grpc::ChainId) -> Result<Self, Self::Error> {
        Ok(ChainId::try_from(chain_id.bytes.as_slice())?)
    }
}

impl From<PublicKey> for grpc::PublicKey {
    fn from(public_key: PublicKey) -> Self {
        Self {
            bytes: public_key.0.to_vec(),
        }
    }
}

impl TryFrom<grpc::PublicKey> for PublicKey {
    type Error = ProtoConversionError;

    fn try_from(public_key: grpc::PublicKey) -> Result<Self, Self::Error> {
        Ok(PublicKey::try_from(public_key.bytes.as_slice())?)
    }
}

impl From<Origin> for grpc::Origin {
    fn from(origin: Origin) -> Self {
        Self {
            sender: Some(origin.sender.into()),
            medium: Some(origin.medium.into()),
        }
    }
}

impl TryFrom<grpc::Origin> for Origin {
    type Error = ProtoConversionError;

    fn try_from(origin: grpc::Origin) -> Result<Self, Self::Error> {
        Ok(Self {
            sender: try_proto_convert!(origin.sender),
            medium: try_proto_convert!(origin.medium),
        })
    }
}

impl From<ValidatorName> for grpc::PublicKey {
    fn from(validator_name: ValidatorName) -> Self {
        Self {
            bytes: validator_name.0 .0.to_vec(),
        }
    }
}

impl TryFrom<grpc::PublicKey> for ValidatorName {
    type Error = ProtoConversionError;

    fn try_from(public_key: grpc::PublicKey) -> Result<Self, Self::Error> {
        Ok(ValidatorName(public_key.try_into()?))
    }
}

impl From<Signature> for grpc::Signature {
    fn from(signature: Signature) -> Self {
        Self {
            bytes: signature.0.as_bytes().to_vec(),
        }
    }
}

impl TryFrom<grpc::Signature> for Signature {
    type Error = ProtoConversionError;

    fn try_from(signature: grpc::Signature) -> Result<Self, Self::Error> {
        Ok(Self(ed25519_dalek::Signature::from_bytes(
            &signature.bytes,
        )?))
    }
}

impl TryFrom<ChainInfoResponse> for grpc::ChainInfoResponse {
    type Error = ProtoConversionError;

    fn try_from(chain_info_response: ChainInfoResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_info: bcs::to_bytes(&chain_info_response.info)?,
            signature: map_into!(chain_info_response.signature),
        })
    }
}

impl TryFrom<grpc::ChainInfoResponse> for ChainInfoResponse {
    type Error = ProtoConversionError;

    fn try_from(chain_info_response: grpc::ChainInfoResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            info: bcs::from_bytes(chain_info_response.chain_info.as_slice())?,
            signature: map_invert!(chain_info_response.signature),
        })
    }
}

impl From<BlockHeight> for grpc::BlockHeight {
    fn from(block_height: BlockHeight) -> Self {
        Self {
            height: block_height.0,
        }
    }
}

impl From<grpc::BlockHeight> for BlockHeight {
    fn from(block_height: grpc::BlockHeight) -> Self {
        Self(block_height.height)
    }
}

impl From<Owner> for grpc::Owner {
    fn from(owner: Owner) -> Self {
        Self {
            bytes: owner.0.as_bytes().to_vec(),
        }
    }
}

impl TryFrom<grpc::Owner> for Owner {
    type Error = ProtoConversionError;

    fn try_from(owner: grpc::Owner) -> Result<Self, Self::Error> {
        Ok(Self(CryptoHash::try_from(owner.bytes.as_slice())?))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use linera_base::crypto::{BcsSignable, CryptoHash, KeyPair};
    use linera_chain::data_types::{Block, BlockAndRound, HashedValue};
    use linera_core::data_types::ChainInfo;
    use serde::{Deserialize, Serialize};
    use std::fmt::Debug;

    #[derive(Debug, Serialize, Deserialize)]
    struct Foo(String);

    impl BcsSignable for Foo {}

    fn get_block() -> Block {
        Block {
            chain_id: ChainId::root(0),
            epoch: Default::default(),
            incoming_messages: vec![],
            operations: vec![],
            height: Default::default(),
            authenticated_signer: None,
            timestamp: Default::default(),
            previous_block_hash: None,
        }
    }

    /// A convenience function for testing. It converts a type into its
    /// RPC equivalent and back - asserting that the two are equal.
    fn round_trip_check<T, M>(value: T)
    where
        T: TryFrom<M> + Clone + Debug + Eq,
        M: TryFrom<T>,
        T::Error: Debug,
        M::Error: Debug,
    {
        let message = M::try_from(value.clone()).unwrap();
        assert_eq!(value, message.try_into().unwrap());
    }

    #[test]
    pub fn test_public_key() {
        let public_key = KeyPair::generate().public();
        round_trip_check::<_, grpc::PublicKey>(public_key);
    }

    #[test]
    pub fn test_origin() {
        let origin_direct = Origin::chain(ChainId::root(0));
        round_trip_check::<_, grpc::Origin>(origin_direct);

        let effect_id = EffectId {
            chain_id: ChainId::root(0),
            height: BlockHeight(10),
            index: 20,
        };

        let application_id = ApplicationId::User(UserApplicationId {
            bytecode_id: BytecodeId(effect_id),
            creation: effect_id,
        });

        let full_name = ChannelFullName {
            application_id,
            name: vec![].into(),
        };

        let origin_medium = Origin::channel(ChainId::root(0), full_name);
        round_trip_check::<_, grpc::Origin>(origin_medium);
    }

    #[test]
    pub fn test_signature() {
        let key_pair = KeyPair::generate();
        let signature = Signature::new(&Foo("test".into()), &key_pair);
        round_trip_check::<_, grpc::Signature>(signature);
    }

    #[test]
    pub fn test_owner() {
        let key_pair = KeyPair::generate();
        let owner = Owner::from(key_pair.public());
        round_trip_check::<_, grpc::Owner>(owner);
    }

    #[test]
    pub fn test_block_height() {
        let block_height = BlockHeight::from(10);
        round_trip_check::<_, grpc::BlockHeight>(block_height);
    }

    #[test]
    pub fn validator_name() {
        let validator_name = ValidatorName::from(KeyPair::generate().public());
        // This is a correct comparison - `ValidatorNameRpc` does not exist in our
        // proto definitions.
        round_trip_check::<_, grpc::PublicKey>(validator_name);
    }

    #[test]
    pub fn test_chain_id() {
        let chain_id = ChainId::root(0);
        round_trip_check::<_, grpc::ChainId>(chain_id);
    }

    #[test]
    pub fn test_application_id() {
        round_trip_check::<_, grpc::ApplicationId>(ApplicationId::System);

        let effect_id = EffectId {
            chain_id: ChainId::root(0),
            height: BlockHeight(10),
            index: 20,
        };

        let application_id_user = ApplicationId::User(UserApplicationId {
            bytecode_id: BytecodeId(effect_id),
            creation: effect_id,
        });

        round_trip_check::<_, grpc::ApplicationId>(application_id_user);
    }

    #[test]
    pub fn test_block_height_range() {
        let block_height_range_none = BlockHeightRange {
            start: BlockHeight::from(10),
            limit: None,
        };
        round_trip_check::<_, grpc::BlockHeightRange>(block_height_range_none);

        let block_height_range_some = BlockHeightRange {
            start: BlockHeight::from(10),
            limit: Some(20),
        };
        round_trip_check::<_, grpc::BlockHeightRange>(block_height_range_some);
    }

    #[test]
    pub fn test_chain_info_response() {
        let chain_info = ChainInfo {
            chain_id: ChainId::root(0),
            epoch: None,
            description: None,
            manager: Default::default(),
            system_balance: Default::default(),
            block_hash: None,
            timestamp: Default::default(),
            next_block_height: Default::default(),
            state_hash: None,
            requested_committees: None,
            requested_pending_messages: vec![],
            requested_sent_certificates: vec![],
            count_received_log: 0,
            requested_received_log: vec![],
            requested_blob: None,
        };

        let chain_info_response_none = ChainInfoResponse {
            // `info` is bcs so no need to test conversions extensively
            info: chain_info.clone(),
            signature: None,
        };
        round_trip_check::<_, grpc::ChainInfoResponse>(chain_info_response_none);

        let chain_info_response_some = ChainInfoResponse {
            // `info` is bcs so no need to test conversions extensively
            info: chain_info,
            signature: Some(Signature::new(&Foo("test".into()), &KeyPair::generate())),
        };
        round_trip_check::<_, grpc::ChainInfoResponse>(chain_info_response_some);
    }

    #[test]
    pub fn test_chain_info_query() {
        let chain_info_query_none = ChainInfoQuery::new(ChainId::root(0));
        round_trip_check::<_, grpc::ChainInfoQuery>(chain_info_query_none);

        let chain_info_query_some = ChainInfoQuery {
            chain_id: ChainId::root(0),
            test_next_block_height: Some(BlockHeight::from(10)),
            request_committees: false,
            request_pending_messages: false,
            request_sent_certificates_in_range: Some(BlockHeightRange {
                start: BlockHeight::from(3),
                limit: Some(5),
            }),
            request_received_log_excluding_first_nth: None,
            request_manager_values: false,
            request_blob: None,
        };
        round_trip_check::<_, grpc::ChainInfoQuery>(chain_info_query_some);
    }

    #[test]
    pub fn test_lite_certificate() {
        let key_pair = KeyPair::generate();
        let certificate_validated = LiteCertificate {
            value: LiteValue {
                value_hash: CryptoHash::new(&Foo("value".into())),
                chain_id: ChainId::root(0),
            },
            signatures: vec![(
                ValidatorName::from(key_pair.public()),
                Signature::new(&Foo("test".into()), &key_pair),
            )],
        };

        round_trip_check::<_, grpc::LiteCertificate>(certificate_validated);
    }

    #[test]
    pub fn test_certificate() {
        let key_pair = KeyPair::generate();
        let certificate_validated = Certificate::new(
            HashedValue::new_validated(
                get_block(),
                vec![],
                CryptoHash::new(&Foo("test".into())),
                Default::default(),
            ),
            vec![(
                ValidatorName::from(key_pair.public()),
                Signature::new(&Foo("test".into()), &key_pair),
            )],
        );

        round_trip_check::<_, grpc::Certificate>(certificate_validated);
    }

    #[test]
    pub fn test_cross_chain_request() {
        let cross_chain_request_update_recipient = UpdateRecipient {
            height_map: vec![(Medium::Direct, vec![])],
            sender: ChainId::root(0),
            recipient: ChainId::root(0),
            certificates: vec![],
        };
        round_trip_check::<_, grpc::CrossChainRequest>(cross_chain_request_update_recipient);

        let cross_chain_request_confirm_updated_recipient = ConfirmUpdatedRecipient {
            origin: Origin::chain(ChainId::root(0)),
            recipient: ChainId::root(0),
            height: Default::default(),
        };
        round_trip_check::<_, grpc::CrossChainRequest>(
            cross_chain_request_confirm_updated_recipient,
        );
    }

    #[test]
    pub fn test_block_proposal() {
        let block_proposal = BlockProposal {
            content: BlockAndRound {
                block: get_block(),
                round: Default::default(),
            },
            owner: Owner::from(KeyPair::generate().public()),
            signature: Signature::new(&Foo("test".into()), &KeyPair::generate()),
            blobs: vec![HashedValue::new_confirmed(
                get_block(),
                vec![],
                CryptoHash::new(&Foo("execution state".into())),
            )],
        };

        round_trip_check::<_, grpc::BlockProposal>(block_proposal);
    }

    #[test]
    pub fn test_notification() {
        let notification = Notification {
            chain_id: ChainId::root(0),
            reason: Reason::NewBlock {
                height: BlockHeight(0),
            },
        };
        round_trip_check::<_, grpc::Notification>(notification);
    }

    #[test]
    pub fn test_reason() {
        let reason_new_block = Reason::NewBlock {
            height: BlockHeight(0),
        };
        round_trip_check::<_, grpc::Reason>(reason_new_block);

        let reason_new_message = Reason::NewMessage {
            origin: Origin::chain(ChainId::root(0)),
            height: BlockHeight(0),
        };
        round_trip_check::<_, grpc::Reason>(reason_new_message);
    }
}
