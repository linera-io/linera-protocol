// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    grpc_network::{grpc, grpc::ChainInfoResult},
    HandleCertificateRequest, HandleLiteCertificateRequest,
};
use ed25519::signature::Signature as edSignature;
use linera_base::{
    crypto::{CryptoError, CryptoHash, PublicKey, Signature},
    data_types::{BlockHeight, RoundNumber},
    ensure,
    identifiers::{ChainId, Owner},
};
use linera_chain::data_types::{
    BlockAndRound, BlockProposal, Certificate, HashedValue, LiteCertificate, LiteValue,
};
use linera_core::{
    data_types::{ChainInfoQuery, ChainInfoResponse, CrossChainRequest},
    node::NodeError,
    worker::Notification,
};
use linera_execution::committee::ValidatorName;
use thiserror::Error;
use tonic::{Code, Status};

#[derive(Error, Debug)]
pub enum ProtoConversionError {
    #[error(transparent)]
    BincodeError(#[from] bincode::Error),
    #[error("Conversion failed due to missing field")]
    MissingField,
    #[error("Signature error: {0}")]
    SignatureError(#[from] ed25519_dalek::SignatureError),
    #[error("Cryptographic error: {0}")]
    CryptoError(#[from] CryptoError),
    #[error("Inconsistent outer/inner chain ids")]
    InconsistentChainId,
}

/// Extracts an optional field from a Proto type and tries to map it.
fn try_proto_convert<S, T>(t: Option<T>) -> Result<S, ProtoConversionError>
where
    T: TryInto<S, Error = ProtoConversionError>,
{
    t.ok_or(ProtoConversionError::MissingField)?.try_into()
}

impl From<ProtoConversionError> for Status {
    fn from(error: ProtoConversionError) -> Self {
        Status::new(Code::InvalidArgument, error.to_string())
    }
}

impl TryFrom<Notification> for grpc::Notification {
    type Error = ProtoConversionError;

    fn try_from(notification: Notification) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_id: Some(notification.chain_id.into()),
            reason: bincode::serialize(&notification.reason)?,
        })
    }
}

impl TryFrom<grpc::Notification> for Notification {
    type Error = ProtoConversionError;

    fn try_from(notification: grpc::Notification) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_id: try_proto_convert(notification.chain_id)?,
            reason: bincode::deserialize(&notification.reason)?,
        })
    }
}

impl TryFrom<ChainInfoResponse> for ChainInfoResult {
    type Error = ProtoConversionError;

    fn try_from(chain_info_response: ChainInfoResponse) -> Result<Self, Self::Error> {
        let response = chain_info_response.try_into()?;
        Ok(ChainInfoResult {
            inner: Some(grpc::chain_info_result::Inner::ChainInfoResponse(response)),
        })
    }
}

impl TryFrom<NodeError> for ChainInfoResult {
    type Error = ProtoConversionError;

    fn try_from(node_error: NodeError) -> Result<Self, Self::Error> {
        let error = bincode::serialize(&node_error)?;
        Ok(ChainInfoResult {
            inner: Some(grpc::chain_info_result::Inner::Error(error)),
        })
    }
}

impl TryFrom<BlockProposal> for grpc::BlockProposal {
    type Error = ProtoConversionError;

    fn try_from(block_proposal: BlockProposal) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_id: Some(block_proposal.content.block.chain_id.into()),
            content: bincode::serialize(&block_proposal.content)?,
            owner: Some(block_proposal.owner.into()),
            signature: Some(block_proposal.signature.into()),
            blobs: bincode::serialize(&block_proposal.blobs)?,
        })
    }
}

impl TryFrom<grpc::BlockProposal> for BlockProposal {
    type Error = ProtoConversionError;

    fn try_from(block_proposal: grpc::BlockProposal) -> Result<Self, Self::Error> {
        let content: BlockAndRound = bincode::deserialize(&block_proposal.content)?;
        ensure!(
            Some(content.block.chain_id.into()) == block_proposal.chain_id,
            ProtoConversionError::InconsistentChainId
        );
        Ok(Self {
            content,
            owner: try_proto_convert(block_proposal.owner)?,
            signature: try_proto_convert(block_proposal.signature)?,
            blobs: bincode::deserialize(&block_proposal.blobs)?,
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
                height_map,
                sender,
                recipient,
                certificates,
            }) => CrossChainRequest::UpdateRecipient {
                height_map: bincode::deserialize(&height_map)?,
                sender: try_proto_convert(sender)?,
                recipient: try_proto_convert(recipient)?,
                certificates: bincode::deserialize(&certificates)?,
            },
            Inner::ConfirmUpdatedRecipient(grpc::ConfirmUpdatedRecipient {
                sender,
                recipient,
                latest_heights,
            }) => CrossChainRequest::ConfirmUpdatedRecipient {
                sender: try_proto_convert(sender)?,
                recipient: try_proto_convert(recipient)?,
                latest_heights: bincode::deserialize(&latest_heights)?,
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
            CrossChainRequest::UpdateRecipient {
                height_map,
                sender,
                recipient,
                certificates,
            } => Inner::UpdateRecipient(grpc::UpdateRecipient {
                height_map: bincode::serialize(&height_map)?,
                sender: Some(sender.into()),
                recipient: Some(recipient.into()),
                certificates: bincode::serialize(&certificates)?,
            }),
            CrossChainRequest::ConfirmUpdatedRecipient {
                sender,
                recipient,
                latest_heights,
            } => Inner::ConfirmUpdatedRecipient(grpc::ConfirmUpdatedRecipient {
                sender: Some(sender.into()),
                recipient: Some(recipient.into()),
                latest_heights: bincode::serialize(&latest_heights)?,
            }),
        };
        Ok(Self { inner: Some(inner) })
    }
}

impl<'a> TryFrom<grpc::LiteCertificate> for HandleLiteCertificateRequest<'a> {
    type Error = ProtoConversionError;

    fn try_from(certificate: grpc::LiteCertificate) -> Result<Self, Self::Error> {
        let value = LiteValue {
            value_hash: CryptoHash::try_from(certificate.hash.as_slice())?,
            chain_id: try_proto_convert(certificate.chain_id)?,
        };
        let signatures = bincode::deserialize(&certificate.signatures)?;
        Ok(Self {
            certificate: LiteCertificate::new(value, RoundNumber(certificate.round), signatures),
            wait_for_outgoing_messages: certificate.wait_for_outgoing_messages,
        })
    }
}

impl<'a> TryFrom<HandleLiteCertificateRequest<'a>> for grpc::LiteCertificate {
    type Error = ProtoConversionError;

    fn try_from(request: HandleLiteCertificateRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            hash: request.certificate.value.value_hash.as_bytes().to_vec(),
            round: request.certificate.round.0,
            chain_id: Some(request.certificate.value.chain_id.into()),
            signatures: bincode::serialize(&request.certificate.signatures)?,
            wait_for_outgoing_messages: request.wait_for_outgoing_messages,
        })
    }
}

impl TryFrom<grpc::Certificate> for HandleCertificateRequest {
    type Error = ProtoConversionError;

    fn try_from(cert_request: grpc::Certificate) -> Result<Self, Self::Error> {
        let value: HashedValue = bincode::deserialize(&cert_request.value)?;
        ensure!(
            Some(value.inner().chain_id().into()) == cert_request.chain_id,
            ProtoConversionError::InconsistentChainId
        );
        let signatures = bincode::deserialize(&cert_request.signatures)?;
        let blobs = bincode::deserialize(&cert_request.blobs)?;
        Ok(HandleCertificateRequest {
            certificate: Certificate::new(value, RoundNumber(cert_request.round), signatures),
            wait_for_outgoing_messages: cert_request.wait_for_outgoing_messages,
            blobs,
        })
    }
}

impl TryFrom<HandleCertificateRequest> for grpc::Certificate {
    type Error = ProtoConversionError;

    fn try_from(request: HandleCertificateRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_id: Some(request.certificate.value().chain_id().into()),
            value: bincode::serialize(&request.certificate.value)?,
            round: request.certificate.round.0,
            signatures: bincode::serialize(&request.certificate.signatures)?,
            blobs: bincode::serialize(&request.blobs)?,
            wait_for_outgoing_messages: request.wait_for_outgoing_messages,
        })
    }
}

impl TryFrom<grpc::ChainInfoQuery> for ChainInfoQuery {
    type Error = ProtoConversionError;

    fn try_from(chain_info_query: grpc::ChainInfoQuery) -> Result<Self, Self::Error> {
        let request_sent_certificates_in_range = chain_info_query
            .request_sent_certificates_in_range
            .map(|range| bincode::deserialize(&range))
            .transpose()?;
        let request_blob = chain_info_query
            .request_blob
            .map(|bytes| bincode::deserialize(&bytes))
            .transpose()?;

        Ok(Self {
            request_committees: chain_info_query.request_committees,
            request_pending_messages: chain_info_query.request_pending_messages,
            chain_id: try_proto_convert(chain_info_query.chain_id)?,
            request_sent_certificates_in_range,
            request_received_log_excluding_first_nth: chain_info_query
                .request_received_log_excluding_first_nth,
            test_next_block_height: chain_info_query.test_next_block_height.map(Into::into),
            request_manager_values: chain_info_query.request_manager_values,
            request_blob,
        })
    }
}

impl TryFrom<ChainInfoQuery> for grpc::ChainInfoQuery {
    type Error = ProtoConversionError;

    fn try_from(chain_info_query: ChainInfoQuery) -> Result<Self, Self::Error> {
        let request_sent_certificates_in_range = chain_info_query
            .request_sent_certificates_in_range
            .map(|range| bincode::serialize(&range))
            .transpose()?;
        let request_blob = chain_info_query
            .request_blob
            .map(|hash| bincode::serialize(&hash))
            .transpose()?;

        Ok(Self {
            chain_id: Some(chain_info_query.chain_id.into()),
            request_committees: chain_info_query.request_committees,
            request_pending_messages: chain_info_query.request_pending_messages,
            test_next_block_height: chain_info_query.test_next_block_height.map(Into::into),
            request_sent_certificates_in_range,
            request_received_log_excluding_first_nth: chain_info_query
                .request_received_log_excluding_first_nth,
            request_manager_values: chain_info_query.request_manager_values,
            request_blob,
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
            chain_info: bincode::serialize(&chain_info_response.info)?,
            signature: chain_info_response.signature.map(Into::into),
        })
    }
}

impl TryFrom<grpc::ChainInfoResponse> for ChainInfoResponse {
    type Error = ProtoConversionError;

    fn try_from(chain_info_response: grpc::ChainInfoResponse) -> Result<Self, Self::Error> {
        let signature = chain_info_response
            .signature
            .map(TryInto::try_into)
            .transpose()?;
        let info = bincode::deserialize(chain_info_response.chain_info.as_slice())?;
        Ok(Self { info, signature })
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
    use linera_chain::data_types::{Block, BlockAndRound, ExecutedBlock, HashedValue};
    use linera_core::data_types::ChainInfo;
    use serde::{Deserialize, Serialize};
    use std::{borrow::Cow, fmt::Debug};

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
            // `info` is bincode so no need to test conversions extensively
            info: chain_info.clone(),
            signature: None,
        };
        round_trip_check::<_, grpc::ChainInfoResponse>(chain_info_response_none);

        let chain_info_response_some = ChainInfoResponse {
            // `info` is bincode so no need to test conversions extensively
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
            request_sent_certificates_in_range: Some(linera_core::data_types::BlockHeightRange {
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
        let certificate = LiteCertificate {
            value: LiteValue {
                value_hash: CryptoHash::new(&Foo("value".into())),
                chain_id: ChainId::root(0),
            },
            round: RoundNumber(2),
            signatures: Cow::Owned(vec![(
                ValidatorName::from(key_pair.public()),
                Signature::new(&Foo("test".into()), &key_pair),
            )]),
        };
        let request = HandleLiteCertificateRequest {
            certificate,
            wait_for_outgoing_messages: true,
        };

        round_trip_check::<_, grpc::LiteCertificate>(request);
    }

    #[test]
    pub fn test_certificate() {
        let key_pair = KeyPair::generate();
        let certificate = Certificate::new(
            HashedValue::new_validated(ExecutedBlock {
                block: get_block(),
                messages: vec![],
                state_hash: CryptoHash::new(&Foo("test".into())),
            }),
            RoundNumber(3),
            vec![(
                ValidatorName::from(key_pair.public()),
                Signature::new(&Foo("test".into()), &key_pair),
            )],
        );
        let blobs = vec![HashedValue::new_validated(ExecutedBlock {
            block: get_block(),
            messages: vec![],
            state_hash: CryptoHash::new(&Foo("also test".into())),
        })];
        let request = HandleCertificateRequest {
            certificate,
            blobs,
            wait_for_outgoing_messages: false,
        };

        round_trip_check::<_, grpc::Certificate>(request);
    }

    #[test]
    pub fn test_cross_chain_request() {
        let cross_chain_request_update_recipient = CrossChainRequest::UpdateRecipient {
            height_map: vec![(linera_chain::data_types::Medium::Direct, vec![])],
            sender: ChainId::root(0),
            recipient: ChainId::root(0),
            certificates: vec![],
        };
        round_trip_check::<_, grpc::CrossChainRequest>(cross_chain_request_update_recipient);

        let cross_chain_request_confirm_updated_recipient =
            CrossChainRequest::ConfirmUpdatedRecipient {
                sender: ChainId::root(0),
                recipient: ChainId::root(0),
                latest_heights: vec![(
                    linera_chain::data_types::Medium::Direct,
                    Default::default(),
                )],
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
            blobs: vec![HashedValue::new_confirmed(ExecutedBlock {
                block: get_block(),
                messages: vec![],
                state_hash: CryptoHash::new(&Foo("execution state".into())),
            })],
        };

        round_trip_check::<_, grpc::BlockProposal>(block_proposal);
    }

    #[test]
    pub fn test_notification() {
        let notification = Notification {
            chain_id: ChainId::root(0),
            reason: linera_core::worker::Reason::NewBlock {
                height: BlockHeight(0),
            },
        };
        round_trip_check::<_, grpc::Notification>(notification);
    }
}
