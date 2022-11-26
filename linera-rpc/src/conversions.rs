use ed25519::signature::Signature as edSignature;
use thiserror::Error;
use tonic::{Code, Status};

use crate::grpc_network::grpc::{
    ChainInfoResult, ConfirmUpdateRecipient, CrossChainRequest as CrossChainRequestRpc,
    NameSignaturePair, UpdateRecipient,
};
use linera_core::messages::CrossChainRequest;

use crate::grpc_network::grpc::BlockProposal as BlockProposalRpc;
use linera_chain::messages::BlockProposal;

use crate::grpc_network::grpc::Certificate as CertificateRpc;
use linera_chain::messages::Certificate;

use crate::grpc_network::grpc::ChainInfoQuery as ChainInfoQueryRpc;
use linera_core::messages::ChainInfoQuery;

use crate::grpc_network::grpc::Origin as OriginRpc;
use linera_base::messages::{Origin, ValidatorName};

use crate::grpc_network::grpc::Medium as MediumRpc;
use linera_base::messages::Medium;

use crate::grpc_network::grpc::BlockHeightRange as BlockHeightRangeRPC;
use linera_core::messages::BlockHeightRange;

use crate::grpc_network::grpc::ApplicationId as ApplicationIdRPC;
use linera_base::messages::ApplicationId;

use crate::grpc_network::grpc::ChainId as ChainIdRPC;
use linera_base::messages::ChainId;

use crate::grpc_network::grpc::PublicKey as PublicKeyRPC;
use linera_base::crypto::{CryptoError, PublicKey};

use crate::grpc_network::grpc::Signature as SignatureRPC;
use linera_base::crypto::Signature;

use crate::grpc_network::grpc::ChainInfoResponse as ChainInfoResponseRPC;
use linera_core::messages::ChainInfoResponse;

use crate::grpc_network::grpc::BlockHeight as BlockHeightRPC;
use linera_base::messages::BlockHeight;

use crate::grpc_network::grpc::{cross_chain_request::Inner, Owner as OwnerRPC};
use linera_base::messages::Owner;
use linera_core::node::NodeError;

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

/// Delegates the call to an underlying handler and converts types to and from proto
#[macro_export]
macro_rules! convert_and_delegate {
    ($self:ident, $handler:ident, $req:ident) => {{
        log::debug!(
            "server handler [{}] received delegating request [{:?}] ",
            stringify!($handler),
            $req
        );
        Ok(Response::new(
            match $self
                .state
                .clone()
                .$handler($req.into_inner().try_into()?)
                .await
            {
                Ok(chain_info_response) => chain_info_response.try_into()?,
                Err(error) => NodeError::from(error).into(),
            },
        ))
    }};
}

#[macro_export]
macro_rules! client_delegate {
    ($self:ident, $handler:ident, $req:ident) => {{
        log::debug!(
            "client handler [{}] received delegating request [{:?}] ",
            stringify!($handler),
            $req
        );
        let request = Request::new($req.try_into().expect("todo"));
        match $self
            .0
            .$handler(request)
            .await
            .map_err(|s| NodeError::GrpcError {
                error: format!(
                    "remote request [{}] failed with status: {:?}",
                    stringify!($handler),
                    s
                ),
            })?
            .into_inner()
            .inner
            .ok_or(NodeError::GrpcError {
                error: "missing body from response".to_string(),
            })? {
            Inner::ChainInfoResponse(response) => {
                Ok(response.try_into().map_err(|_| NodeError::GrpcError {
                    error: "failed to marshal response".to_string(),
                })?)
            }
            Inner::Error(error) => {
                Err(bcs::from_bytes(&error).map_err(|_| NodeError::GrpcError {
                    error: "failed to marshal error message".to_string(),
                })?)
            }
        }
    }};
}

#[macro_export]
macro_rules! mass_client_delegate {
    ($client:ident, $handler:ident, $msg:ident, $responses: ident) => {{
        let response = $client.$handler(Request::new((*$msg).try_into()?)).await?;
        match response
            .into_inner()
            .inner
            .ok_or(ProtoConversionError::MissingField)?
        {
            Inner::ChainInfoResponse(chain_info_response) => {
                $responses.push(Message::ChainInfoResponse(Box::new(
                    chain_info_response.try_into()?,
                )));
            }
            Inner::Error(error) => {
                error!(
                    "Received error response: {:?}",
                    bcs::from_bytes::<NodeError>(&error).expect("todo")
                )
            }
        }
    }};
}

impl From<ProtoConversionError> for Status {
    fn from(error: ProtoConversionError) -> Self {
        Status::new(Code::InvalidArgument, error.to_string())
    }
}

impl TryFrom<ChainInfoResponse> for ChainInfoResult {
    type Error = ProtoConversionError;

    fn try_from(chain_info_response: ChainInfoResponse) -> Result<Self, Self::Error> {
        Ok(ChainInfoResult {
            inner: Some(
                crate::grpc_network::grpc::chain_info_result::Inner::ChainInfoResponse(
                    chain_info_response.try_into()?,
                ),
            ),
        })
    }
}

impl From<NodeError> for ChainInfoResult {
    fn from(node_error: NodeError) -> Self {
        ChainInfoResult {
            inner: Some(crate::grpc_network::grpc::chain_info_result::Inner::Error(
                bcs::to_bytes(&node_error).expect("todo"),
            )),
        }
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

        Ok(Certificate::new(
            bcs::from_bytes(certificate.value.as_slice())?,
            signatures,
        ))
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
            Medium::Direct => MediumRpc { channel: None },
            Medium::Channel(channel) => MediumRpc {
                channel: Some(channel),
            },
        }
    }
}

impl From<MediumRpc> for Medium {
    fn from(medium: MediumRpc) -> Self {
        match medium.channel {
            None => Medium::Direct,
            Some(medium) => Medium::Channel(medium),
        }
    }
}

impl From<BlockHeightRange> for BlockHeightRangeRPC {
    fn from(block_height_range: BlockHeightRange) -> Self {
        Self {
            start: Some(block_height_range.start.into()),
            limit: map_as!(block_height_range.limit, u64),
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
            medium: proto_convert!(origin.medium),
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
        Ok(ValidatorName(public_key.try_into()?))
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
        Ok(Self(ed25519_dalek::Signature::from_bytes(
            &signature.bytes,
        )?))
    }
}

impl TryFrom<ChainInfoResponse> for ChainInfoResponseRPC {
    type Error = ProtoConversionError;

    fn try_from(chain_info_response: ChainInfoResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            chain_info: bcs::to_bytes(&chain_info_response.info)?,
            signature: map_into!(chain_info_response.signature),
        })
    }
}

impl TryFrom<ChainInfoResponseRPC> for ChainInfoResponse {
    type Error = ProtoConversionError;

    fn try_from(chain_info_response: ChainInfoResponseRPC) -> Result<Self, Self::Error> {
        Ok(Self {
            info: bcs::from_bytes(chain_info_response.chain_info.as_slice())?,
            signature: map_invert!(chain_info_response.signature),
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
        Self(block_height.height)
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
        Ok(Self(try_proto_convert!(owner.inner)))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use linera_base::crypto::{BcsSignable, HashValue, KeyPair};
    use linera_chain::messages::{Block, BlockAndRound, Value};
    use linera_core::messages::ChainInfo;
    use serde::{Deserialize, Serialize};
    use std::str::FromStr;
    use structopt::lazy_static::lazy_static;

    #[derive(Debug, Serialize, Deserialize)]
    struct Foo(String);

    impl BcsSignable for Foo {}

    lazy_static! {
        static ref CHAIN_ID: ChainId = ChainId::from_str("dc07bbb3e3583738cfccc9489cd0959703d6ae9fd73316ab2fdea0e8bcff2467cbd986a25b352afd422b123aa84e9b680ee6fd9f56c685cb2b5e29d87a2ac5d9").unwrap();
        static ref BLOCK: Block = Block {
                    chain_id: *CHAIN_ID,
                    epoch: Default::default(),
                    incoming_messages: vec![],
                    operations: vec![],
                    height: Default::default(),
                    previous_block_hash: None,
                };
    }

    /// A convenience macro for testing. It converts a type into its
    /// RPC equivalent and back - asserting that the two are equal.
    macro_rules! compare {
        ($msg:ident, $rpc:ident) => {
            let rpc = $rpc::try_from($msg.clone()).unwrap();
            assert_eq!($msg, rpc.try_into().unwrap());
        };
    }

    #[test]
    pub fn test_public_key() {
        let public_key = KeyPair::generate().public();
        compare!(public_key, PublicKeyRPC);
    }

    #[test]
    pub fn test_origin() {
        let origin_direct = Origin::chain(*CHAIN_ID);
        compare!(origin_direct, OriginRpc);

        let origin_medium = Origin::channel(*CHAIN_ID, "some channel".to_string());
        compare!(origin_medium, OriginRpc);
    }

    #[test]
    pub fn test_signature() {
        let key_pair = KeyPair::generate();
        let signature = Signature::new(&Foo("test".into()), &key_pair);
        compare!(signature, SignatureRPC);
    }

    #[test]
    pub fn test_owner() {
        let key_pair = KeyPair::generate();
        let owner = Owner::from(key_pair.public());
        compare!(owner, OwnerRPC);
    }

    #[test]
    pub fn test_block_height() {
        let block_height = BlockHeight::from(10u64);
        compare!(block_height, BlockHeightRPC);
    }

    #[test]
    pub fn validator_name() {
        let validator_name = ValidatorName::from(KeyPair::generate().public());
        // This is a correct comparison - `ValidatorNameRpc` does not exist in our
        // proto definitions.
        compare!(validator_name, PublicKeyRPC);
    }

    #[test]
    pub fn test_chain_id() {
        let chain_id = *CHAIN_ID;
        compare!(chain_id, ChainIdRPC);
    }

    #[test]
    pub fn test_application_id() {
        let application_id = ApplicationId(10u64);
        compare!(application_id, ApplicationIdRPC);
    }

    #[test]
    pub fn test_block_height_range() {
        let block_height_range_none = BlockHeightRange {
            start: BlockHeight::from(10u64),
            limit: None,
        };
        compare!(block_height_range_none, BlockHeightRangeRPC);

        let block_height_range_some = BlockHeightRange {
            start: BlockHeight::from(10u64),
            limit: Some(20),
        };
        compare!(block_height_range_some, BlockHeightRangeRPC);
    }

    #[test]
    pub fn test_chain_info_response() {
        let chain_info = ChainInfo {
            chain_id: *CHAIN_ID,
            epoch: None,
            description: None,
            manager: Default::default(),
            system_balance: Default::default(),
            block_hash: None,
            next_block_height: Default::default(),
            state_hash: None,
            requested_committees: None,
            requested_pending_messages: vec![],
            requested_sent_certificates: vec![],
            count_received_certificates: 0,
            requested_received_certificates: vec![],
        };

        let chain_info_response_none = ChainInfoResponse {
            // `info` is bcs so no need to test conversions extensively
            info: chain_info.clone(),
            signature: None,
        };
        compare!(chain_info_response_none, ChainInfoResponseRPC);

        let chain_info_response_some = ChainInfoResponse {
            // `info` is bcs so no need to test conversions extensively
            info: chain_info,
            signature: Some(Signature::new(&Foo("test".into()), &KeyPair::generate())),
        };
        compare!(chain_info_response_some, ChainInfoResponseRPC);
    }

    #[test]
    pub fn test_chain_info_query() {
        let chain_info_query_none = ChainInfoQuery::new(*CHAIN_ID);
        compare!(chain_info_query_none, ChainInfoQueryRpc);

        let chain_info_query_some = ChainInfoQuery {
            chain_id: *CHAIN_ID,
            test_next_block_height: Some(BlockHeight::from(10)),
            request_committees: false,
            request_pending_messages: false,
            request_sent_certificates_in_range: Some(BlockHeightRange {
                start: BlockHeight::from(3),
                limit: Some(5),
            }),
            request_received_certificates_excluding_first_nth: None,
        };
        compare!(chain_info_query_some, ChainInfoQueryRpc);
    }

    #[test]
    pub fn test_certificate() {
        let key_pair = KeyPair::generate();
        let certificate_validated = Certificate::new(
            Value::ValidatedBlock {
                block: BLOCK.clone(),
                round: Default::default(),
                effects: vec![],
                state_hash: HashValue::new(&Foo("test".into())),
            },
            vec![(
                ValidatorName::from(key_pair.public()),
                Signature::new(&Foo("test".into()), &key_pair),
            )],
        );

        compare!(certificate_validated, CertificateRpc);
    }

    #[test]
    pub fn test_cross_chain_request() {
        let cross_chain_request_update_recipient = CrossChainRequest::UpdateRecipient {
            application_id: ApplicationId(10u64),
            origin: Origin::chain(*CHAIN_ID),
            recipient: *CHAIN_ID,
            certificates: vec![],
        };
        compare!(cross_chain_request_update_recipient, CrossChainRequestRpc);

        let cross_chain_request_confirm_updated_recipient =
            CrossChainRequest::ConfirmUpdatedRecipient {
                application_id: ApplicationId(10u64),
                origin: Origin::chain(*CHAIN_ID),
                recipient: *CHAIN_ID,
                height: Default::default(),
            };
        compare!(
            cross_chain_request_confirm_updated_recipient,
            CrossChainRequestRpc
        );
    }

    #[test]
    pub fn test_block_proposal() {
        let block_proposal = BlockProposal {
            content: BlockAndRound {
                block: BLOCK.clone(),
                round: Default::default(),
            },
            owner: Owner::from(KeyPair::generate().public()),
            signature: Signature::new(&Foo("test".into()), &KeyPair::generate()),
        };

        compare!(block_proposal, BlockProposalRpc);
    }
}
