// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    data_types::{Blob, BlobContent, BlockHeight},
    identifiers::{BlobId, ChainId},
};
use linera_chain::types::ConfirmedBlockCertificate;

use crate::client::requests_scheduler::cache::SubsumingKey;

/// Unique identifier for different types of download requests.
///
/// Used for request deduplication to avoid redundant downloads of the same data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RequestKey {
    /// Download certificates by specific heights
    Certificates {
        chain_id: ChainId,
        heights: Vec<BlockHeight>,
    },
    /// Download a blob by ID
    Blob(BlobId),
    /// Download a pending blob
    PendingBlob { chain_id: ChainId, blob_id: BlobId },
    /// Download certificate for a specific blob
    CertificateForBlob(BlobId),
}

impl RequestKey {
    /// Returns the chain ID associated with the request, if applicable.
    pub(super) fn chain_id(&self) -> Option<ChainId> {
        match self {
            RequestKey::Certificates { chain_id, .. } => Some(*chain_id),
            RequestKey::PendingBlob { chain_id, .. } => Some(*chain_id),
            _ => None,
        }
    }

    /// Converts certificate-related requests to a common representation of (chain_id, sorted heights).
    ///
    /// This helper method normalizes both `Certificates` and `CertificatesByHeights` variants
    /// into a uniform format for easier comparison and overlap detection.
    ///
    /// # Returns
    /// - `Some((chain_id, heights))` for certificate requests, where heights are sorted
    /// - `None` for non-certificate requests (Blob, PendingBlob, CertificateForBlob)
    fn heights(&self) -> Option<Vec<BlockHeight>> {
        match self {
            RequestKey::Certificates { heights, .. } => Some(heights.clone()),
            _ => None,
        }
    }
}

/// Result types that can be shared across deduplicated requests
#[derive(Debug, Clone)]
pub enum RequestResult {
    Certificates(Vec<ConfirmedBlockCertificate>),
    Blob(Option<Blob>),
    BlobContent(BlobContent),
    Certificate(Box<ConfirmedBlockCertificate>),
}

/// Marker trait for types that can be converted to/from `RequestResult`
/// for use in the requests cache.
pub trait Cacheable: TryFrom<RequestResult> + Into<RequestResult> {}
impl<T> Cacheable for T where T: TryFrom<RequestResult> + Into<RequestResult> {}

impl From<Option<Blob>> for RequestResult {
    fn from(blob: Option<Blob>) -> Self {
        RequestResult::Blob(blob)
    }
}

impl From<Vec<ConfirmedBlockCertificate>> for RequestResult {
    fn from(certs: Vec<ConfirmedBlockCertificate>) -> Self {
        RequestResult::Certificates(certs)
    }
}

impl From<BlobContent> for RequestResult {
    fn from(content: BlobContent) -> Self {
        RequestResult::BlobContent(content)
    }
}

impl From<ConfirmedBlockCertificate> for RequestResult {
    fn from(cert: ConfirmedBlockCertificate) -> Self {
        RequestResult::Certificate(Box::new(cert))
    }
}

impl TryFrom<RequestResult> for Option<Blob> {
    type Error = ();

    fn try_from(result: RequestResult) -> Result<Self, Self::Error> {
        match result {
            RequestResult::Blob(blob) => Ok(blob),
            _ => Err(()),
        }
    }
}

impl TryFrom<RequestResult> for Vec<ConfirmedBlockCertificate> {
    type Error = ();

    fn try_from(result: RequestResult) -> Result<Self, Self::Error> {
        match result {
            RequestResult::Certificates(certs) => Ok(certs),
            _ => Err(()),
        }
    }
}

impl TryFrom<RequestResult> for BlobContent {
    type Error = ();

    fn try_from(result: RequestResult) -> Result<Self, Self::Error> {
        match result {
            RequestResult::BlobContent(content) => Ok(content),
            _ => Err(()),
        }
    }
}

impl TryFrom<RequestResult> for ConfirmedBlockCertificate {
    type Error = ();

    fn try_from(result: RequestResult) -> Result<Self, Self::Error> {
        match result {
            RequestResult::Certificate(cert) => Ok(*cert),
            _ => Err(()),
        }
    }
}

impl SubsumingKey<RequestResult> for super::request::RequestKey {
    fn subsumes(&self, other: &Self) -> bool {
        // Different chains can't subsume each other
        if self.chain_id() != other.chain_id() {
            return false;
        }

        let (in_flight_req_heights, new_req_heights) = match (self.heights(), other.heights()) {
            (Some(range1), Some(range2)) => (range1, range2),
            _ => return false, // We subsume only certificate requests
        };

        let mut in_flight_req_heights_iter = in_flight_req_heights.into_iter();

        for new_height in new_req_heights {
            if !in_flight_req_heights_iter.any(|h| h == new_height) {
                return false; // Found a height not covered by in-flight request
            }
        }
        true
    }

    fn try_extract_result(
        &self,
        in_flight_request: &RequestKey,
        result: &RequestResult,
    ) -> Option<RequestResult> {
        // Only certificate results can be extracted
        let certificates = match result {
            RequestResult::Certificates(certs) => certs,
            _ => return None,
        };

        if !in_flight_request.subsumes(self) {
            return None; // Can't extract if not subsumed
        }

        let mut requested_heights = self.heights()?;
        if requested_heights.is_empty() {
            return Some(RequestResult::Certificates(vec![])); // Nothing requested
        }
        let mut certificates_iter = certificates.iter();
        let mut collected = vec![];
        while let Some(height) = requested_heights.first() {
            // Remove certs below the requested height.
            if let Some(cert) = certificates_iter.find(|cert| &cert.value().height() == height) {
                collected.push(cert.clone());
                requested_heights.remove(0);
            } else {
                return None; // Missing a requested height
            }
        }

        Some(RequestResult::Certificates(collected))
    }
}

#[cfg(test)]
mod tests {
    use linera_base::{crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId};

    use super::{RequestKey, SubsumingKey};

    #[test]
    fn test_subsumes_complete_containment() {
        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let large = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(11), BlockHeight(12), BlockHeight(13)],
        };
        let small = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(12)],
        };
        assert!(large.subsumes(&small));
        assert!(!small.subsumes(&large));
    }

    #[test]
    fn test_subsumes_partial_containment() {
        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let req1 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(12), BlockHeight(13)],
        };
        let req2 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(12), BlockHeight(14)],
        };
        assert!(!req1.subsumes(&req2));
        assert!(!req2.subsumes(&req1));
    }

    #[test]
    fn test_subsumes_different_chains() {
        let chain1 = ChainId(CryptoHash::test_hash("chain1"));
        let chain2 = ChainId(CryptoHash::test_hash("chain2"));
        let req1 = RequestKey::Certificates {
            chain_id: chain1,
            heights: vec![BlockHeight(12)],
        };
        let req2 = RequestKey::Certificates {
            chain_id: chain2,
            heights: vec![BlockHeight(12)],
        };
        assert!(!req1.subsumes(&req2));
    }

    // Helper function to create a test certificate at a specific height
    fn make_test_cert(
        height: u64,
        chain_id: ChainId,
    ) -> linera_chain::types::ConfirmedBlockCertificate {
        use linera_base::{
            crypto::ValidatorKeypair,
            data_types::{Round, Timestamp},
        };
        use linera_chain::{
            block::ConfirmedBlock,
            data_types::{BlockExecutionOutcome, LiteValue, LiteVote},
            test::{make_first_block, BlockTestExt, VoteTestExt},
        };

        let keypair = ValidatorKeypair::generate();
        let mut proposed_block = make_first_block(chain_id).with_timestamp(Timestamp::from(height));

        // Set the correct height
        proposed_block.height = BlockHeight(height);

        // Create a Block from the proposed block with default execution outcome
        let block = BlockExecutionOutcome::default().with(proposed_block);

        // Create a ConfirmedBlock
        let confirmed_block = ConfirmedBlock::new(block);

        // Create a LiteVote and convert to Vote
        let lite_vote = LiteVote::new(
            LiteValue::new(&confirmed_block),
            Round::MultiLeader(0),
            &keypair.secret_key,
        );

        // Convert to full vote
        let vote = lite_vote.with_value(confirmed_block).unwrap();

        // Convert vote to certificate
        vote.into_certificate(keypair.secret_key.public())
    }

    #[test]
    fn test_try_extract_result_non_certificate_result() {
        use super::RequestResult;

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let req1 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(12)],
        };
        let req2 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(12)],
        };

        // Non-certificate result should return None
        let blob_result = RequestResult::Blob(None);
        assert!(req1.try_extract_result(&req2, &blob_result).is_none());
    }

    #[test]
    fn test_try_extract_result_empty_request_range() {
        use super::RequestResult;

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let req1 = RequestKey::Certificates {
            chain_id,
            heights: vec![],
        };
        let req2 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(10)],
        };

        let certs = vec![make_test_cert(10, chain_id)];
        let result = RequestResult::Certificates(certs);

        // Empty request is always extractable, should return empty result
        match req1.try_extract_result(&req2, &result) {
            Some(RequestResult::Certificates(extracted_certs)) => {
                assert!(extracted_certs.is_empty());
            }
            _ => panic!("Expected Some empty Certificates result"),
        }
    }

    #[test]
    fn test_try_extract_result_empty_result_range() {
        use super::RequestResult;

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let req1 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(12)],
        };
        let req2 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(12)],
        };

        let result = RequestResult::Certificates(vec![]); // Empty result

        // Empty result should return None
        assert!(req1.try_extract_result(&req2, &result).is_none());
    }

    #[test]
    fn test_try_extract_result_non_overlapping_ranges() {
        use super::RequestResult;

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let new_req = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(10)],
        };
        let in_flight_req = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(11)],
        };

        // Result does not contain all requested heights
        let certs = vec![make_test_cert(11, chain_id)];
        let result = RequestResult::Certificates(certs);

        // No overlap, should return None
        assert!(new_req
            .try_extract_result(&in_flight_req, &result)
            .is_none());
    }

    #[test]
    fn test_try_extract_result_partial_overlap_missing_start() {
        use super::RequestResult;

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let req1 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(10), BlockHeight(11), BlockHeight(12)],
        };
        let req2 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(11), BlockHeight(12)],
        };

        // Result missing the first height (10)
        let certs = vec![make_test_cert(11, chain_id), make_test_cert(12, chain_id)];
        let result = RequestResult::Certificates(certs);

        // Missing start height, should return None
        assert!(req1.try_extract_result(&req2, &result).is_none());
    }

    #[test]
    fn test_try_extract_result_partial_overlap_missing_end() {
        use super::RequestResult;

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let req1 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(10), BlockHeight(11), BlockHeight(12)],
        };
        let req2 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(10), BlockHeight(11)],
        };

        // Result missing the last height (14)
        let certs = vec![make_test_cert(10, chain_id), make_test_cert(11, chain_id)];
        let result = RequestResult::Certificates(certs);

        // Missing end height, should return None
        assert!(req1.try_extract_result(&req2, &result).is_none());
    }

    #[test]
    fn test_try_extract_result_partial_overlap_missing_middle() {
        use super::RequestResult;

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let new_req = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(10), BlockHeight(12), BlockHeight(13)],
        };
        let in_flight_req = RequestKey::Certificates {
            chain_id,
            heights: vec![
                BlockHeight(10),
                BlockHeight(12),
                BlockHeight(13),
                BlockHeight(14),
            ],
        };

        let certs = vec![
            make_test_cert(10, chain_id),
            make_test_cert(13, chain_id),
            make_test_cert(14, chain_id),
        ];
        let result = RequestResult::Certificates(certs);

        assert!(new_req
            .try_extract_result(&in_flight_req, &result)
            .is_none());
        assert!(in_flight_req
            .try_extract_result(&new_req, &result)
            .is_none());
    }

    #[test]
    fn test_try_extract_result_exact_match() {
        use super::RequestResult;

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let req1 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(10), BlockHeight(11), BlockHeight(12)],
        }; // [10, 11, 12]
        let req2 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(10), BlockHeight(11), BlockHeight(12)],
        };

        let certs = vec![
            make_test_cert(10, chain_id),
            make_test_cert(11, chain_id),
            make_test_cert(12, chain_id),
        ];
        let result = RequestResult::Certificates(certs.clone());

        // Exact match should return all certificates
        let extracted = req1.try_extract_result(&req2, &result);
        assert!(extracted.is_some());
        match extracted.unwrap() {
            RequestResult::Certificates(extracted_certs) => {
                assert_eq!(extracted_certs, certs);
            }
            _ => panic!("Expected Certificates result"),
        }
    }

    #[test]
    fn test_try_extract_result_superset_extraction() {
        use super::RequestResult;

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let req1 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(12), BlockHeight(13)],
        };
        let req2 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(12), BlockHeight(13)],
        };

        // Result has more certificates than requested
        let certs = vec![
            make_test_cert(10, chain_id),
            make_test_cert(11, chain_id),
            make_test_cert(12, chain_id),
            make_test_cert(13, chain_id),
            make_test_cert(14, chain_id),
        ];
        let result = RequestResult::Certificates(certs);

        // Should extract only the requested range [12, 13]
        let extracted = req1.try_extract_result(&req2, &result);
        assert!(extracted.is_some());
        match extracted.unwrap() {
            RequestResult::Certificates(extracted_certs) => {
                assert_eq!(extracted_certs.len(), 2);
                assert_eq!(extracted_certs[0].value().height(), BlockHeight(12));
                assert_eq!(extracted_certs[1].value().height(), BlockHeight(13));
            }
            _ => panic!("Expected Certificates result"),
        }
    }

    #[test]
    fn test_try_extract_result_single_height() {
        use super::RequestResult;

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let req1 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(15)],
        }; // [15]
        let req2 = RequestKey::Certificates {
            chain_id,
            heights: vec![BlockHeight(10), BlockHeight(15), BlockHeight(20)],
        };

        let certs = vec![
            make_test_cert(10, chain_id),
            make_test_cert(15, chain_id),
            make_test_cert(20, chain_id),
        ];
        let result = RequestResult::Certificates(certs);

        // Should extract only height 15
        let extracted = req1.try_extract_result(&req2, &result);
        assert!(extracted.is_some());
        match extracted.unwrap() {
            RequestResult::Certificates(extracted_certs) => {
                assert_eq!(extracted_certs.len(), 1);
                assert_eq!(extracted_certs[0].value().height(), BlockHeight(15));
            }
            _ => panic!("Expected Certificates result"),
        }
    }

    #[test]
    fn test_try_extract_result_different_chains() {
        use super::RequestResult;

        let chain1 = ChainId(CryptoHash::test_hash("chain1"));
        let chain2 = ChainId(CryptoHash::test_hash("chain2"));
        let req1 = RequestKey::Certificates {
            chain_id: chain1,
            heights: vec![BlockHeight(12)],
        };
        let req2 = RequestKey::Certificates {
            chain_id: chain2,
            heights: vec![BlockHeight(12)],
        };

        let certs = vec![make_test_cert(12, chain1)];
        let result = RequestResult::Certificates(certs);

        // Different chains should return None
        assert!(req1.try_extract_result(&req2, &result).is_none());
    }
}
