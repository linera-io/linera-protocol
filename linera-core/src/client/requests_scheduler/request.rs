// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Request deduplication and subsumption for validator downloads.
//!
//! This module provides types and logic for deduplicating concurrent requests
//! to validators, reducing unnecessary network traffic and load.
//!
//! # Supported Request Types
//!
//! - **Certificates**: Download confirmed block certificates by heights
//! - **Blobs**: Download blob data by ID
//! - **ChainInfo**: Query comprehensive chain state information
//!
//! # Subsumption
//!
//! Some request types support subsumption, where one request can satisfy multiple others:
//!
//! ## Certificate Subsumption
//! A request for heights `[10, 11, 12, 13]` can serve a request for `[11, 12]`.
//!
//! ## ChainInfo Subsumption
//! A comprehensive ChainInfoQuery can serve subset queries. For example:
//! - Query A: `request_committees=true`, `request_pending_message_bundles=true`
//! - Query B: `request_committees=true`
//!
//! Query A subsumes B because it requests all the data B needs (and more).
//!
//! # Deduplication Benefits
//!
//! When multiple components need chain information:
//! - **Without deduplication**: Each makes a separate network request
//! - **With deduplication**: One network request serves all callers
//! - **With subsumption**: A comprehensive request can serve multiple subset requests
//!
//! This significantly reduces network traffic and validator load in high-concurrency scenarios.

use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{Blob, BlobContent, BlockHeight},
    identifiers::{BlobId, ChainId},
};
use linera_chain::types::ConfirmedBlockCertificate;

use crate::{
    client::requests_scheduler::cache::SubsumingKey,
    data_types::{ChainInfo, ChainInfoQuery},
};

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
    /// Query chain information with various optional fields
    ///
    /// Includes the validator being queried to prevent deduplication across different validators.
    /// Supports subsumption: a query requesting more data can serve queries requesting subsets.
    /// For example, a query with `request_committees=true` and `request_pending_message_bundles=true`
    /// can serve a query with only `request_committees=true`.
    ChainInfo {
        query: Box<ChainInfoQuery>,
        validator: ValidatorPublicKey,
    },
}

impl RequestKey {
    /// Returns the chain ID associated with the request, if applicable.
    pub(super) fn chain_id(&self) -> Option<ChainId> {
        match self {
            RequestKey::Certificates { chain_id, .. } => Some(*chain_id),
            RequestKey::PendingBlob { chain_id, .. } => Some(*chain_id),
            RequestKey::ChainInfo { query, .. } => Some(query.chain_id),
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

/// Checks if one vector of block heights contains all elements of another.
///
/// Returns `true` if `superset` contains all heights present in `subset`.
/// Order doesn't matter - this is a set membership check.
/// Used for ChainInfo subsumption logic with height vectors.
fn heights_vec_subsumes(superset: &[BlockHeight], subset: &[BlockHeight]) -> bool {
    subset.iter().all(|h| superset.contains(h))
}

/// Result types that can be shared across deduplicated requests
#[derive(Debug, Clone)]
pub enum RequestResult {
    Certificates(Vec<ConfirmedBlockCertificate>),
    Blob(Option<Blob>),
    BlobContent(BlobContent),
    Certificate(Box<ConfirmedBlockCertificate>),
    /// Chain information result
    ///
    /// Contains comprehensive chain state information that can be filtered to serve
    /// subset requests through the subsumption mechanism.
    ChainInfo(Box<ChainInfo>),
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

impl From<Box<ChainInfo>> for RequestResult {
    fn from(info: Box<ChainInfo>) -> Self {
        RequestResult::ChainInfo(info)
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

impl TryFrom<RequestResult> for Box<ChainInfo> {
    type Error = ();

    fn try_from(result: RequestResult) -> Result<Self, Self::Error> {
        match result {
            RequestResult::ChainInfo(info) => Ok(info),
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

        // ChainInfo subsumption rules:
        // Query A subsumes query B if A requests all the data that B requests (and potentially more).
        // This allows a comprehensive query to serve multiple subset queries.
        //
        // Subsumption criteria:
        // 0. validator: Must match exactly (queries to different validators cannot be deduplicated)
        // 1. chain_id: Must match exactly (checked above)
        // 2. Boolean flags: If B requests it, A must also request it
        // 3. Exact-match fields: Must match exactly (owner, test_height, timeout)
        // 4. Collection fields: A must request a superset (certificate heights)
        // 5. Range fields: A must request more or equal entries (received_log)
        if let (
            RequestKey::ChainInfo {
                query: a,
                validator: validator_a,
            },
            RequestKey::ChainInfo {
                query: b,
                validator: validator_b,
            },
        ) = (self, other)
        {
            // Validator must match exactly - queries to different validators cannot be deduplicated
            if validator_a != validator_b {
                return false;
            }

            // Chain ID already checked above

            // Boolean flags: if b requests it (true), a must also request it (true)
            if b.request_committees && !a.request_committees {
                return false;
            }
            if b.request_pending_message_bundles && !a.request_pending_message_bundles {
                return false;
            }
            if b.request_manager_values && !a.request_manager_values {
                return false;
            }
            if b.request_fallback && !a.request_fallback {
                return false;
            }
            if b.create_network_actions && !a.create_network_actions {
                return false;
            }

            // Exact-match fields: must match exactly
            if a.request_owner_balance != b.request_owner_balance {
                return false;
            }
            if a.test_next_block_height != b.test_next_block_height {
                return false;
            }
            if a.request_leader_timeout != b.request_leader_timeout {
                return false;
            }

            // Collection fields: A must request a superset
            // A's vec must contain all elements of B's vec
            if !heights_vec_subsumes(
                &a.request_sent_certificate_hashes_by_heights,
                &b.request_sent_certificate_hashes_by_heights,
            ) {
                return false;
            }

            // Range field: request_received_log_excluding_first_n
            // If B requests Some(n) and A requests Some(m), then m ≤ n (A asks for more or equal entries)
            match (
                a.request_received_log_excluding_first_n,
                b.request_received_log_excluding_first_n,
            ) {
                (Some(a_n), Some(b_n)) => {
                    if a_n > b_n {
                        return false; // A asks for fewer entries than B
                    }
                }
                (None, Some(_)) => {
                    return false; // B requests something, A doesn't
                }
                _ => {
                    // (Some(_), None) or (None, None) - OK
                }
            }

            return true;
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
        // ChainInfo extraction logic:
        // Given a comprehensive ChainInfo response from an in-flight request,
        // extract only the subset of data requested by this query.
        //
        // Process:
        // 1. Verify in_flight_request subsumes self (has all requested data)
        // 2. Copy all base fields (chain_id, epoch, timestamps, etc.)
        // 3. Conditionally include optional fields based on self's flags
        // 4. Filter collection fields to only include requested items
        //
        // This allows one network request to serve multiple callers requesting
        // different subsets of chain information.
        if let (
            RequestKey::ChainInfo {
                query: self_query, ..
            },
            RequestKey::ChainInfo {
                query: in_flight_query,
                ..
            },
        ) = (self, in_flight_request)
        {
            // Verify subsumption
            if !in_flight_request.subsumes(self) {
                return None;
            }

            // Extract ChainInfo from result
            let source_info = match result {
                RequestResult::ChainInfo(info) => info,
                _ => return None,
            };

            // Create new ChainInfo with base fields copied from source
            let extracted_info = ChainInfo {
                chain_id: source_info.chain_id,
                epoch: source_info.epoch,
                description: source_info.description.clone(),
                manager: source_info.manager.clone(),
                chain_balance: source_info.chain_balance,
                block_hash: source_info.block_hash,
                timestamp: source_info.timestamp,
                next_block_height: source_info.next_block_height,
                state_hash: source_info.state_hash,
                count_received_log: source_info.count_received_log,
                // Conditional fields: include only if requested
                requested_committees: if self_query.request_committees {
                    source_info.requested_committees.clone()
                } else {
                    None
                },
                requested_owner_balance: {
                    // Only include owner balance if it's for the requested owner
                    // Note: The in-flight query must have requested the same owner (verified by subsumption)
                    source_info.requested_owner_balance
                },
                requested_pending_message_bundles: if self_query.request_pending_message_bundles {
                    source_info.requested_pending_message_bundles.clone()
                } else {
                    vec![]
                },
                // Collection field filtering for certificate hashes
                requested_sent_certificate_hashes: {
                    // Filter certificate hashes based on requested heights
                    // The source hashes correspond to in_flight_query's heights in order
                    if self_query
                        .request_sent_certificate_hashes_by_heights
                        .is_empty()
                    {
                        vec![]
                    } else {
                        // Build index mapping from in_flight heights to source hash indices
                        let mut result_hashes = Vec::new();
                        for requested_height in
                            &self_query.request_sent_certificate_hashes_by_heights
                        {
                            if let Some(index) = in_flight_query
                                .request_sent_certificate_hashes_by_heights
                                .iter()
                                .position(|h| h == requested_height)
                            {
                                if let Some(hash) =
                                    source_info.requested_sent_certificate_hashes.get(index)
                                {
                                    result_hashes.push(*hash);
                                } else {
                                    // Missing hash for requested height - return None
                                    return None;
                                }
                            } else {
                                // Height not in in_flight query (should not happen due to subsumption)
                                return None;
                            }
                        }
                        result_hashes
                    }
                },
                requested_received_log: {
                    // Filter received log based on exclusion count
                    // source_info.requested_received_log already excluded `in_flight_exclude` entries
                    // self_query wants to exclude `self_exclude` entries
                    // Since subsumption ensures in_flight_exclude <= self_exclude,
                    // we need to skip additional (self_exclude - in_flight_exclude) entries
                    match (
                        self_query.request_received_log_excluding_first_n,
                        in_flight_query.request_received_log_excluding_first_n,
                    ) {
                        (Some(self_exclude), Some(in_flight_exclude)) => {
                            let additional_skip = self_exclude.saturating_sub(in_flight_exclude);
                            source_info
                                .requested_received_log
                                .iter()
                                .skip(additional_skip as usize)
                                .cloned()
                                .collect()
                        }
                        (Some(self_exclude), None) => {
                            // in_flight didn't exclude any, so skip self_exclude from source
                            source_info
                                .requested_received_log
                                .iter()
                                .skip(self_exclude as usize)
                                .cloned()
                                .collect()
                        }
                        _ => vec![],
                    }
                },
            };

            return Some(RequestResult::ChainInfo(Box::new(extracted_info)));
        }

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
    use crate::data_types::ChainInfoQuery;

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

    #[test]
    fn test_try_extract_chain_info_base_fields() {
        use linera_base::{
            crypto::{CryptoHash, ValidatorPublicKey},
            data_types::{Amount, Epoch, Timestamp},
        };

        use super::RequestResult;
        use crate::data_types::{ChainInfo, ChainInfoQuery};

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let query = ChainInfoQuery::new(chain_id);
        let validator = ValidatorPublicKey::test_key(0);
        let req = RequestKey::ChainInfo {
            query: Box::new(query.clone()),
            validator,
        };
        let in_flight_req = RequestKey::ChainInfo {
            query: Box::new(query),
            validator,
        };

        // Create a ChainInfo result with specific base field values
        let chain_info = ChainInfo {
            chain_id,
            epoch: Epoch::from(5),
            description: None,
            manager: Box::default(),
            chain_balance: Amount::from_tokens(100),
            block_hash: Some(CryptoHash::test_hash("block")),
            timestamp: Timestamp::from(1234567890),
            next_block_height: BlockHeight(42),
            state_hash: Some(CryptoHash::test_hash("state")),
            requested_owner_balance: None,
            requested_committees: None,
            requested_pending_message_bundles: vec![],
            requested_sent_certificate_hashes: vec![],
            count_received_log: 10,
            requested_received_log: vec![],
        };
        let result = RequestResult::ChainInfo(Box::new(chain_info.clone()));

        // Extract result
        let extracted = req.try_extract_result(&in_flight_req, &result);
        assert!(extracted.is_some());

        // Verify base fields were copied correctly
        match extracted.unwrap() {
            RequestResult::ChainInfo(info) => {
                assert_eq!(info.chain_id, chain_info.chain_id);
                assert_eq!(info.epoch, chain_info.epoch);
                assert_eq!(info.chain_balance, chain_info.chain_balance);
                assert_eq!(info.block_hash, chain_info.block_hash);
                assert_eq!(info.timestamp, chain_info.timestamp);
                assert_eq!(info.next_block_height, chain_info.next_block_height);
                assert_eq!(info.state_hash, chain_info.state_hash);
                assert_eq!(info.count_received_log, chain_info.count_received_log);
            }
            _ => panic!("Expected ChainInfo result"),
        }
    }

    #[test]
    fn test_try_extract_chain_info_conditional_committees() {
        use std::collections::BTreeMap;

        use linera_base::{crypto::ValidatorPublicKey, data_types::Epoch};
        use linera_execution::committee::Committee;

        use super::RequestResult;
        use crate::data_types::{ChainInfo, ChainInfoQuery};

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let validator = ValidatorPublicKey::test_key(0);

        // Query WITHOUT committees
        let query_no_committees = ChainInfoQuery::new(chain_id);
        // Query WITH committees
        let query_with_committees = ChainInfoQuery::new(chain_id).with_committees();

        let req_no_committees = RequestKey::ChainInfo {
            query: Box::new(query_no_committees.clone()),
            validator,
        };
        let req_with_committees = RequestKey::ChainInfo {
            query: Box::new(query_with_committees.clone()),
            validator,
        };
        let in_flight_req = RequestKey::ChainInfo {
            query: Box::new(query_with_committees),
            validator,
        };

        // Create committees data
        let mut committees = BTreeMap::new();
        committees.insert(Epoch::ZERO, Committee::default());

        // Create a ChainInfo result with committees
        let chain_info = ChainInfo {
            chain_id,
            epoch: Epoch::ZERO,
            description: None,
            manager: Box::default(),
            chain_balance: Default::default(),
            block_hash: None,
            timestamp: Default::default(),
            next_block_height: BlockHeight(0),
            state_hash: None,
            requested_owner_balance: None,
            requested_committees: Some(committees.clone()),
            requested_pending_message_bundles: vec![],
            requested_sent_certificate_hashes: vec![],
            count_received_log: 0,
            requested_received_log: vec![],
        };
        let result = RequestResult::ChainInfo(Box::new(chain_info));

        // Extract with committees requested - should include them
        let extracted_with = req_with_committees.try_extract_result(&in_flight_req, &result);
        assert!(extracted_with.is_some());
        match extracted_with.unwrap() {
            RequestResult::ChainInfo(info) => {
                assert!(info.requested_committees.is_some());
                assert_eq!(info.requested_committees.unwrap(), committees);
            }
            _ => panic!("Expected ChainInfo result"),
        }

        // Extract without committees requested - should NOT include them
        let extracted_without = req_no_committees.try_extract_result(&in_flight_req, &result);
        assert!(extracted_without.is_some());
        match extracted_without.unwrap() {
            RequestResult::ChainInfo(info) => {
                assert!(info.requested_committees.is_none());
            }
            _ => panic!("Expected ChainInfo result"),
        }
    }

    #[test]
    fn test_try_extract_chain_info_conditional_owner_balance() {
        use linera_base::{
            crypto::ValidatorPublicKey,
            data_types::{Amount, Epoch},
            identifiers::AccountOwner,
        };

        use super::RequestResult;
        use crate::data_types::{ChainInfo, ChainInfoQuery};

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let owner = AccountOwner::from(CryptoHash::test_hash("owner1"));
        let validator = ValidatorPublicKey::test_key(0);

        let query = ChainInfoQuery::new(chain_id).with_owner_balance(owner);
        let req = RequestKey::ChainInfo {
            query: Box::new(query.clone()),
            validator,
        };
        let in_flight_req = RequestKey::ChainInfo {
            query: Box::new(query),
            validator,
        };

        // Create a ChainInfo result with owner balance
        let chain_info = ChainInfo {
            chain_id,
            epoch: Epoch::ZERO,
            description: None,
            manager: Box::default(),
            chain_balance: Default::default(),
            block_hash: None,
            timestamp: Default::default(),
            next_block_height: BlockHeight(0),
            state_hash: None,
            requested_owner_balance: Some(Amount::from_tokens(50)),
            requested_committees: None,
            requested_pending_message_bundles: vec![],
            requested_sent_certificate_hashes: vec![],
            count_received_log: 0,
            requested_received_log: vec![],
        };
        let result = RequestResult::ChainInfo(Box::new(chain_info.clone()));

        // Extract - should include owner balance
        let extracted = req.try_extract_result(&in_flight_req, &result);
        assert!(extracted.is_some());
        match extracted.unwrap() {
            RequestResult::ChainInfo(info) => {
                assert_eq!(info.requested_owner_balance, Some(Amount::from_tokens(50)));
            }
            _ => panic!("Expected ChainInfo result"),
        }
    }

    #[test]
    fn test_try_extract_chain_info_conditional_message_bundles() {
        use linera_base::{crypto::ValidatorPublicKey, data_types::Epoch};

        use super::RequestResult;
        use crate::data_types::{ChainInfo, ChainInfoQuery};

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let validator = ValidatorPublicKey::test_key(0);

        // Query WITHOUT message bundles
        let query_no_bundles = ChainInfoQuery::new(chain_id);
        // Query WITH message bundles
        let query_with_bundles = ChainInfoQuery::new(chain_id).with_pending_message_bundles();

        let req_no_bundles = RequestKey::ChainInfo {
            query: Box::new(query_no_bundles.clone()),
            validator,
        };
        let req_with_bundles = RequestKey::ChainInfo {
            query: Box::new(query_with_bundles.clone()),
            validator,
        };
        let in_flight_req = RequestKey::ChainInfo {
            query: Box::new(query_with_bundles),
            validator,
        };

        // Create a ChainInfo result with message bundles (empty vec for simplicity)
        let chain_info = ChainInfo {
            chain_id,
            epoch: Epoch::ZERO,
            description: None,
            manager: Box::default(),
            chain_balance: Default::default(),
            block_hash: None,
            timestamp: Default::default(),
            next_block_height: BlockHeight(0),
            state_hash: None,
            requested_owner_balance: None,
            requested_committees: None,
            requested_pending_message_bundles: vec![],
            requested_sent_certificate_hashes: vec![],
            count_received_log: 0,
            requested_received_log: vec![],
        };
        let result = RequestResult::ChainInfo(Box::new(chain_info));

        // Extract with bundles requested - should include them (even if empty)
        let extracted_with = req_with_bundles.try_extract_result(&in_flight_req, &result);
        assert!(extracted_with.is_some());
        match extracted_with.unwrap() {
            RequestResult::ChainInfo(info) => {
                // Should have the vec, even if empty
                assert_eq!(info.requested_pending_message_bundles.len(), 0);
            }
            _ => panic!("Expected ChainInfo result"),
        }

        // Extract without bundles requested - should have empty vec
        let extracted_without = req_no_bundles.try_extract_result(&in_flight_req, &result);
        assert!(extracted_without.is_some());
        match extracted_without.unwrap() {
            RequestResult::ChainInfo(info) => {
                assert_eq!(info.requested_pending_message_bundles.len(), 0);
            }
            _ => panic!("Expected ChainInfo result"),
        }
    }

    #[test]
    fn test_try_extract_chain_info_filter_certificate_hashes() {
        use linera_base::{
            crypto::{CryptoHash, ValidatorPublicKey},
            data_types::Epoch,
        };

        use super::RequestResult;
        use crate::data_types::{ChainInfo, ChainInfoQuery};

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let validator = ValidatorPublicKey::test_key(0);

        // In-flight query requests heights [10, 11, 12, 13]
        let in_flight_query = ChainInfoQuery::new(chain_id)
            .with_sent_certificate_hashes_by_heights(vec![
                BlockHeight(10),
                BlockHeight(11),
                BlockHeight(12),
                BlockHeight(13),
            ]);

        // Self query requests subset [11, 13]
        let self_query = ChainInfoQuery::new(chain_id)
            .with_sent_certificate_hashes_by_heights(vec![BlockHeight(11), BlockHeight(13)]);

        let req = RequestKey::ChainInfo {
            query: Box::new(self_query),
            validator,
        };
        let in_flight_req = RequestKey::ChainInfo {
            query: Box::new(in_flight_query),
            validator,
        };

        // Create hashes for heights [10, 11, 12, 13]
        let hash_10 = CryptoHash::test_hash("hash10");
        let hash_11 = CryptoHash::test_hash("hash11");
        let hash_12 = CryptoHash::test_hash("hash12");
        let hash_13 = CryptoHash::test_hash("hash13");

        let chain_info = ChainInfo {
            chain_id,
            epoch: Epoch::ZERO,
            description: None,
            manager: Box::default(),
            chain_balance: Default::default(),
            block_hash: None,
            timestamp: Default::default(),
            next_block_height: BlockHeight(0),
            state_hash: None,
            requested_owner_balance: None,
            requested_committees: None,
            requested_pending_message_bundles: vec![],
            requested_sent_certificate_hashes: vec![hash_10, hash_11, hash_12, hash_13],
            count_received_log: 0,
            requested_received_log: vec![],
        };
        let result = RequestResult::ChainInfo(Box::new(chain_info));

        // Extract - should only get hashes for [11, 13]
        let extracted = req.try_extract_result(&in_flight_req, &result);
        assert!(extracted.is_some());
        match extracted.unwrap() {
            RequestResult::ChainInfo(info) => {
                assert_eq!(info.requested_sent_certificate_hashes.len(), 2);
                assert_eq!(info.requested_sent_certificate_hashes[0], hash_11);
                assert_eq!(info.requested_sent_certificate_hashes[1], hash_13);
            }
            _ => panic!("Expected ChainInfo result"),
        }
    }

    #[test]
    fn test_try_extract_chain_info_filter_received_log() {
        use linera_base::{crypto::ValidatorPublicKey, data_types::Epoch};
        use linera_chain::data_types::ChainAndHeight;

        use super::RequestResult;
        use crate::data_types::{ChainInfo, ChainInfoQuery};

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let validator = ValidatorPublicKey::test_key(0);

        // In-flight query excludes first 2 entries
        let in_flight_query = ChainInfoQuery::new(chain_id).with_received_log_excluding_first_n(2);

        // Self query wants to exclude first 5 entries (so skip 3 more from in-flight result)
        let self_query = ChainInfoQuery::new(chain_id).with_received_log_excluding_first_n(5);

        let req = RequestKey::ChainInfo {
            query: Box::new(self_query),
            validator,
        };
        let in_flight_req = RequestKey::ChainInfo {
            query: Box::new(in_flight_query),
            validator,
        };

        // Create received log - in_flight already excluded first 2, so this starts at index 2
        let log_entries = vec![
            ChainAndHeight {
                chain_id: ChainId(CryptoHash::test_hash("chain2")),
                height: BlockHeight(2),
            },
            ChainAndHeight {
                chain_id: ChainId(CryptoHash::test_hash("chain3")),
                height: BlockHeight(3),
            },
            ChainAndHeight {
                chain_id: ChainId(CryptoHash::test_hash("chain4")),
                height: BlockHeight(4),
            },
            ChainAndHeight {
                chain_id: ChainId(CryptoHash::test_hash("chain5")),
                height: BlockHeight(5),
            },
        ];

        let chain_info = ChainInfo {
            chain_id,
            epoch: Epoch::ZERO,
            description: None,
            manager: Box::default(),
            chain_balance: Default::default(),
            block_hash: None,
            timestamp: Default::default(),
            next_block_height: BlockHeight(0),
            state_hash: None,
            requested_owner_balance: None,
            requested_committees: None,
            requested_pending_message_bundles: vec![],
            requested_sent_certificate_hashes: vec![],
            count_received_log: 6,
            requested_received_log: log_entries.clone(),
        };
        let result = RequestResult::ChainInfo(Box::new(chain_info));

        // Extract - should skip first 3 from in-flight result (indices 0,1,2), leaving index 3 onwards
        let extracted = req.try_extract_result(&in_flight_req, &result);
        assert!(extracted.is_some());
        match extracted.unwrap() {
            RequestResult::ChainInfo(info) => {
                assert_eq!(info.requested_received_log.len(), 1);
                assert_eq!(info.requested_received_log[0].height, BlockHeight(5));
            }
            _ => panic!("Expected ChainInfo result"),
        }
    }

    #[test]
    fn test_try_extract_chain_info_full_extraction() {
        use linera_base::{
            crypto::{CryptoHash, ValidatorPublicKey},
            data_types::Epoch,
        };

        use super::RequestResult;
        use crate::data_types::{ChainInfo, ChainInfoQuery};

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let validator = ValidatorPublicKey::test_key(0);

        // Complex query with multiple fields
        let query = ChainInfoQuery::new(chain_id)
            .with_committees()
            .with_sent_certificate_hashes_by_heights(vec![BlockHeight(10)])
            .with_received_log_excluding_first_n(0);

        let req = RequestKey::ChainInfo {
            query: Box::new(query.clone()),
            validator,
        };
        let in_flight_req = RequestKey::ChainInfo {
            query: Box::new(query.clone()),
            validator,
        };

        let hash_10 = CryptoHash::test_hash("hash10");

        let chain_info = ChainInfo {
            chain_id,
            epoch: Epoch::ZERO,
            description: None,
            manager: Box::default(),
            chain_balance: Default::default(),
            block_hash: None,
            timestamp: Default::default(),
            next_block_height: BlockHeight(0),
            state_hash: None,
            requested_owner_balance: None,
            requested_committees: Some(Default::default()),
            requested_pending_message_bundles: vec![],
            requested_sent_certificate_hashes: vec![hash_10],
            count_received_log: 0,
            requested_received_log: vec![],
        };
        let result = RequestResult::ChainInfo(Box::new(chain_info.clone()));

        // Extract - should include all requested fields
        let extracted = req.try_extract_result(&in_flight_req, &result);
        assert!(extracted.is_some());
        match extracted.unwrap() {
            RequestResult::ChainInfo(info) => {
                assert!(info.requested_committees.is_some());
                assert_eq!(info.requested_sent_certificate_hashes.len(), 1);
                assert_eq!(info.requested_sent_certificate_hashes[0], hash_10);
                assert_eq!(info.requested_received_log.len(), 0);
            }
            _ => panic!("Expected ChainInfo result"),
        }
    }

    #[test]
    fn test_try_extract_chain_info_empty_request() {
        use linera_base::{crypto::ValidatorPublicKey, data_types::Epoch};

        use super::RequestResult;
        use crate::data_types::{ChainInfo, ChainInfoQuery};

        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let validator = ValidatorPublicKey::test_key(0);

        // Minimal request
        let query = ChainInfoQuery::new(chain_id);
        let req = RequestKey::ChainInfo {
            query: Box::new(query.clone()),
            validator,
        };
        let in_flight_req = RequestKey::ChainInfo {
            query: Box::new(query.clone()),
            validator,
        };

        let chain_info = ChainInfo {
            chain_id,
            epoch: Epoch::ZERO,
            description: None,
            manager: Box::default(),
            chain_balance: Default::default(),
            block_hash: None,
            timestamp: Default::default(),
            next_block_height: BlockHeight(0),
            state_hash: None,
            requested_owner_balance: None,
            requested_committees: None,
            requested_pending_message_bundles: vec![],
            requested_sent_certificate_hashes: vec![],
            count_received_log: 0,
            requested_received_log: vec![],
        };
        let result = RequestResult::ChainInfo(Box::new(chain_info));

        // Extract - should have minimal data
        let extracted = req.try_extract_result(&in_flight_req, &result);
        assert!(extracted.is_some());
        match extracted.unwrap() {
            RequestResult::ChainInfo(info) => {
                assert!(info.requested_committees.is_none());
                assert_eq!(info.requested_sent_certificate_hashes.len(), 0);
                assert_eq!(info.requested_received_log.len(), 0);
            }
            _ => panic!("Expected ChainInfo result"),
        }
    }

    // Property-based tests for ChainInfo subsumption
    // These tests use Arbitrary to generate random ChainInfoQuery instances and verify
    // that subsumption properties hold across the entire input space.

    #[cfg(all(test, not(target_arch = "wasm32")))]
    mod property_tests {
        use proptest::prelude::*;
        use rand::seq::SliceRandom;

        use super::*;

        /// Returns a random subset of the input slice
        fn random_subset<T: Clone>(items: &[T], rng: &mut impl rand::Rng) -> Vec<T> {
            if items.is_empty() {
                return vec![];
            }
            let subset_size = rng.gen_range(0..=items.len());
            items.choose_multiple(rng, subset_size).cloned().collect()
        }

        /// Derives a subset query from a base query.
        /// The resulting query should be subsumed by the base query.
        fn derive_subset_query(base: &ChainInfoQuery, rng: &mut impl rand::Rng) -> ChainInfoQuery {
            let mut subset = ChainInfoQuery::new(base.chain_id);

            // Exact-match fields must be copied exactly
            subset.request_owner_balance = base.request_owner_balance;
            subset.test_next_block_height = base.test_next_block_height;
            subset.request_leader_timeout = base.request_leader_timeout;

            // Boolean flags: if base has true, subset can be true or false
            // if base has false, subset must be false
            subset.request_committees = base.request_committees && rng.gen_bool(0.5);
            subset.request_pending_message_bundles =
                base.request_pending_message_bundles && rng.gen_bool(0.5);
            subset.request_manager_values = base.request_manager_values && rng.gen_bool(0.5);
            subset.request_fallback = base.request_fallback && rng.gen_bool(0.5);
            subset.create_network_actions = base.create_network_actions && rng.gen_bool(0.5);

            // Collection fields: pick a random subset of heights
            subset.request_sent_certificate_hashes_by_heights =
                random_subset(&base.request_sent_certificate_hashes_by_heights, rng);

            // Range field: if base excludes n entries, subset can exclude >= n entries
            subset.request_received_log_excluding_first_n =
                base.request_received_log_excluding_first_n.map(|n| {
                    let additional = rng.gen_range(0..=10);
                    n.saturating_add(additional)
                });

            subset
        }

        /// Derives a superset query that the base query cannot subsume.
        /// The resulting query has additional requirements not in the base query.
        fn derive_superset_query(
            base: &ChainInfoQuery,
            rng: &mut impl rand::Rng,
        ) -> ChainInfoQuery {
            let mut superset = base.clone();

            // Choose one way to add requirements
            match rng.gen_range(0..4) {
                0 => {
                    // Add a boolean flag requirement
                    if !base.request_committees {
                        superset.request_committees = true;
                    } else if !base.request_pending_message_bundles {
                        superset.request_pending_message_bundles = true;
                    } else if !base.request_manager_values {
                        superset.request_manager_values = true;
                    } else if !base.request_fallback {
                        superset.request_fallback = true;
                    }
                }
                1 => {
                    // Add a height not in base
                    let new_height = BlockHeight(rng.gen_range(1000..2000));
                    if !base
                        .request_sent_certificate_hashes_by_heights
                        .contains(&new_height)
                    {
                        superset
                            .request_sent_certificate_hashes_by_heights
                            .push(new_height);
                    }
                }
                2 => {
                    // Request more log entries
                    match base.request_received_log_excluding_first_n {
                        Some(n) if n > 0 => {
                            superset.request_received_log_excluding_first_n =
                                Some(n.saturating_sub(1))
                        }
                        None => superset.request_received_log_excluding_first_n = Some(0),
                        _ => {}
                    }
                }
                _ => {
                    // Change exact-match field to make queries incompatible
                    superset.test_next_block_height = Some(BlockHeight(rng.gen_range(1..1000)));
                }
            }

            superset
        }

        proptest! {
            /// Property: Every query subsumes itself (reflexivity)
            #[test]
            fn prop_subsumption_reflexivity(query in any::<ChainInfoQuery>()) {
                use linera_base::crypto::ValidatorPublicKey;
                let validator = ValidatorPublicKey::test_key(0);
                let key = RequestKey::ChainInfo { query: Box::new(query), validator };
                prop_assert!(key.subsumes(&key));
            }

            /// Property: Subsumption is transitive
            /// If A subsumes B and B subsumes C, then A subsumes C
            #[test]
            fn prop_subsumption_transitivity(query_a in any::<ChainInfoQuery>()) {
                use linera_base::crypto::ValidatorPublicKey;
                let validator = ValidatorPublicKey::test_key(0);
                let mut rng = rand::thread_rng();
                let query_b = derive_subset_query(&query_a, &mut rng);
                let query_c = derive_subset_query(&query_b, &mut rng);

                let key_a = RequestKey::ChainInfo { query: Box::new(query_a.clone()), validator };
                let key_b = RequestKey::ChainInfo { query: Box::new(query_b.clone()), validator };
                let key_c = RequestKey::ChainInfo { query: Box::new(query_c.clone()), validator };

                prop_assert!(key_a.subsumes(&key_b), "A should subsume B");
                prop_assert!(key_b.subsumes(&key_c), "B should subsume C");
                prop_assert!(key_a.subsumes(&key_c), "A should subsume C (transitivity)");
            }

            /// Property: A query subsumes all its proper subsets
            #[test]
            fn prop_subsumption_subset(query_a in any::<ChainInfoQuery>()) {
                use linera_base::crypto::ValidatorPublicKey;
                let validator = ValidatorPublicKey::test_key(0);
                let mut rng = rand::thread_rng();
                let query_b = derive_subset_query(&query_a, &mut rng);

                let key_a = RequestKey::ChainInfo { query: Box::new(query_a.clone()), validator };
                let key_b = RequestKey::ChainInfo { query: Box::new(query_b.clone()), validator };

                prop_assert!(key_a.subsumes(&key_b), "Comprehensive query should subsume subset query");
            }

            /// Property: A query does not subsume queries with additional requirements
            #[test]
            fn prop_subsumption_non_subsumption(query_a in any::<ChainInfoQuery>()) {
                use linera_base::crypto::ValidatorPublicKey;
                let validator = ValidatorPublicKey::test_key(0);
                let mut rng = rand::thread_rng();
                let query_b = derive_superset_query(&query_a, &mut rng);

                let key_a = RequestKey::ChainInfo { query: Box::new(query_a.clone()), validator };
                let key_b = RequestKey::ChainInfo { query: Box::new(query_b.clone()), validator };

                // Only assert non-subsumption if we actually added a requirement
                // (superset derivation might not change anything in edge cases)
                if query_a != query_b {
                    prop_assert!(!key_a.subsumes(&key_b), "Query should not subsume superset with additional requirements");
                }
            }
        }
    }
}
