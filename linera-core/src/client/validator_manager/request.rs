// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    data_types::{Blob, BlobContent, BlockHeight},
    identifiers::{BlobId, ChainId},
};
use linera_chain::types::ConfirmedBlockCertificate;

use crate::node::NodeError;

/// Unique identifier for different types of download requests.
///
/// Used for request deduplication to avoid redundant downloads of the same data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RequestKey {
    /// Download certificates from a starting height
    Certificates {
        chain_id: ChainId,
        start: BlockHeight,
        limit: u64,
    },
    /// Download certificates by specific heights
    CertificatesByHeights {
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
            RequestKey::CertificatesByHeights { chain_id, .. } => Some(*chain_id),
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
    pub(super) fn height_range(&self) -> Option<Vec<BlockHeight>> {
        match self {
            RequestKey::Certificates { start, limit, .. } => {
                let heights: Vec<BlockHeight> = (0..*limit)
                    .map(|offset| BlockHeight(start.0 + offset))
                    .collect();
                Some(heights)
            }
            RequestKey::CertificatesByHeights { heights, .. } => Some(heights.clone()),
            _ => None,
        }
    }

    /// Checks if this request fully subsumes another request.
    ///
    /// Request A subsumes request B if A's result would contain all the data that
    /// B's result would contain. This means B's request is redundant if A is already
    /// in-flight or cached.
    ///
    /// # Examples
    /// ```ignore
    /// let large = RequestKey::Certificates { chain_id, start: 10, limit: 10 }; // [10..20]
    /// let small = RequestKey::CertificatesByHeights { chain_id, heights: vec![12,13,14] };
    /// assert!(large.subsumes(&small)); // large contains all of small's heights
    /// ```
    pub fn subsumes(&self, other: &RequestKey) -> bool {
        // Different chains can't subsume each other
        if self.chain_id() != other.chain_id() {
            return false;
        }

        let heights1 = match self.height_range() {
            Some(range) => range,
            None => return self == other, // Non-certificate requests must match exactly
        };
        let heights2 = match other.height_range() {
            Some(range) => range,
            None => return false, // Can't subsume different variant types
        };

        // Check if all heights in other are contained in self
        heights2.iter().all(|h| heights1.contains(h))
    }

    /// Attempts to extract a subset result for this request from a larger request's result.
    ///
    /// This is used when a request A subsumes this request B. We can extract B's result
    /// from A's result by filtering the certificates to only those requested by B.
    ///
    /// # Arguments
    /// - `from`: The key of the larger request that subsumes this one
    /// - `result`: The result from the larger request
    ///
    /// # Returns
    /// - `Some(RequestResult)` with the extracted subset if possible
    /// - `None` if extraction is not possible (wrong variant, different chain, etc.)
    pub fn can_extract_result(
        &self,
        from: &RequestKey,
        result: &RequestResult,
    ) -> Option<RequestResult> {
        // Only certificate results can be extracted
        let certificates = match result {
            RequestResult::Certificates(Ok(certs)) => certs,
            _ => return None,
        };

        if self.chain_id().is_none() || from.chain_id().is_none() {
            return None;
        }

        let heights_self = self.height_range()?;

        // Filter certificates to only those at the requested heights
        let filtered: Vec<_> = certificates
            .iter()
            .filter(|cert| heights_self.contains(&cert.value().height()))
            .cloned()
            .collect();

        Some(RequestResult::Certificates(Ok(filtered)))
    }
}

/// Result types that can be shared across deduplicated requests
#[derive(Debug, Clone)]
pub enum RequestResult {
    Certificates(Result<Vec<ConfirmedBlockCertificate>, NodeError>),
    Blob(Result<Option<Blob>, NodeError>),
    BlobContent(Result<BlobContent, NodeError>),
    Certificate(Box<Result<ConfirmedBlockCertificate, NodeError>>),
}

impl RequestResult {
    /// Returns true if the result represents a successful operation
    pub(super) fn is_ok(&self) -> bool {
        match self {
            RequestResult::Certificates(result) => result.is_ok(),
            RequestResult::Blob(result) => result.is_ok(),
            RequestResult::BlobContent(result) => result.is_ok(),
            RequestResult::Certificate(result) => result.is_ok(),
        }
    }
}

impl From<RequestResult> for Result<Vec<ConfirmedBlockCertificate>, NodeError> {
    fn from(result: RequestResult) -> Self {
        match result {
            RequestResult::Certificates(r) => r,
            _ => panic!("Invalid RequestResult variant"),
        }
    }
}

impl From<Result<Vec<ConfirmedBlockCertificate>, NodeError>> for RequestResult {
    fn from(result: Result<Vec<ConfirmedBlockCertificate>, NodeError>) -> Self {
        RequestResult::Certificates(result)
    }
}

impl From<RequestResult> for Result<Option<Blob>, NodeError> {
    fn from(result: RequestResult) -> Self {
        match result {
            RequestResult::Blob(r) => r,
            _ => panic!("Invalid RequestResult variant"),
        }
    }
}

impl From<Result<Option<Blob>, NodeError>> for RequestResult {
    fn from(result: Result<Option<Blob>, NodeError>) -> Self {
        RequestResult::Blob(result)
    }
}

impl From<RequestResult> for Result<BlobContent, NodeError> {
    fn from(result: RequestResult) -> Self {
        match result {
            RequestResult::BlobContent(r) => r,
            _ => panic!("Invalid RequestResult variant"),
        }
    }
}

impl From<Result<BlobContent, NodeError>> for RequestResult {
    fn from(result: Result<BlobContent, NodeError>) -> Self {
        RequestResult::BlobContent(result)
    }
}

impl From<RequestResult> for Result<ConfirmedBlockCertificate, NodeError> {
    fn from(result: RequestResult) -> Self {
        match result {
            RequestResult::Certificate(r) => *r,
            _ => panic!("Invalid RequestResult variant"),
        }
    }
}

impl From<Result<ConfirmedBlockCertificate, NodeError>> for RequestResult {
    fn from(result: Result<ConfirmedBlockCertificate, NodeError>) -> Self {
        RequestResult::Certificate(Box::new(result))
    }
}

#[cfg(test)]
mod tests {
    use linera_base::{crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId};

    use super::RequestKey;

    #[test]
    fn test_subsumes_complete_containment() {
        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let large = RequestKey::Certificates {
            chain_id,
            start: BlockHeight(10),
            limit: 10,
        }; // [10..20]
        let small = RequestKey::CertificatesByHeights {
            chain_id,
            heights: vec![BlockHeight(12), BlockHeight(13), BlockHeight(14)],
        };
        assert!(large.subsumes(&small));
        assert!(!small.subsumes(&large));
    }

    #[test]
    fn test_subsumes_partial_containment() {
        let chain_id = ChainId(CryptoHash::test_hash("chain1"));
        let req1 = RequestKey::Certificates {
            chain_id,
            start: BlockHeight(10),
            limit: 5,
        }; // [10,11,12,13,14]
        let req2 = RequestKey::CertificatesByHeights {
            chain_id,
            heights: vec![BlockHeight(12), BlockHeight(15)], // 15 is outside range
        };
        assert!(!req1.subsumes(&req2)); // Can't subsume, 15 is not in req1
    }

    #[test]
    fn test_subsumes_different_chains() {
        let chain1 = ChainId(CryptoHash::test_hash("chain1"));
        let chain2 = ChainId(CryptoHash::test_hash("chain2"));
        let req1 = RequestKey::Certificates {
            chain_id: chain1,
            start: BlockHeight(10),
            limit: 10,
        };
        let req2 = RequestKey::CertificatesByHeights {
            chain_id: chain2,
            heights: vec![BlockHeight(12)],
        };
        assert!(!req1.subsumes(&req2));
    }
}
