// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    data_types::{Blob, BlobContent, BlockHeight},
    identifiers::{BlobId, ChainId},
};
use linera_chain::types::ConfirmedBlockCertificate;

use crate::client::validator_manager::cache::SubsumingKey;

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
    fn height_range(&self) -> Option<Vec<BlockHeight>> {
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
}

/// Result types that can be shared across deduplicated requests
#[derive(Debug, Clone)]
pub enum RequestResult {
    Certificates(Vec<ConfirmedBlockCertificate>),
    Blob(Option<Blob>),
    BlobContent(BlobContent),
    Certificate(Box<ConfirmedBlockCertificate>),
}

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

impl From<RequestResult> for Option<Blob> {
    fn from(result: RequestResult) -> Self {
        match result {
            RequestResult::Blob(blob) => blob,
            _ => panic!("Cannot convert RequestResult to Option<Blob>"),
        }
    }
}

impl From<RequestResult> for Vec<ConfirmedBlockCertificate> {
    fn from(result: RequestResult) -> Self {
        match result {
            RequestResult::Certificates(certs) => certs,
            _ => panic!("Cannot convert RequestResult to Vec<ConfirmedBlockCertificate>"),
        }
    }
}

impl From<RequestResult> for BlobContent {
    fn from(result: RequestResult) -> Self {
        match result {
            RequestResult::BlobContent(content) => content,
            _ => panic!("Cannot convert RequestResult to BlobContent"),
        }
    }
}

impl From<RequestResult> for ConfirmedBlockCertificate {
    fn from(result: RequestResult) -> Self {
        match result {
            RequestResult::Certificate(cert) => *cert,
            _ => panic!("Cannot convert RequestResult to ConfirmedBlockCertificate"),
        }
    }
}

impl SubsumingKey<RequestResult> for super::request::RequestKey {
    fn subsumes(&self, other: &Self) -> bool {
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

        if heights1.is_empty() {
            return false; // An empty set cannot subsume any non-empty set
        }

        if heights2.is_empty() {
            return true; // An empty set is always subsumed
        }

        heights1.first().expect("heights1 is not empty")
            <= heights2.first().expect("heights2 is not empty")
            && heights1.last().expect("heights1 is not empty")
                >= heights2.last().expect("heights2 is not empty")
    }

    fn try_extract_result(
        &self,
        other: &RequestKey,
        result: &RequestResult,
    ) -> Option<RequestResult> {
        // Only certificate results can be extracted
        let certificates = match result {
            RequestResult::Certificates(certs) => certs,
            _ => return None,
        };

        if !self.subsumes(other) {
            return None; // Can't extract if not subsumed
        }

        let requested_heights = self.height_range()?;
        if requested_heights.is_empty() {
            return Some(RequestResult::Certificates(vec![])); // Nothing requested
        }
        let requested_start = requested_heights.first().unwrap();
        let requested_end = requested_heights.last().unwrap();

        // Filter certificates to only those at the requested heights
        let filtered: Vec<_> = certificates
            .iter()
            .filter(|cert| {
                &cert.value().height() >= requested_start && &cert.value().height() <= requested_end
            })
            .cloned()
            .collect();

        if filtered.is_empty() {
            return None; // No matching certificates found
        }
        if filtered.first().unwrap().value().height() != *requested_start
            || filtered.last().unwrap().value().height() != *requested_end
        {
            return None; // Missing some requested heights
        }

        Some(RequestResult::Certificates(filtered))
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
