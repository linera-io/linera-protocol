// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Pure routing helpers for the per-burn fallback path.
//!
//! `split_to_fit` decides how to chunk a list of pending-burn positions
//! within a single Linera-block transaction given an async fits-predicate.
//! The predicate carries all IO (real `eth_estimateGas` against an EVM
//! node in production, a synthetic closure in tests). No code in this
//! module touches the network or the file system.

use linera_chain::types::ConfirmedBlockCertificate;

/// One pending burn whose single-position `processBurns(cert, tx_index, [pos])`
/// chunk also fails the predicate — recovery is impossible at this
/// layer (Phase 2 inclusion proofs are the next rung).
#[derive(Debug, thiserror::Error)]
#[error("burn at tx {tx_index} position {pos_in_tx} does not fit")]
pub struct SingleBurnTooLarge {
    pub tx_index: u32,
    pub pos_in_tx: u32,
}

/// Splits `positions_in_tx` into the minimum number of chunks such that
/// every chunk satisfies `fits_fn(cert, tx_index, &chunk).await`.
/// Operates strictly within one tx — each `processBurns` call targets a
/// single transaction. Callers group pending burns by tx first and
/// invoke this function per group. Returns chunks in input order.
/// Iterative LIFO traversal — no recursion.
pub async fn split_to_fit<F>(
    cert: &ConfirmedBlockCertificate,
    tx_index: u32,
    positions_in_tx: &[u32],
    fits_fn: F,
) -> Result<Vec<Vec<u32>>, SingleBurnTooLarge>
where
    F: AsyncFn(&ConfirmedBlockCertificate, u32, &[u32]) -> bool,
{
    if positions_in_tx.is_empty() {
        return Ok(Vec::new());
    }
    let mut chunks: Vec<Vec<u32>> = Vec::new();
    let mut stack: Vec<&[u32]> = vec![positions_in_tx];
    while let Some(slice) = stack.pop() {
        if fits_fn(cert, tx_index, slice).await {
            chunks.push(slice.to_vec());
            continue;
        }
        if slice.len() == 1 {
            return Err(SingleBurnTooLarge {
                tx_index,
                pos_in_tx: slice[0],
            });
        }
        let mid = slice.len() / 2;
        let (left, right) = slice.split_at(mid);
        stack.push(right);
        stack.push(left);
    }
    Ok(chunks)
}

/// Turns a raw `eth_estimateGas` result into a fits/doesn't-fit decision.
/// `Ok(_)` means the node already accepted the estimate. A gas-exceeded
/// RPC error means the call wouldn't fit. Other errors bubble up.
pub fn estimate_fits(r: Result<u64, alloy::contract::Error>) -> anyhow::Result<bool> {
    use crate::relay::evm::is_gas_exceeded_error;
    match r {
        Ok(_) => Ok(true),
        Err(e) if is_gas_exceeded_error(&e) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use linera_base::crypto::ValidatorSecretKey;

    use super::*;
    use crate::test_helpers::create_signed_certificate;

    fn dummy_cert() -> ConfirmedBlockCertificate {
        let secret = ValidatorSecretKey::generate();
        let public = secret.public();
        create_signed_certificate(&secret, &public)
    }

    const TX: u32 = 7;

    #[tokio::test]
    async fn split_to_fit_returns_one_chunk_when_everything_fits() {
        let cert = dummy_cert();
        let positions = vec![0u32, 1, 2, 3, 4];
        let chunks = split_to_fit(&cert, TX, &positions, async |_, _, _| true)
            .await
            .unwrap();
        assert_eq!(chunks, vec![positions]);
    }

    #[tokio::test]
    async fn split_to_fit_chunks_until_predicate_accepts() {
        let cert = dummy_cert();
        let positions = vec![0u32, 1, 2, 3, 4];
        let chunks = split_to_fit(&cert, TX, &positions, async |_, _, c: &[u32]| c.len() <= 2)
            .await
            .unwrap();
        // Halving order: [0,1,2,3,4] → [0,1] + [2,3,4]; LIFO visits [0,1]
        // first (accepted whole), then [2,3,4] → [2] + [3,4] (both
        // accepted). Input-order chunks: [[0, 1], [2], [3, 4]].
        assert_eq!(chunks, vec![vec![0, 1], vec![2], vec![3, 4]]);
    }

    #[tokio::test]
    async fn split_to_fit_per_burn_when_only_singletons_fit() {
        let cert = dummy_cert();
        let positions = vec![10u32, 20, 30];
        let chunks = split_to_fit(&cert, TX, &positions, async |_, _, c: &[u32]| c.len() == 1)
            .await
            .unwrap();
        assert_eq!(chunks, vec![vec![10], vec![20], vec![30]]);
    }

    #[tokio::test]
    async fn split_to_fit_errors_when_single_burn_does_not_fit() {
        let cert = dummy_cert();
        let positions = vec![42u32];
        let err = split_to_fit(&cert, TX, &positions, async |_, _, _| false)
            .await
            .unwrap_err();
        assert_eq!(err.tx_index, TX);
        assert_eq!(err.pos_in_tx, 42);
    }

    #[tokio::test]
    async fn split_to_fit_empty_input_returns_empty_output() {
        let cert = dummy_cert();
        let chunks = split_to_fit(&cert, TX, &[], async |_, _, _| true)
            .await
            .unwrap();
        assert!(chunks.is_empty());
    }

    #[tokio::test]
    async fn split_to_fit_predicate_only_ever_called_with_one_tx_index() {
        use std::cell::RefCell;
        let cert = dummy_cert();
        let positions = vec![0u32, 1, 2, 3];
        let seen = RefCell::new(Vec::<u32>::new());
        let _chunks: Vec<Vec<u32>> = split_to_fit(&cert, TX, &positions, async |_, tx, _| {
            seen.borrow_mut().push(tx);
            true
        })
        .await
        .unwrap();
        let seen = seen.into_inner();
        assert!(!seen.is_empty());
        for tx in seen {
            assert_eq!(tx, TX, "split_to_fit must never cross tx boundaries");
        }
    }

    #[test]
    fn estimate_fits_ok_returns_true() {
        assert!(estimate_fits(Ok(123_456)).unwrap());
        assert!(estimate_fits(Ok(0)).unwrap());
    }

    #[test]
    fn estimate_fits_gas_exceeded_error_returns_false() {
        use alloy::transports::TransportErrorKind;
        let transport_err =
            TransportErrorKind::custom_str("gas required exceeds allowance (30000000)");
        let contract_err = alloy::contract::Error::TransportError(transport_err);
        assert!(!estimate_fits(Err(contract_err)).unwrap());
    }

    #[test]
    fn estimate_fits_other_error_bubbles_up() {
        use alloy::transports::TransportErrorKind;
        let transport_err = TransportErrorKind::custom_str("nonce too low");
        let contract_err = alloy::contract::Error::TransportError(transport_err);
        assert!(estimate_fits(Err(contract_err)).is_err());
    }
}
