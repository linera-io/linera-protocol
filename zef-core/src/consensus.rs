// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{base_types::*, ensure, error::Error, messages::*};
use std::collections::{BTreeMap, BTreeSet};

/// State of a one-shot consensus instance.
#[derive(Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ConsensusState {
    /// Accounts expected to be locked and managed by the protocol.
    pub accounts: Vec<AccountId>,
    /// Expected sequence number for each locked account.
    pub sequence_numbers: BTreeMap<AccountId, SequenceNumber>,
    /// Accounts locked so far and for which controlling key.
    pub locked_accounts: BTreeMap<AccountId, AccountOwner>,
    /// Authorized participants.
    pub participants: BTreeSet<AccountOwner>,
    /// Pending proposal.
    pub proposed: Option<ConsensusProposal>,
    /// Locked proposal and its "pre-commit" certificate.
    pub locked: Option<Certificate>,
    /// The certificate that created this instance.
    pub received: Certificate,
}

impl ConsensusState {
    pub fn new(expected: Vec<(AccountId, SequenceNumber)>, received: Certificate) -> Self {
        let accounts: Vec<_> = expected.iter().map(|(id, _)| id.clone()).collect();
        let sequence_numbers: BTreeMap<_, _> = expected.into_iter().collect();
        assert_eq!(accounts.len(), sequence_numbers.len());
        Self {
            accounts,
            sequence_numbers,
            locked_accounts: BTreeMap::new(),
            participants: BTreeSet::new(),
            proposed: None,
            locked: None,
            received,
        }
    }

    pub(crate) fn make_requests(&self, decision: ConsensusDecision) -> Result<Vec<Request>, Error> {
        match decision {
            ConsensusDecision::Abort => Ok(Vec::new()),
            ConsensusDecision::Confirm => {
                let num_accounts = self.accounts.len();
                for id in &self.accounts {
                    ensure!(
                        self.locked_accounts.contains_key(id),
                        Error::MissingConsensusLock {
                            account_id: id.clone()
                        }
                    );
                }
                let mut requests = Vec::new();
                for (i, id) in self.accounts.iter().enumerate() {
                    let sequence_number = *self.sequence_numbers.get(id).unwrap();
                    let next_id = &self.accounts[(i + 1) % num_accounts];
                    let new_owner = *self.locked_accounts.get(next_id).unwrap();
                    requests.push(Request {
                        account_id: id.clone(),
                        operation: Operation::ChangeOwner { new_owner },
                        sequence_number,
                    });
                }
                Ok(requests)
            }
        }
    }
}
