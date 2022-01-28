// Copyright (c) Facebook, Inc. and its affiliates.
// SPDX-License-Identifier: Apache-2.0

use crate::transport::NetworkProtocol;
use zef_core::{
    base_types::*,
    client::AccountClientState,
    committee::Committee,
    messages::{Address, Certificate, Operation, Value},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
};

pub trait Import: DeserializeOwned {
    fn read(path: &Path) -> Result<Self, std::io::Error> {
        let data = fs::read(path)?;
        Ok(serde_json::from_slice(data.as_slice())?)
    }
}

pub trait Export: Serialize {
    fn write(&self, path: &Path) -> Result<(), std::io::Error> {
        let file = OpenOptions::new().create(true).write(true).open(path)?;
        let mut writer = BufWriter::new(file);
        let data = serde_json::to_string_pretty(self).unwrap();
        writer.write_all(data.as_ref())?;
        writer.write_all(b"\n")?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthorityConfig {
    pub network_protocol: NetworkProtocol,
    pub name: AuthorityName,
    pub host: String,
    pub base_port: u32,
    pub num_shards: u32,
}

impl AuthorityConfig {
    pub fn print(&self) {
        let data = serde_json::to_string(self).unwrap();
        println!("{}", data);
    }
}

#[derive(Serialize, Deserialize)]
pub struct AuthorityServerConfig {
    pub authority: AuthorityConfig,
    pub key: KeyPair,
}

impl Import for AuthorityServerConfig {}
impl Export for AuthorityServerConfig {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CommitteeConfig {
    pub authorities: Vec<AuthorityConfig>,
}

impl Import for CommitteeConfig {}
impl Export for CommitteeConfig {}

impl CommitteeConfig {
    pub fn into_committee(self) -> Committee {
        Committee::new(self.voting_rights())
    }

    fn voting_rights(&self) -> BTreeMap<AuthorityName, usize> {
        let mut map = BTreeMap::new();
        for authority in &self.authorities {
            map.insert(authority.name, 1);
        }
        map
    }
}

#[derive(Serialize, Deserialize)]
pub struct UserAccount {
    pub account_id: AccountId,
    pub key_pair: Option<KeyPair>,
    pub next_sequence_number: SequenceNumber,
    pub balance: Balance,
    pub sent_certificates: Vec<Certificate>,
    pub received_certificates: Vec<Certificate>,
}

impl UserAccount {
    pub fn new(account_id: AccountId) -> Self {
        Self {
            account_id,
            key_pair: None,
            next_sequence_number: SequenceNumber::new(),
            balance: Balance::default(),
            sent_certificates: Vec::new(),
            received_certificates: Vec::new(),
        }
    }

    pub fn make_initial(account_id: AccountId, balance: Balance) -> Self {
        let key_pair = KeyPair::generate();
        Self {
            account_id,
            key_pair: Some(key_pair),
            next_sequence_number: SequenceNumber::new(),
            balance,
            sent_certificates: Vec::new(),
            received_certificates: Vec::new(),
        }
    }
}

pub struct AccountsConfig {
    accounts: BTreeMap<AccountId, UserAccount>,
}

impl AccountsConfig {
    pub fn get(&self, account_id: &AccountId) -> Option<&UserAccount> {
        self.accounts.get(account_id)
    }

    pub fn get_or_insert(&mut self, account_id: AccountId) -> &UserAccount {
        self.accounts
            .entry(account_id.clone())
            .or_insert_with(|| UserAccount::new(account_id))
    }

    pub fn insert(&mut self, account: UserAccount) {
        self.accounts.insert(account.account_id.clone(), account);
    }

    pub fn num_accounts(&self) -> usize {
        self.accounts.len()
    }

    pub fn last_account(&mut self) -> Option<&UserAccount> {
        self.accounts.values().last()
    }

    pub fn accounts_mut(&mut self) -> impl Iterator<Item = &mut UserAccount> {
        self.accounts.values_mut()
    }

    pub fn update_from_state<A>(&mut self, state: &AccountClientState<A>) {
        let account = self
            .accounts
            .entry(state.account_id().clone())
            .or_insert_with(|| UserAccount::new(state.account_id().clone()));
        account.key_pair = state.key_pair().map(|k| k.copy());
        account.next_sequence_number = state.next_sequence_number();
        account.balance = state.balance();
        account.sent_certificates = state.sent_certificates().clone();
        account.received_certificates = state.received_certificates().cloned().collect();
    }

    pub fn update_for_received_request(&mut self, certificate: Certificate) {
        let request = match &certificate.value {
            Value::Confirm(r) => r,
            _ => return,
        };
        if let Operation::Transfer {
            recipient: Address::Account(recipient),
            amount,
            ..
        } = &request.operation
        {
            if let Some(config) = self.accounts.get_mut(recipient) {
                if let Err(position) = config
                    .received_certificates
                    .binary_search_by_key(&certificate.value.confirm_key(), |order| {
                        order.value.confirm_key()
                    })
                {
                    config.balance = config.balance.try_add((*amount).into()).unwrap();
                    config.received_certificates.insert(position, certificate)
                }
            }
        }
    }

    pub fn read_or_create(path: &Path) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        let reader = BufReader::new(file);
        let stream = serde_json::Deserializer::from_reader(reader).into_iter();
        Ok(Self {
            accounts: stream
                .filter_map(Result::ok)
                .map(|account: UserAccount| (account.account_id.clone(), account))
                .collect(),
        })
    }

    pub fn write(&self, path: &Path) -> Result<(), std::io::Error> {
        let file = OpenOptions::new().write(true).open(path)?;
        let mut writer = BufWriter::new(file);
        for account in self.accounts.values() {
            serde_json::to_writer(&mut writer, account)?;
            writer.write_all(b"\n")?;
        }
        Ok(())
    }
}

pub struct InitialStateConfig {
    pub accounts: Vec<(AccountId, AccountOwner, Balance)>,
}

impl InitialStateConfig {
    pub fn read(path: &Path) -> Result<Self, failure::Error> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut accounts = Vec::new();
        for line in reader.lines() {
            let line = line?;
            let elements = line.split(':').collect::<Vec<_>>();
            if elements.len() != 3 {
                failure::bail!("expecting three columns separated with ':'")
            }
            let id = elements[0].parse()?;
            let pubkey = elements[1].parse()?;
            let balance = elements[2].parse()?;
            accounts.push((id, pubkey, balance));
        }
        Ok(Self { accounts })
    }

    pub fn write(&self, path: &Path) -> Result<(), std::io::Error> {
        let file = OpenOptions::new().create(true).write(true).open(path)?;
        let mut writer = BufWriter::new(file);
        for (id, pubkey, balance) in &self.accounts {
            writeln!(writer, "{}:{}:{}", id, pubkey, balance)?;
        }
        Ok(())
    }
}
