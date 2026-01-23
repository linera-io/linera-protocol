// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Validator management commands.

use std::{collections::HashMap, num::NonZero, str::FromStr};

use anyhow::Context as _;
use futures::stream::TryStreamExt as _;
use linera_base::{
    crypto::{AccountPublicKey, ValidatorPublicKey},
    identifiers::ChainId,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::{data_types::ClientOutcome, node::ValidatorNodeProvider, Wallet as _};
use linera_execution::committee::{Committee, ValidatorState};
use serde::{Deserialize, Serialize};

/// Type alias for the complex ClientContext type used throughout validator operations.
/// This alias helps avoid clippy's type_complexity warnings while maintaining type safety.
/// Uses generic Environment trait to avoid coupling to implementation details.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Votes(pub NonZero<u64>);

impl Default for Votes {
    fn default() -> Self {
        Self(nonzero_lit::u64!(1))
    }
}

impl FromStr for Votes {
    type Err = <NonZero<u64> as FromStr>::Err;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Votes(s.parse()?))
    }
}

/// Specification for a validator to add or modify.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Spec {
    pub public_key: ValidatorPublicKey,
    pub account_key: AccountPublicKey,
    pub network_address: url::Url,
    #[serde(default)]
    pub votes: Votes,
}

/// Represents an update to a validator's configuration in batch operations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Change {
    pub account_key: AccountPublicKey,
    pub address: url::Url,
    #[serde(default)]
    pub votes: Votes,
}

/// Structure for batch validator operations from JSON file.
/// Maps validator public keys to their desired state:
/// - `null` means remove the validator
/// - `{accountKey, address, votes}` means add or modify the validator
/// - Keys not present in the map are left unchanged
pub type BatchFile = HashMap<ValidatorPublicKey, Option<Change>>;

/// Structure for batch validator queries from JSON file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryBatch {
    pub validators: Vec<Spec>,
}

/// Validator subcommands.
#[derive(Debug, Clone, clap::Subcommand)]
pub enum Command {
    Add(Add),
    BatchQuery(BatchQuery),
    Update(Update),
    List(List),
    Query(Query),
    Remove(Remove),
    Sync(Sync),
}

/// Add a validator to the committee.
///
/// Adds a new validator with the specified public key, account key, network address,
/// and voting weight. The validator must not already exist in the committee.
#[derive(Debug, Clone, clap::Parser)]
pub struct Add {
    /// Public key of the validator to add
    #[arg(long)]
    public_key: ValidatorPublicKey,
    /// Account public key for receiving payments and rewards
    #[arg(long)]
    account_key: AccountPublicKey,
    /// Network address where the validator can be reached (e.g., grpcs://host:port)
    #[arg(long)]
    address: url::Url,
    /// Voting weight for consensus (default: 1)
    #[arg(long, required = false)]
    votes: Votes,
    /// Skip online connectivity verification before adding
    #[arg(long)]
    skip_online_check: bool,
}

/// Query multiple validators using a JSON specification file.
///
/// Reads validator specifications from a JSON file and queries their state.
/// The JSON should contain an array of validator objects with publicKey and networkAddress.
#[derive(Debug, Clone, clap::Parser)]
pub struct BatchQuery {
    /// Path to JSON file containing validator query specifications
    file: clio::Input,
    /// Chain ID to query (defaults to default chain)
    #[arg(long)]
    chain_id: Option<ChainId>,
}

/// Apply multiple validator changes from JSON input.
///
/// Reads a JSON object mapping validator public keys to their desired state:
/// - Key with state object (address, votes, accountKey): add or modify validator
/// - Key with null: remove validator
/// - Keys not present: unchanged
///
/// Input can be provided via file path, stdin pipe, or shell redirect.
#[derive(Debug, Clone, clap::Parser)]
pub struct Update {
    /// Path to JSON file with validator changes (omit or use "-" for stdin)
    #[arg(required = false)]
    file: clio::Input,
    /// Preview changes without applying them
    #[arg(long)]
    dry_run: bool,
    /// Skip confirmation prompt (use with caution)
    #[arg(long, short = 'y')]
    yes: bool,
    /// Skip online connectivity checks for validators being added or modified
    #[arg(long)]
    skip_online_check: bool,
}

/// List all validators in the committee.
///
/// Displays the current validator set with their network addresses, voting weights,
/// and connection status. Optionally filter by minimum voting weight.
#[derive(Debug, Clone, clap::Parser)]
pub struct List {
    /// Chain ID to query (defaults to default chain)
    #[arg(long)]
    chain_id: Option<ChainId>,
    /// Only show validators with at least this many votes
    #[arg(long)]
    min_votes: Option<u64>,
}

/// Query a single validator's state and connectivity.
///
/// Connects to a validator at the specified network address and queries its
/// view of the blockchain state, including block height and committee information.
#[derive(Debug, Clone, clap::Parser)]
pub struct Query {
    /// Network address of the validator (e.g., grpcs://host:port)
    address: String,
    /// Chain ID to query about (defaults to default chain)
    #[arg(long)]
    chain_id: Option<ChainId>,
    /// Expected public key of the validator (for verification)
    #[arg(long)]
    public_key: Option<ValidatorPublicKey>,
}

/// Remove a validator from the committee.
///
/// Removes the validator with the specified public key from the committee.
/// The validator will no longer participate in consensus.
#[derive(Debug, Clone, clap::Parser)]
pub struct Remove {
    /// Public key of the validator to remove
    #[arg(long)]
    public_key: ValidatorPublicKey,
}

/// Synchronize chain state to a validator.
///
/// Pushes the current chain state from local storage to a validator node,
/// ensuring the validator has up-to-date information about specified chains.
#[derive(Debug, Clone, clap::Parser)]
pub struct Sync {
    /// Network address of the validator to sync (e.g., grpcs://host:port)
    address: String,
    /// Chain IDs to synchronize (defaults to all chains in wallet)
    #[arg(long)]
    chains: Vec<ChainId>,
    /// Verify validator is online before syncing
    #[arg(long)]
    check_online: bool,
}

/// Parse a batch operations file or stdin.
/// Reads from the provided clio::Input, which handles both files and stdin transparently.
fn parse_batch_file(input: clio::Input) -> anyhow::Result<BatchFile> {
    Ok(serde_json::from_reader(input)?)
}

/// Parse a validator query batch file.
fn parse_query_batch_file(input: clio::Input) -> anyhow::Result<QueryBatch> {
    Ok(serde_json::from_reader(input)?)
}

impl Command {
    /// Main entry point for handling validator commands.
    pub async fn run(
        &self,
        context: &mut ClientContext<
            impl linera_core::Environment<ValidatorNode = linera_rpc::Client>,
        >,
    ) -> anyhow::Result<()> {
        use Command::*;

        match self {
            Add(command) => command.run(context).await,
            BatchQuery(command) => command.run(context).await,
            Update(command) => command.run(context).await,
            List(command) => command.run(context).await,
            Query(command) => command.run(context).await,
            Remove(command) => command.run(context).await,
            Sync(command) => Box::pin(command.run(context)).await,
        }
    }
}

impl Add {
    async fn run(
        &self,
        context: &mut ClientContext<impl linera_core::Environment>,
    ) -> anyhow::Result<()> {
        tracing::info!("Starting operation to add validator");
        let time_start = std::time::Instant::now();

        // Check validator is online if requested
        if !self.skip_online_check {
            let node = context
                .make_node_provider()
                .make_node(self.address.as_str())?;
            context
                .check_compatible_version_info(self.address.as_str(), &node)
                .await?;
            context
                .check_matching_network_description(self.address.as_str(), &node)
                .await?;
        }

        let admin_chain_id = context.admin_chain_id();
        let chain_client = context.make_chain_client(admin_chain_id).await?;

        // Synchronize the chain state
        chain_client.synchronize_chain_state(admin_chain_id).await?;

        let maybe_certificate = context
            .apply_client_command(&chain_client, |chain_client| {
                let me = self.clone();
                let chain_client = chain_client.clone();
                async move {
                    // Create the new committee.
                    let mut committee = chain_client.local_committee().await?;
                    let policy = committee.policy().clone();
                    let mut validators = committee.validators().clone();

                    validators.insert(
                        me.public_key,
                        ValidatorState {
                            network_address: me.address.to_string(),
                            votes: me.votes.0.get(),
                            account_public_key: me.account_key,
                        },
                    );

                    committee = Committee::new(validators, policy);
                    chain_client
                        .stage_new_committee(committee)
                        .await
                        .map(|outcome| outcome.map(Some))
                }
            })
            .await
            .context("Failed to stage committee")?;

        let Some(certificate) = maybe_certificate else {
            return Ok(());
        };
        tracing::info!("Created new committee:\n{:?}", certificate);

        let time_total = time_start.elapsed();
        tracing::info!("Operation confirmed after {} ms", time_total.as_millis());

        Ok(())
    }
}

impl BatchQuery {
    async fn run(
        &self,
        context: &mut ClientContext<impl linera_core::Environment>,
    ) -> anyhow::Result<()> {
        let batch = parse_query_batch_file(self.file.clone())
            .context("parsing query batch file `{file}`")?;
        let chain_id = self.chain_id.unwrap_or_else(|| context.default_chain());
        println!(
            "Querying {} validators about chain {chain_id}.\n",
            batch.validators.len()
        );

        let node_provider = context.make_node_provider();
        let mut has_errors = false;

        for spec in batch.validators {
            let node = node_provider.make_node(spec.network_address.as_str())?;
            let results = context
                .query_validator(
                    spec.network_address.as_str(),
                    &node,
                    chain_id,
                    Some(&spec.public_key),
                )
                .await;

            if !results.errors().is_empty() {
                has_errors = true;
                for error in results.errors() {
                    tracing::error!("Validator {}: {}", spec.public_key, error);
                }
            }

            results.print(
                Some(&spec.public_key),
                Some(spec.network_address.as_str()),
                None,
                None,
            );
        }

        if has_errors {
            anyhow::bail!("Found issues while querying validators");
        }

        Ok(())
    }
}

impl Update {
    async fn run(
        &self,
        context: &mut ClientContext<impl linera_core::Environment>,
    ) -> anyhow::Result<()> {
        tracing::info!("Starting batch update operation");
        let time_start = std::time::Instant::now();

        // Parse the batch file or stdin
        let batch = parse_batch_file(self.file.clone())
            .with_context(|| format!("parsing batch file `{}`", self.file))?;

        if batch.is_empty() {
            tracing::warn!("No validator changes specified in input.");
            return Ok(());
        }

        // Separate operations by type for logging and validation
        let mut adds = Vec::new();
        let mut modifies = Vec::new();
        let mut removes = Vec::new();

        // Get current committee to determine if operation is add or modify
        let admin_chain_id = context.client().admin_chain_id();
        let chain_client = context.make_chain_client(admin_chain_id).await?;
        let current_committee = chain_client.local_committee().await?;
        let current_validators = current_committee.validators();

        for (public_key, change_opt) in &batch {
            match change_opt {
                None => {
                    // null = removal
                    removes.push(*public_key);
                }
                Some(spec) => {
                    if current_validators.contains_key(public_key) {
                        modifies.push((public_key, spec));
                    } else {
                        adds.push((public_key, spec));
                    }
                }
            }
        }

        // Display recap of changes
        println!(
            "\n╔══════════════════════════════════════════════════════════════════════════════╗"
        );
        println!(
            "║                        VALIDATOR BATCH UPDATE RECAP                          ║"
        );
        println!(
            "╚══════════════════════════════════════════════════════════════════════════════╝\n"
        );

        println!("Summary:");
        println!("  • {} validator(s) to add", adds.len());
        println!("  • {} validator(s) to modify", modifies.len());
        println!("  • {} validator(s) to remove", removes.len());
        println!();

        if !adds.is_empty() {
            println!("Validators to ADD:");
            for (pk, spec) in &adds {
                println!("  + {}", pk);
                println!("    Address:     {}", spec.address);
                println!("    Account Key: {}", spec.account_key);
                println!("    Votes:       {}", spec.votes.0.get());
            }
            println!();
        }

        if !modifies.is_empty() {
            println!("Validators to MODIFY:");
            for (pk, spec) in &modifies {
                println!("  * {}", pk);
                println!("    New Address:     {}", spec.address);
                println!("    New Account Key: {}", spec.account_key);
                println!("    New Votes:       {}", spec.votes.0.get());
            }
            println!();
        }

        if !removes.is_empty() {
            println!("Validators to REMOVE:");
            for pk in &removes {
                println!("  - {}", pk);
            }
            println!();
        }

        if self.dry_run {
            println!(
                "═════════════════════════════════════════════════════════════════════════════"
            );
            println!("DRY RUN MODE: No changes will be applied");
            println!(
                "═════════════════════════════════════════════════════════════════════════════\n"
            );
            return Ok(());
        }

        // Confirmation prompt (unless --yes flag is set)
        if !self.yes {
            println!(
                "═════════════════════════════════════════════════════════════════════════════"
            );
            println!("⚠️  WARNING: This operation will modify the validator committee.");
            println!("             Changes are permanent and will be broadcast to the network.");
            println!(
                "═════════════════════════════════════════════════════════════════════════════\n"
            );
            println!("Do you want to proceed? Type 'YES' (uppercase) to confirm: ");

            use std::io::{self, Write};
            io::stdout().flush()?;

            let mut input = String::new();
            io::stdin()
                .read_line(&mut input)
                .context("Failed to read confirmation input")?;

            let input = input.trim();
            if input != "YES" {
                println!("\nOperation cancelled. (Expected 'YES', got '{}')", input);
                return Ok(());
            }
            println!("\nConfirmed. Proceeding with batch update...\n");
        }

        // Check all validators are online if requested
        if !self.skip_online_check {
            let node_provider = context.make_node_provider();

            tracing::info!("Checking validators are online...");
            for (_, spec) in adds.iter().chain(modifies.iter()) {
                let address = &spec.address;
                let node = node_provider.make_node(address.as_str())?;
                context
                    .check_compatible_version_info(address.as_str(), &node)
                    .await?;
                context
                    .check_matching_network_description(address.as_str(), &node)
                    .await?;
            }
        }

        let admin_chain_id = context.admin_chain_id();
        let chain_client = context.make_chain_client(admin_chain_id).await?;

        // Synchronize the chain state
        chain_client.synchronize_chain_state(admin_chain_id).await?;

        let batch_clone = batch.clone();
        let maybe_certificate = context
            .apply_client_command(&chain_client, |chain_client| {
                let chain_client = chain_client.clone();
                let batch = batch_clone.clone();
                async move {
                    // Get current committee
                    let mut committee = chain_client.local_committee().await?;
                    let policy = committee.policy().clone();
                    let mut validators = committee.validators().clone();

                    // Apply operations based on the batch specification
                    for (public_key, change_opt) in &batch {
                        if let Some(spec) = change_opt {
                            // Update object - add or modify validator
                            let address = &spec.address;
                            let votes = spec.votes.0.get();
                            let account_key = spec.account_key;

                            let exists = validators.contains_key(public_key);
                            validators.insert(
                                *public_key,
                                ValidatorState {
                                    network_address: address.to_string(),
                                    votes,
                                    account_public_key: account_key,
                                },
                            );

                            if exists {
                                tracing::info!(
                                    "Modified validator {} @ {} ({} votes)",
                                    public_key,
                                    address,
                                    votes
                                );
                            } else {
                                tracing::info!(
                                    "Added validator {} @ {} ({} votes)",
                                    public_key,
                                    address,
                                    votes
                                );
                            }
                        } else {
                            // null - remove validator
                            if validators.remove(public_key).is_none() {
                                tracing::warn!(
                                    "Validator {} does not exist; skipping remove",
                                    public_key
                                );
                            } else {
                                tracing::info!("Removed validator {}", public_key);
                            }
                        }
                    }

                    // Create new committee
                    committee = Committee::new(validators, policy);
                    chain_client
                        .stage_new_committee(committee)
                        .await
                        .map(|outcome| outcome.map(Some))
                }
            })
            .await
            .context("Failed to stage committee")?;

        let Some(certificate) = maybe_certificate else {
            tracing::info!("No changes applied");
            return Ok(());
        };

        tracing::info!("Created new committee:\n{:?}", certificate);
        let time_total = time_start.elapsed();
        tracing::info!("Batch update confirmed after {} ms", time_total.as_millis());

        Ok(())
    }
}

impl List {
    async fn run(
        &self,
        context: &mut ClientContext<impl linera_core::Environment>,
    ) -> anyhow::Result<()> {
        let chain_id = self.chain_id.unwrap_or_else(|| context.default_chain());
        println!("Querying validators about chain {chain_id}.\n");

        let local_results = context.query_local_node(chain_id).await?;
        let chain_client = context.make_chain_client(chain_id).await?;
        tracing::info!("Querying validators about chain {}", chain_id);
        let result = chain_client.local_committee().await;
        context.update_wallet_from_client(&chain_client).await?;
        let committee = result.context("Failed to get local committee")?;

        tracing::info!(
            "Using the local set of validators: {:?}",
            committee.validators()
        );

        let node_provider = context.make_node_provider();
        let mut validator_results = Vec::new();

        for (name, state) in committee.validators() {
            if self.min_votes.is_some_and(|votes| state.votes < votes) {
                continue; // Skip validator with little voting weight.
            }
            let address = &state.network_address;
            let node = node_provider.make_node(address)?;
            let results = context
                .query_validator(address, &node, chain_id, Some(name))
                .await;
            validator_results.push((name, address, state.votes, results));
        }

        let mut faulty_validators = std::collections::BTreeMap::<_, Vec<_>>::new();
        for (name, address, _votes, results) in &validator_results {
            for error in results.errors() {
                tracing::error!("{}", error);
                faulty_validators
                    .entry((*name, *address))
                    .or_default()
                    .push(error);
            }
        }

        // Print local node results first (everything)
        println!("Local Node:");
        local_results.print(None, None, None, None);
        println!();

        // Print validator results (only differences from local node)
        for (name, address, votes, results) in &validator_results {
            results.print(
                Some(name),
                Some(address),
                Some(*votes),
                Some(&local_results),
            );
        }

        if !faulty_validators.is_empty() {
            println!("\nFaulty validators:");
            for ((name, address), errors) in faulty_validators {
                println!("  {} at {}: {} error(s)", name, address, errors.len());
            }
            anyhow::bail!("Found faulty validators");
        }

        Ok(())
    }
}

impl Query {
    async fn run(
        &self,
        context: &mut ClientContext<impl linera_core::Environment>,
    ) -> anyhow::Result<()> {
        let node = context.make_node_provider().make_node(&self.address)?;
        let chain_id = self.chain_id.unwrap_or_else(|| context.default_chain());
        println!("Querying validator about chain {chain_id}.\n");

        let results = context
            .query_validator(&self.address, &node, chain_id, self.public_key.as_ref())
            .await;

        for error in results.errors() {
            tracing::error!("{}", error);
        }

        results.print(self.public_key.as_ref(), Some(&self.address), None, None);

        if !results.errors().is_empty() {
            anyhow::bail!(
                "Found one or several issue(s) while querying validator {}",
                self.address
            );
        }

        Ok(())
    }
}

impl Remove {
    async fn run(
        &self,
        context: &mut ClientContext<impl linera_core::Environment>,
    ) -> anyhow::Result<()> {
        tracing::info!("Starting operation to remove validator");
        let time_start = std::time::Instant::now();

        let admin_chain_id = context.admin_chain_id();
        let chain_client = context.make_chain_client(admin_chain_id).await?;

        // Synchronize the chain state
        chain_client.synchronize_chain_state(admin_chain_id).await?;

        let maybe_certificate = context
            .apply_client_command(&chain_client, |chain_client| {
                let chain_client = chain_client.clone();
                async move {
                    // Create the new committee.
                    let mut committee = chain_client.local_committee().await?;
                    let policy = committee.policy().clone();
                    let mut validators = committee.validators().clone();

                    if validators.remove(&self.public_key).is_none() {
                        tracing::error!("Validator {} does not exist; aborting.", self.public_key);
                        return Ok(ClientOutcome::Committed(None));
                    }

                    committee = Committee::new(validators, policy);
                    chain_client
                        .stage_new_committee(committee)
                        .await
                        .map(|outcome| outcome.map(Some))
                }
            })
            .await
            .context("Failed to stage committee")?;

        let Some(certificate) = maybe_certificate else {
            return Ok(());
        };
        tracing::info!("Created new committee:\n{:?}", certificate);

        let time_total = time_start.elapsed();
        tracing::info!("Operation confirmed after {} ms", time_total.as_millis());

        Ok(())
    }
}

impl Sync {
    async fn run(
        &self,
        context: &mut ClientContext<
            impl linera_core::Environment<ValidatorNode = linera_rpc::Client>,
        >,
    ) -> anyhow::Result<()> {
        tracing::info!("Starting sync operation for validator at {}", self.address);

        // Check validator is online if requested
        if self.check_online {
            let node_provider = context.make_node_provider();
            let node = node_provider.make_node(&self.address)?;
            context
                .check_compatible_version_info(&self.address, &node)
                .await?;
            context
                .check_matching_network_description(&self.address, &node)
                .await?;
        }

        // If no chains specified, use all chains from wallet
        let chains_to_sync = if self.chains.is_empty() {
            context.wallet().chain_ids().try_collect().await?
        } else {
            self.chains.clone()
        };

        tracing::info!(
            "Syncing {} chains to validator {}",
            chains_to_sync.len(),
            self.address
        );

        // Create validator node
        let node_provider = context.make_node_provider();
        let validator = node_provider.make_node(&self.address)?;

        // Sync each chain
        for chain_id in chains_to_sync {
            tracing::info!("Syncing chain {} to {}", chain_id, self.address);
            let chain = context.make_chain_client(chain_id).await?;

            Box::pin(chain.sync_validator(validator.clone())).await?;
            tracing::info!("Chain {} synced successfully", chain_id);
        }

        tracing::info!("Sync operation completed successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_parse_batch_file_valid() {
        // Generate correct JSON format using test keys
        let pk0 = ValidatorPublicKey::test_key(0);
        let pk1 = ValidatorPublicKey::test_key(1);
        let pk2 = ValidatorPublicKey::test_key(2);

        let mut batch = BatchFile::new();

        // Add operation - validator with full spec
        batch.insert(
            pk0,
            Some(Change {
                account_key: AccountPublicKey::test_key(0),
                address: "grpcs://validator1.example.com:443".parse().unwrap(),
                votes: Votes(NonZero::new(100).unwrap()),
            }),
        );

        // Modify operation - validator with full spec (would be modify if validator exists)
        batch.insert(
            pk1,
            Some(Change {
                account_key: AccountPublicKey::test_key(1),
                address: "grpcs://validator2.example.com:443".parse().unwrap(),
                votes: Votes(NonZero::new(150).unwrap()),
            }),
        );

        // Remove operation - null
        batch.insert(pk2, None);

        let json = serde_json::to_string(&batch).unwrap();

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let input = clio::Input::new(temp_file.path().to_str().unwrap()).unwrap();
        let result = parse_batch_file(input);
        assert!(
            result.is_ok(),
            "Failed to parse batch file: {:?}",
            result.err()
        );

        let parsed_batch = result.unwrap();
        assert_eq!(parsed_batch.len(), 3);

        // Check pk0 (add)
        assert!(parsed_batch.contains_key(&pk0));
        let spec0 = parsed_batch.get(&pk0).unwrap().as_ref().unwrap();
        assert_eq!(spec0.votes.0.get(), 100);

        // Check pk1 (modify)
        assert!(parsed_batch.contains_key(&pk1));
        let spec1 = parsed_batch.get(&pk1).unwrap().as_ref().unwrap();
        assert_eq!(spec1.votes.0.get(), 150);

        // Check pk2 (remove with null)
        assert!(parsed_batch.contains_key(&pk2));
        assert!(parsed_batch.get(&pk2).unwrap().is_none());
    }

    #[test]
    fn test_parse_batch_file_empty() {
        let json = r#"{}"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let input = clio::Input::new(temp_file.path().to_str().unwrap()).unwrap();
        let result = parse_batch_file(input);
        assert!(result.is_ok());

        let batch = result.unwrap();
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_parse_query_batch_file_valid() {
        // Generate correct JSON format using test keys
        let spec1 = Spec {
            public_key: ValidatorPublicKey::test_key(0),
            account_key: AccountPublicKey::test_key(0),
            network_address: "grpcs://validator1.example.com:443".parse().unwrap(),
            votes: Votes(NonZero::new(100).unwrap()),
        };
        let spec2 = Spec {
            public_key: ValidatorPublicKey::test_key(1),
            account_key: AccountPublicKey::test_key(1),
            network_address: "grpcs://validator2.example.com:443".parse().unwrap(),
            votes: Votes(NonZero::new(150).unwrap()),
        };

        let batch = QueryBatch {
            validators: vec![spec1, spec2],
        };

        let json = serde_json::to_string(&batch).unwrap();

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = parse_query_batch_file(temp_file.path().try_into().unwrap());
        assert!(
            result.is_ok(),
            "Failed to parse query batch file: {:?}",
            result.err()
        );

        let parsed_batch = result.unwrap();
        assert_eq!(parsed_batch.validators.len(), 2);
        assert_eq!(parsed_batch.validators[0].votes.0.get(), 100);
        assert_eq!(parsed_batch.validators[1].votes.0.get(), 150);
    }
}
