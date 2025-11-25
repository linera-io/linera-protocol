// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Validator management commands.

use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::{bail, Context, Result};
use clap::Subcommand;
use linera_base::{
    crypto::{AccountPublicKey, Signer, ValidatorPublicKey},
    identifiers::ChainId,
};
use linera_client::{
    chain_listener::ClientContext as _, client_context::ClientContext,
    client_options::ClientContextOptions, wallet::Wallet,
};
use linera_core::{data_types::ClientOutcome, node::ValidatorNodeProvider};
use linera_execution::committee::{Committee, ValidatorState};
use linera_persistent::Persist;
use linera_rpc::node_provider::NodeProvider;
use linera_storage::Storage;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// Specification for a validator to add or modify.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidatorSpec {
    pub public_key: ValidatorPublicKey,
    pub account_key: AccountPublicKey,
    pub network_address: String,
    #[serde(default)]
    pub votes: u64,
}

impl ValidatorSpec {
    /// Validate the validator specification.
    fn validate(&self) -> Result<()> {
        if self.votes == 0 {
            bail!("Validator votes must be greater than 0");
        }
        if self.network_address.is_empty() {
            bail!("Validator network address cannot be empty");
        }
        Ok(())
    }
}

/// Specification for validator changes in batch operations.
/// When all fields are None, this represents a removal operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidatorChangeSpec {
    pub account_key: Option<AccountPublicKey>,
    #[serde(rename = "address")]
    pub network_address: Option<String>,
    pub votes: Option<u64>,
}

impl ValidatorChangeSpec {
    /// Check if this spec represents an empty/remove operation.
    fn is_empty(&self) -> bool {
        self.account_key.is_none() && self.network_address.is_none() && self.votes.is_none()
    }

    /// Validate that all required fields are present for add/modify operations.
    fn validate(&self) -> Result<()> {
        if self.is_empty() {
            return Ok(()); // Empty is valid (represents removal)
        }

        if self.account_key.is_none() {
            bail!("account_key is required for add/modify operations");
        }
        if self.network_address.is_none() {
            bail!("address is required for add/modify operations");
        }
        let address = self.network_address.as_ref().unwrap();
        if address.is_empty() {
            bail!("Validator network address cannot be empty");
        }
        let votes = self.votes.unwrap_or(1);
        if votes == 0 {
            bail!("Validator votes must be greater than 0");
        }

        Ok(())
    }

    /// Get the votes value, defaulting to 1 if not specified.
    fn votes(&self) -> u64 {
        self.votes.unwrap_or(1)
    }
}

/// Structure for batch validator operations from JSON file.
/// Maps validator public keys to their desired state:
/// - `None` or empty spec (all fields None) means remove the validator
/// - `Some(spec)` with values means add or modify the validator
pub type ValidatorBatchFile = HashMap<ValidatorPublicKey, Option<ValidatorChangeSpec>>;

/// Structure for batch validator queries from JSON file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorQueryBatch {
    pub validators: Vec<ValidatorSpec>,
}

/// Validator subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum ValidatorCommand {
    /// Add a validator to the committee.
    ///
    /// Adds a new validator with the specified public key, account key, network address,
    /// and voting weight. The validator must not already exist in the committee.
    Add {
        /// Public key of the validator to add
        #[arg(long)]
        public_key: ValidatorPublicKey,
        /// Account public key for receiving payments and rewards
        #[arg(long)]
        account_key: AccountPublicKey,
        /// Network address where the validator can be reached (e.g., grpcs://host:port)
        #[arg(long)]
        address: String,
        /// Voting weight for consensus (default: 1)
        #[arg(long, default_value = "1")]
        votes: u64,
        /// Skip online connectivity verification before adding
        #[arg(long)]
        skip_online_check: bool,
    },

    /// Query multiple validators using a JSON specification file.
    ///
    /// Reads validator specifications from a JSON file and queries their state.
    /// The JSON should contain an array of validator objects with publicKey and networkAddress.
    BatchQuery {
        /// Path to JSON file containing validator query specifications
        file: String,
        /// Chain ID to query (defaults to default chain)
        #[arg(long)]
        chain_id: Option<ChainId>,
    },

    /// Apply multiple validator changes from JSON input.
    ///
    /// Reads a JSON object mapping validator public keys to their desired state:
    /// - Key with state object (address, votes, accountKey): add or modify validator
    /// - Key with null or {}: remove validator
    /// - Keys not present: unchanged
    ///
    /// Input can be provided via file path, stdin pipe, or shell redirect.
    BatchUpdate {
        /// Path to JSON file with validator changes (omit or use "-" for stdin)
        file: Option<String>,
        /// Preview changes without applying them
        #[arg(long)]
        dry_run: bool,
        /// Skip confirmation prompt (use with caution)
        #[arg(long, short = 'y')]
        yes: bool,
        /// Skip online connectivity checks for validators being added or modified
        #[arg(long)]
        skip_online_check: bool,
    },

    /// List all validators in the committee.
    ///
    /// Displays the current validator set with their network addresses, voting weights,
    /// and connection status. Optionally filter by minimum voting weight.
    List {
        /// Chain ID to query (defaults to default chain)
        #[arg(long)]
        chain_id: Option<ChainId>,
        /// Only show validators with at least this many votes
        #[arg(long)]
        min_votes: Option<u64>,
    },

    /// Query a single validator's state and connectivity.
    ///
    /// Connects to a validator at the specified network address and queries its
    /// view of the blockchain state, including block height and committee information.
    Query {
        /// Network address of the validator (e.g., grpcs://host:port)
        address: String,
        /// Chain ID to query about (defaults to default chain)
        #[arg(long)]
        chain_id: Option<ChainId>,
        /// Expected public key of the validator (for verification)
        #[arg(long)]
        public_key: Option<ValidatorPublicKey>,
    },

    /// Remove a validator from the committee.
    ///
    /// Removes the validator with the specified public key from the committee.
    /// The validator will no longer participate in consensus.
    Remove {
        /// Public key of the validator to remove
        #[arg(long)]
        public_key: ValidatorPublicKey,
    },

    /// Synchronize chain state to a validator.
    ///
    /// Pushes the current chain state from local storage to a validator node,
    /// ensuring the validator has up-to-date information about specified chains.
    Sync {
        /// Network address of the validator to sync (e.g., grpcs://host:port)
        address: String,
        /// Chain IDs to synchronize (defaults to all chains in wallet)
        #[arg(long)]
        chains: Vec<ChainId>,
        /// Verify validator is online before syncing
        #[arg(long)]
        check_online: bool,
    },
}

/// Parse a batch operations file or stdin.
/// If path is None or "-", reads from stdin.
/// Otherwise reads from the specified file path.
fn parse_batch_file(path: Option<&str>) -> Result<ValidatorBatchFile> {
    let contents = match path {
        None | Some("-") => {
            // Read from stdin
            use std::io::Read;
            let mut buffer = String::new();
            std::io::stdin()
                .read_to_string(&mut buffer)
                .context("Failed to read from stdin")?;
            buffer
        }
        Some(path_str) => {
            // Read from file
            std::fs::read_to_string(path_str)
                .with_context(|| format!("Failed to read batch file: {}", path_str))?
        }
    };

    let batch: ValidatorBatchFile =
        serde_json::from_str(&contents).context("Failed to parse batch JSON")?;

    // Validate all specs
    for (public_key, spec_opt) in &batch {
        if let Some(spec) = spec_opt {
            spec.validate()
                .with_context(|| format!("Invalid validator spec for {}", public_key))?;
        }
    }

    Ok(batch)
}

/// Parse a validator query batch file.
fn parse_query_batch_file(path: &Path) -> Result<ValidatorQueryBatch> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read query batch file: {}", path.display()))?;
    let batch: ValidatorQueryBatch = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse query batch file: {}", path.display()))?;
    Ok(batch)
}

/// Main entry point for handling validator commands.
pub async fn handle_command<S, W, Si>(
    context_options: ClientContextOptions,
    storage: S,
    wallet: W,
    signer: Si,
    block_cache_size: usize,
    execution_state_cache_size: usize,
    command: ValidatorCommand,
) -> Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
    W: Persist<Target = Wallet>,
    Si: Signer + Send + Sync + 'static,
{
    use ValidatorCommand::*;

    match command {
        Add {
            public_key,
            account_key,
            address,
            votes,
            skip_online_check,
        } => {
            let context = ClientContext::new(
                storage.clone(),
                context_options.clone(),
                wallet,
                signer,
                block_cache_size,
                execution_state_cache_size,
            );
            let context = Arc::new(Mutex::new(context));
            handle_add(
                context,
                public_key,
                account_key,
                address,
                votes,
                skip_online_check,
            )
            .await
        }

        BatchQuery { file, chain_id } => {
            let context = ClientContext::new(
                storage.clone(),
                context_options.clone(),
                wallet,
                signer,
                block_cache_size,
                execution_state_cache_size,
            );
            handle_query_batch(context, file, chain_id).await
        }

        BatchUpdate {
            file,
            dry_run,
            yes,
            skip_online_check,
        } => {
            let context = ClientContext::new(
                storage.clone(),
                context_options.clone(),
                wallet,
                signer,
                block_cache_size,
                execution_state_cache_size,
            );
            let context = Arc::new(Mutex::new(context));
            handle_batch_update(context, file.as_deref(), dry_run, yes, skip_online_check).await
        }

        List {
            chain_id,
            min_votes,
        } => {
            let context = ClientContext::new(
                storage,
                context_options,
                wallet,
                signer,
                block_cache_size,
                execution_state_cache_size,
            );
            handle_list(context, chain_id, min_votes).await
        }

        Query {
            address,
            chain_id,
            public_key,
        } => {
            let context = ClientContext::new(
                storage.clone(),
                context_options.clone(),
                wallet,
                signer,
                block_cache_size,
                execution_state_cache_size,
            );
            handle_query(context, address, chain_id, public_key).await
        }

        Remove { public_key } => {
            let context = ClientContext::new(
                storage.clone(),
                context_options.clone(),
                wallet,
                signer,
                block_cache_size,
                execution_state_cache_size,
            );
            let context = Arc::new(Mutex::new(context));
            handle_remove(context, public_key).await
        }

        Sync {
            address,
            chains,
            check_online,
        } => {
            let context = ClientContext::new(
                storage.clone(),
                context_options.clone(),
                wallet,
                signer,
                block_cache_size,
                execution_state_cache_size,
            );
            let context = Arc::new(Mutex::new(context));
            handle_sync(context, address, chains, check_online).await
        }
    }
}

// Handler implementations will go here...
// (Next message will contain the handler implementations)

/// Handle query command: query a single validator about a chain.
async fn handle_query<S, W, Si>(
    context: ClientContext<linera_core::environment::Impl<S, NodeProvider, Si>, W>,
    address: String,
    chain_id: Option<ChainId>,
    public_key: Option<ValidatorPublicKey>,
) -> Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
    W: Persist<Target = Wallet>,
    Si: Signer + Send + Sync + 'static,
{
    let node = context.make_node_provider().make_node(&address)?;
    let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
    println!("Querying validator about chain {chain_id}.\n");

    let results = context
        .query_validator(&address, &node, chain_id, public_key.as_ref())
        .await;

    for error in results.errors() {
        error!("{}", error);
    }

    results.print(public_key.as_ref(), Some(&address), None, None);

    if !results.errors().is_empty() {
        bail!("Found one or several issue(s) while querying validator {address}");
    }

    Ok(())
}

/// Handle query-batch command: query multiple validators from a JSON file.
async fn handle_query_batch<S, W, Si>(
    context: ClientContext<linera_core::environment::Impl<S, NodeProvider, Si>, W>,
    file: String,
    chain_id: Option<ChainId>,
) -> Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
    W: Persist<Target = Wallet>,
    Si: Signer + Send + Sync + 'static,
{
    let batch = parse_query_batch_file(Path::new(&file))?;
    let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
    println!(
        "Querying {} validators about chain {chain_id}.\n",
        batch.validators.len()
    );

    let node_provider = context.make_node_provider();
    let mut has_errors = false;

    for spec in batch.validators {
        let node = node_provider.make_node(&spec.network_address)?;
        let results = context
            .query_validator(
                &spec.network_address,
                &node,
                chain_id,
                Some(&spec.public_key),
            )
            .await;

        if !results.errors().is_empty() {
            has_errors = true;
            for error in results.errors() {
                error!("Validator {}: {}", spec.public_key, error);
            }
        }

        results.print(
            Some(&spec.public_key),
            Some(&spec.network_address),
            None,
            None,
        );
    }

    if has_errors {
        bail!("Found issues while querying validators");
    }

    Ok(())
}

/// Handle list command: list all validators in the committee.
async fn handle_list<S, W, Si>(
    mut context: ClientContext<linera_core::environment::Impl<S, NodeProvider, Si>, W>,
    chain_id: Option<ChainId>,
    min_votes: Option<u64>,
) -> Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
    W: Persist<Target = Wallet>,
    Si: Signer + Send + Sync + 'static,
{
    let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
    println!("Querying validators about chain {chain_id}.\n");

    let local_results = context.query_local_node(chain_id).await;
    let chain_client = context.make_chain_client(chain_id);
    info!("Querying validators about chain {}", chain_id);
    let result = chain_client.local_committee().await;
    context.update_wallet_from_client(&chain_client).await?;
    let committee = result.context("Failed to get local committee")?;

    info!(
        "Using the local set of validators: {:?}",
        committee.validators()
    );

    let node_provider = context.make_node_provider();
    let mut validator_results = Vec::new();

    for (name, state) in committee.validators() {
        if min_votes.is_some_and(|votes| state.votes < votes) {
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
            error!("{}", error);
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
        bail!("Found faulty validators");
    }

    Ok(())
}

/// Handle add command: add a new validator to the committee.
async fn handle_add<S, W, Si>(
    context: Arc<Mutex<ClientContext<linera_core::environment::Impl<S, NodeProvider, Si>, W>>>,
    public_key: ValidatorPublicKey,
    account_key: AccountPublicKey,
    address: String,
    votes: u64,
    skip_online_check: bool,
) -> Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
    W: Persist<Target = Wallet>,
    Si: Signer + Send + Sync + 'static,
{
    info!("Starting operation to add validator");
    let time_start = std::time::Instant::now();

    // Validate the validator spec
    let spec = ValidatorSpec {
        public_key,
        account_key,
        network_address: address.clone(),
        votes,
    };
    spec.validate()?;

    // Check validator is online if requested
    let mut context = context.lock().await;
    if !skip_online_check {
        let node = context.make_node_provider().make_node(&address)?;
        context
            .check_compatible_version_info(&address, &node)
            .await?;
        context
            .check_matching_network_description(&address, &node)
            .await?;
    }

    let admin_id = context.wallet().genesis_admin_chain();
    let chain_client = context.make_chain_client(admin_id);

    // Synchronize the chain state
    chain_client.synchronize_chain_state(admin_id).await?;

    let maybe_certificate = context
        .apply_client_command(&chain_client, |chain_client| {
            let chain_client = chain_client.clone();
            let address = address.clone();
            async move {
                // Create the new committee.
                let mut committee = chain_client.local_committee().await?;
                let policy = committee.policy().clone();
                let mut validators = committee.validators().clone();

                validators.insert(
                    public_key,
                    ValidatorState {
                        network_address: address,
                        votes,
                        account_public_key: account_key,
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
    info!("Created new committee:\n{:?}", certificate);

    let time_total = time_start.elapsed();
    info!("Operation confirmed after {} ms", time_total.as_millis());

    Ok(())
}

/// Handle remove command: remove a validator from the committee.
async fn handle_remove<S, W, Si>(
    context: Arc<Mutex<ClientContext<linera_core::environment::Impl<S, NodeProvider, Si>, W>>>,
    public_key: ValidatorPublicKey,
) -> Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
    W: Persist<Target = Wallet>,
    Si: Signer + Send + Sync + 'static,
{
    info!("Starting operation to remove validator");
    let time_start = std::time::Instant::now();

    let mut context = context.lock().await;
    let admin_id = context.wallet().genesis_admin_chain();
    let chain_client = context.make_chain_client(admin_id);

    // Synchronize the chain state
    chain_client.synchronize_chain_state(admin_id).await?;

    let maybe_certificate = context
        .apply_client_command(&chain_client, |chain_client| {
            let chain_client = chain_client.clone();
            async move {
                // Create the new committee.
                let mut committee = chain_client.local_committee().await?;
                let policy = committee.policy().clone();
                let mut validators = committee.validators().clone();

                if validators.remove(&public_key).is_none() {
                    error!("Validator {public_key} does not exist; aborting.");
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
    info!("Created new committee:\n{:?}", certificate);

    let time_total = time_start.elapsed();
    info!("Operation confirmed after {} ms", time_total.as_millis());

    Ok(())
}

/// Handle batch-update command: apply a batch file with add/modify/remove operations.
async fn handle_batch_update<S, W, Si>(
    context: Arc<Mutex<ClientContext<linera_core::environment::Impl<S, NodeProvider, Si>, W>>>,
    file: Option<&str>,
    dry_run: bool,
    yes: bool,
    skip_online_check: bool,
) -> Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
    W: Persist<Target = Wallet>,
    Si: Signer + Send + Sync + 'static,
{
    info!("Starting batch update operation");
    let time_start = std::time::Instant::now();

    // Parse the batch file or stdin
    let batch = parse_batch_file(file)?;

    if batch.is_empty() {
        println!("No validator changes specified in input.");
        return Ok(());
    }

    // Separate operations by type for logging and validation
    let mut adds = Vec::new();
    let mut modifies = Vec::new();
    let mut removes = Vec::new();

    // Get current committee to determine if operation is add or modify
    let context_guard = context.lock().await;
    let admin_id = context_guard.wallet().genesis_admin_chain();
    let chain_client = context_guard.make_chain_client(admin_id);
    let current_committee = chain_client.local_committee().await?;
    let current_validators = current_committee.validators();
    drop(context_guard);

    for (public_key, spec_opt) in &batch {
        match spec_opt {
            None => {
                removes.push(*public_key);
            }
            Some(spec) if spec.is_empty() => {
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
    println!("\n╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                        VALIDATOR BATCH UPDATE RECAP                          ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝\n");

    println!("Summary:");
    println!("  • {} validator(s) to add", adds.len());
    println!("  • {} validator(s) to modify", modifies.len());
    println!("  • {} validator(s) to remove", removes.len());
    println!();

    if !adds.is_empty() {
        println!("Validators to ADD:");
        for (pk, spec) in &adds {
            println!("  + {}", pk);
            println!(
                "    Address:     {}",
                spec.network_address.as_ref().unwrap()
            );
            println!("    Account Key: {}", spec.account_key.unwrap());
            println!("    Votes:       {}", spec.votes());
        }
        println!();
    }

    if !modifies.is_empty() {
        println!("Validators to MODIFY:");
        for (pk, spec) in &modifies {
            println!("  * {}", pk);
            println!(
                "    New Address:     {}",
                spec.network_address.as_ref().unwrap()
            );
            println!("    New Account Key: {}", spec.account_key.unwrap());
            println!("    New Votes:       {}", spec.votes());
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

    if dry_run {
        println!("═════════════════════════════════════════════════════════════════════════════");
        println!("DRY RUN MODE: No changes will be applied");
        println!("═════════════════════════════════════════════════════════════════════════════\n");
        return Ok(());
    }

    // Confirmation prompt (unless --yes flag is set)
    if !yes {
        println!("═════════════════════════════════════════════════════════════════════════════");
        println!("⚠️  WARNING: This operation will modify the validator committee.");
        println!("             Changes are permanent and will be broadcast to the network.");
        println!("═════════════════════════════════════════════════════════════════════════════\n");
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
    if !skip_online_check {
        let context_guard = context.lock().await;
        let node_provider = context_guard.make_node_provider();

        info!("Checking validators are online...");
        for (_, spec) in adds.iter().chain(modifies.iter()) {
            let address = spec.network_address.as_ref().unwrap();
            let node = node_provider.make_node(address)?;
            context_guard
                .check_compatible_version_info(address, &node)
                .await?;
            context_guard
                .check_matching_network_description(address, &node)
                .await?;
        }
        drop(context_guard);
    }

    let mut context = context.lock().await;
    let admin_id = context.wallet().genesis_admin_chain();
    let chain_client = context.make_chain_client(admin_id);

    // Synchronize the chain state
    chain_client.synchronize_chain_state(admin_id).await?;

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
                for (public_key, spec_opt) in &batch {
                    match spec_opt {
                        None => {
                            // Explicit null - remove
                            if validators.remove(public_key).is_none() {
                                warn!("Validator {} does not exist; skipping remove", public_key);
                            } else {
                                info!("Removed validator {}", public_key);
                            }
                        }
                        Some(spec) if spec.is_empty() => {
                            // Empty object - remove
                            if validators.remove(public_key).is_none() {
                                warn!("Validator {} does not exist; skipping remove", public_key);
                            } else {
                                info!("Removed validator {}", public_key);
                            }
                        }
                        Some(spec) => {
                            // Has values - add or modify
                            let address = spec.network_address.as_ref().unwrap().clone();
                            let votes = spec.votes();
                            let account_key = spec.account_key.unwrap();

                            let exists = validators.contains_key(public_key);
                            validators.insert(
                                *public_key,
                                ValidatorState {
                                    network_address: address.clone(),
                                    votes,
                                    account_public_key: account_key,
                                },
                            );

                            if exists {
                                info!(
                                    "Modified validator {} @ {} ({} votes)",
                                    public_key, address, votes
                                );
                            } else {
                                info!(
                                    "Added validator {} @ {} ({} votes)",
                                    public_key, address, votes
                                );
                            }
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
        info!("No changes applied");
        return Ok(());
    };

    info!("Created new committee:\n{:?}", certificate);
    let time_total = time_start.elapsed();
    info!("Batch update confirmed after {} ms", time_total.as_millis());

    Ok(())
}

/// Handle sync command: sync validator(s) to specific chains.
async fn handle_sync<S, W, Si>(
    context: Arc<Mutex<ClientContext<linera_core::environment::Impl<S, NodeProvider, Si>, W>>>,
    address: String,
    chains: Vec<linera_base::identifiers::ChainId>,
    check_online: bool,
) -> Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
    W: Persist<Target = Wallet>,
    Si: Signer + Send + Sync + 'static,
{
    info!("Starting sync operation for validator at {}", address);

    let context = context.lock().await;

    // Check validator is online if requested
    if check_online {
        let node_provider = context.make_node_provider();
        let node = node_provider.make_node(&address)?;
        context
            .check_compatible_version_info(&address, &node)
            .await?;
        context
            .check_matching_network_description(&address, &node)
            .await?;
    }

    // If no chains specified, use all chains from wallet
    let chains_to_sync = if chains.is_empty() {
        context.wallet().chain_ids()
    } else {
        chains
    };

    info!(
        "Syncing {} chains to validator {}",
        chains_to_sync.len(),
        address
    );

    // Create validator node
    let node_provider = context.make_node_provider();
    let validator = node_provider.make_node(&address)?;

    // Sync each chain
    for chain_id in chains_to_sync {
        info!("Syncing chain {} to {}", chain_id, address);
        let chain = context.make_chain_client(chain_id);

        chain.sync_validator(validator.clone()).await?;
        info!("Chain {} synced successfully", chain_id);
    }

    info!("Sync operation completed successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_validate_validator_change_spec_valid() {
        let spec = ValidatorChangeSpec {
            account_key: Some(AccountPublicKey::test_key(0)),
            network_address: Some("grpcs://validator.example.com:443".to_string()),
            votes: Some(100),
        };

        assert!(spec.validate().is_ok());
    }

    #[test]
    fn test_validate_validator_change_spec_zero_votes() {
        let spec = ValidatorChangeSpec {
            account_key: Some(AccountPublicKey::test_key(0)),
            network_address: Some("grpcs://validator.example.com:443".to_string()),
            votes: Some(0),
        };

        let result = spec.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("votes must be greater than 0"));
    }

    #[test]
    fn test_validate_validator_change_spec_empty_address() {
        let spec = ValidatorChangeSpec {
            account_key: Some(AccountPublicKey::test_key(0)),
            network_address: Some(String::new()),
            votes: Some(100),
        };

        let result = spec.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("network address cannot be empty"));
    }

    #[test]
    fn test_validate_validator_change_spec_empty() {
        let spec = ValidatorChangeSpec {
            account_key: None,
            network_address: None,
            votes: None,
        };

        assert!(spec.is_empty());
        assert!(spec.validate().is_ok()); // Empty is valid (represents removal)
    }

    #[test]
    fn test_parse_batch_file_valid() {
        // Generate correct JSON format using test keys
        let pk0 = ValidatorPublicKey::test_key(0);
        let pk1 = ValidatorPublicKey::test_key(1);
        let pk2 = ValidatorPublicKey::test_key(2);

        let mut batch = ValidatorBatchFile::new();

        // Add operation - validator with full spec
        batch.insert(
            pk0,
            Some(ValidatorChangeSpec {
                account_key: Some(AccountPublicKey::test_key(0)),
                network_address: Some("grpcs://validator1.example.com:443".to_string()),
                votes: Some(100),
            }),
        );

        // Modify operation - validator with full spec (would be modify if validator exists)
        batch.insert(
            pk1,
            Some(ValidatorChangeSpec {
                account_key: Some(AccountPublicKey::test_key(1)),
                network_address: Some("grpcs://validator2.example.com:443".to_string()),
                votes: Some(150),
            }),
        );

        // Remove operation - null
        batch.insert(pk2, None);

        let json = serde_json::to_string(&batch).unwrap();

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = parse_batch_file(Some(temp_file.path().to_str().unwrap()));
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
        assert_eq!(spec0.votes, Some(100));

        // Check pk1 (modify)
        assert!(parsed_batch.contains_key(&pk1));
        let spec1 = parsed_batch.get(&pk1).unwrap().as_ref().unwrap();
        assert_eq!(spec1.votes, Some(150));

        // Check pk2 (remove)
        assert!(parsed_batch.contains_key(&pk2));
        assert!(parsed_batch.get(&pk2).unwrap().is_none());
    }

    #[test]
    fn test_parse_batch_file_empty() {
        let json = r#"{}"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = parse_batch_file(Some(temp_file.path().to_str().unwrap()));
        assert!(result.is_ok());

        let batch = result.unwrap();
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_parse_batch_file_invalid_json() {
        let json = "{ invalid json }";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = parse_batch_file(Some(temp_file.path().to_str().unwrap()));
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_batch_file_nonexistent() {
        let result = parse_batch_file(Some("/nonexistent/file.json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_batch_file_empty_spec_as_remove() {
        // Test that empty object {} is treated as removal
        let pk = ValidatorPublicKey::test_key(0);
        let mut batch = ValidatorBatchFile::new();
        batch.insert(
            pk,
            Some(ValidatorChangeSpec {
                account_key: None,
                network_address: None,
                votes: None,
            }),
        );

        let json = serde_json::to_string(&batch).unwrap();

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = parse_batch_file(Some(temp_file.path().to_str().unwrap()));
        assert!(result.is_ok());

        let parsed_batch = result.unwrap();
        let spec = parsed_batch.get(&pk).unwrap().as_ref().unwrap();
        assert!(spec.is_empty());
    }

    #[test]
    fn test_parse_query_batch_file_valid() {
        // Generate correct JSON format using test keys
        let spec1 = ValidatorSpec {
            public_key: ValidatorPublicKey::test_key(0),
            account_key: AccountPublicKey::test_key(0),
            network_address: "grpcs://validator1.example.com:443".to_string(),
            votes: 100,
        };
        let spec2 = ValidatorSpec {
            public_key: ValidatorPublicKey::test_key(1),
            account_key: AccountPublicKey::test_key(1),
            network_address: "grpcs://validator2.example.com:443".to_string(),
            votes: 150,
        };

        let batch = ValidatorQueryBatch {
            validators: vec![spec1, spec2],
        };

        let json = serde_json::to_string(&batch).unwrap();

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = parse_query_batch_file(temp_file.path());
        assert!(
            result.is_ok(),
            "Failed to parse query batch file: {:?}",
            result.err()
        );

        let parsed_batch = result.unwrap();
        assert_eq!(parsed_batch.validators.len(), 2);
        assert_eq!(parsed_batch.validators[0].votes, 100);
        assert_eq!(parsed_batch.validators[1].votes, 150);
    }

    #[test]
    fn test_parse_query_batch_file_invalid_json() {
        let json = "{ invalid json }";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = parse_query_batch_file(temp_file.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_query_batch_file_nonexistent() {
        let result = parse_query_batch_file(Path::new("/nonexistent/file.json"));
        assert!(result.is_err());
    }
}
