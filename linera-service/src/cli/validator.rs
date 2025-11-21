// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Validator management commands.

use std::{path::Path, sync::Arc};

use anyhow::{bail, Context, Result};
use clap::Subcommand;
use linera_base::{
    crypto::{AccountPublicKey, Signer, ValidatorPublicKey},
    identifiers::ChainId,
};
use linera_client::{
    chain_listener::ClientContext as _,
    client_context::ClientContext,
    client_options::ClientContextOptions,
    wallet::Wallet,
};
use linera_core::{
    data_types::ClientOutcome,
    node::ValidatorNodeProvider,
};
use linera_execution::committee::{Committee, ValidatorState};
use linera_persistent::Persist;
use linera_rpc::node_provider::NodeProvider;
use linera_storage::Storage;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// Type alias for the complex ClientContext type used in validator handlers.
type MutexedContext<S, W, Si> =
    Arc<Mutex<ClientContext<linera_core::environment::Impl<S, NodeProvider, Si>, W>>>;

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

/// Structure for batch validator operations from JSON file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidatorBatchFile {
    #[serde(default)]
    pub add: Vec<ValidatorSpec>,
    #[serde(default)]
    pub modify: Vec<ValidatorSpec>,
    #[serde(default)]
    pub remove: Vec<ValidatorPublicKey>,
}

/// Structure for batch validator queries from JSON file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorQueryBatch {
    pub validators: Vec<ValidatorSpec>,
}

/// Validator subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum ValidatorCommand {
    /// Query a single validator's state.
    Query {
        /// Network address of the validator to query.
        address: String,
        /// Optional chain ID to query about (defaults to default chain).
        #[arg(long)]
        chain_id: Option<ChainId>,
        /// Optional validator public key to query about.
        #[arg(long)]
        public_key: Option<ValidatorPublicKey>,
    },

    /// Query multiple validators from a JSON file.
    QueryBatch {
        /// Path to JSON file with validator query specifications.
        file: String,
        /// Optional chain ID to query about (defaults to default chain).
        #[arg(long)]
        chain_id: Option<ChainId>,
    },

    /// List all validators in the committee.
    List {
        /// Optional chain ID (defaults to default chain).
        #[arg(long)]
        chain_id: Option<ChainId>,
        /// Optional minimum votes threshold to filter validators.
        #[arg(long)]
        min_votes: Option<u64>,
    },

    /// Add a new validator to the committee.
    Add {
        /// Validator public key.
        #[arg(long)]
        public_key: ValidatorPublicKey,
        /// Account public key for the validator.
        #[arg(long)]
        account_key: AccountPublicKey,
        /// Network address of the validator.
        #[arg(long)]
        address: String,
        /// Voting weight for the validator.
        #[arg(long, default_value = "1")]
        votes: u64,
        /// Skip online connectivity check.
        #[arg(long)]
        skip_online_check: bool,
    },

    /// Remove a validator from the committee.
    Remove {
        /// Public key of validator to remove.
        #[arg(long)]
        public_key: ValidatorPublicKey,
    },

    /// Batch update validators from a JSON file.
    BatchUpdate {
        /// Path to JSON file with batch operations.
        file: String,
        /// Perform validation only without executing operations.
        #[arg(long)]
        dry_run: bool,
        /// Skip online connectivity checks for new validators.
        #[arg(long)]
        skip_online_check: bool,
    },

    /// Synchronize validator configuration from network address.
    Sync {
        /// Network address of the validator to sync with.
        address: String,
        /// Optional list of chain IDs to sync (defaults to default chain).
        #[arg(long)]
        chains: Vec<ChainId>,
        /// Check that validator is online before syncing (default: false).
        #[arg(long)]
        check_online: bool,
    },
}

/// Parse a batch operations file.
fn parse_batch_file(path: &Path) -> Result<ValidatorBatchFile> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read batch file: {}", path.display()))?;
    let batch: ValidatorBatchFile = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse batch file: {}", path.display()))?;
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

/// Validate a validator specification.
fn validate_validator_spec(spec: &ValidatorSpec) -> Result<()> {
    // Validate votes
    if spec.votes == 0 {
        bail!("Validator votes must be greater than 0");
    }

    // Validate network address is not empty
    if spec.network_address.is_empty() {
        bail!("Validator network address cannot be empty");
    }

    Ok(())
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

        QueryBatch { file, chain_id } => {
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

        BatchUpdate {
            file,
            dry_run,
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
            handle_batch_update(context, file, dry_run, skip_online_check).await
        }

        Sync { address, chains, check_online } => {
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
    println!("Querying {} validators about chain {chain_id}.\n", batch.validators.len());
    
    let node_provider = context.make_node_provider();
    let mut has_errors = false;
    
    for spec in batch.validators {
        let node = node_provider.make_node(&spec.network_address)?;
        let results = context
            .query_validator(&spec.network_address, &node, chain_id, Some(&spec.public_key))
            .await;
        
        if !results.errors().is_empty() {
            has_errors = true;
            for error in results.errors() {
                error!("Validator {}: {}", spec.public_key, error);
            }
        }
        
        results.print(Some(&spec.public_key), Some(&spec.network_address), None, None);
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
    context: MutexedContext<S, W, Si>,
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
    validate_validator_spec(&spec)?;
    
    // Check validator is online if requested
    let mut context = context.lock().await;
    if !skip_online_check {
        let node = context.make_node_provider().make_node(&address)?;
        context.check_compatible_version_info(&address, &node).await?;
        context.check_matching_network_description(&address, &node).await?;
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
    context: MutexedContext<S, W, Si>,
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
    context: MutexedContext<S, W, Si>,
    file: String,
    dry_run: bool,
    skip_online_check: bool,
) -> Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
    W: Persist<Target = Wallet>,
    Si: Signer + Send + Sync + 'static,
{
    info!("Starting batch update operation");
    let time_start = std::time::Instant::now();

    // Parse the batch file
    let batch = parse_batch_file(Path::new(&file))?;

    // Validate all specs
    for spec in batch.add.iter().chain(batch.modify.iter()) {
        validate_validator_spec(spec)?;
    }

    info!("Batch file parsed successfully:");
    info!("  {} validators to add", batch.add.len());
    info!("  {} validators to modify", batch.modify.len());
    info!("  {} validators to remove", batch.remove.len());

    if dry_run {
        info!("DRY RUN - No changes will be applied");
        info!("Add operations:");
        for spec in &batch.add {
            info!("  + {} @ {} ({} votes)", spec.public_key, spec.network_address, spec.votes);
        }
        info!("Modify operations:");
        for spec in &batch.modify {
            info!("  * {} @ {} ({} votes)", spec.public_key, spec.network_address, spec.votes);
        }
        info!("Remove operations:");
        for key in &batch.remove {
            info!("  - {}", key);
        }
        return Ok(());
    }

    // Check all validators are online if requested
    if !skip_online_check {
        let context_guard = context.lock().await;
        let node_provider = context_guard.make_node_provider();

        info!("Checking validators are online...");
        for spec in batch.add.iter().chain(batch.modify.iter()) {
            let node = node_provider.make_node(&spec.network_address)?;
            context_guard.check_compatible_version_info(&spec.network_address, &node).await?;
            context_guard.check_matching_network_description(&spec.network_address, &node).await?;
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

                // Apply add operations FIRST
                for spec in &batch.add {
                    if validators.contains_key(&spec.public_key) {
                        warn!("Validator {} already exists; skipping add", spec.public_key);
                        continue;
                    }
                    validators.insert(
                        spec.public_key,
                        ValidatorState {
                            network_address: spec.network_address.clone(),
                            votes: spec.votes,
                            account_public_key: spec.account_key,
                        },
                    );
                    info!("Added validator {} @ {} ({} votes)",
                        spec.public_key, spec.network_address, spec.votes);
                }

                // Apply modify operations SECOND
                for spec in &batch.modify {
                    if !validators.contains_key(&spec.public_key) {
                        warn!("Validator {} does not exist; skipping modify", spec.public_key);
                        continue;
                    }
                    validators.insert(
                        spec.public_key,
                        ValidatorState {
                            network_address: spec.network_address.clone(),
                            votes: spec.votes,
                            account_public_key: spec.account_key,
                        },
                    );
                    info!("Modified validator {} @ {} ({} votes)",
                        spec.public_key, spec.network_address, spec.votes);
                }

                // Apply remove operations LAST
                for public_key in &batch.remove {
                    if validators.remove(public_key).is_none() {
                        warn!("Validator {} does not exist; skipping remove", public_key);
                    } else {
                        info!("Removed validator {}", public_key);
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
    context: MutexedContext<S, W, Si>,
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
        context.check_compatible_version_info(&address, &node).await?;
        context.check_matching_network_description(&address, &node).await?;
    }

    // If no chains specified, use all chains from wallet
    let chains_to_sync = if chains.is_empty() {
        context.wallet().chain_ids()
    } else {
        chains
    };

    info!("Syncing {} chains to validator {}", chains_to_sync.len(), address);

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
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_validate_validator_spec_valid() {
        let spec = ValidatorSpec {
            public_key: ValidatorPublicKey::test_key(0),
            account_key: AccountPublicKey::test_key(0),
            network_address: "grpcs://validator.example.com:443".to_string(),
            votes: 100,
        };

        assert!(validate_validator_spec(&spec).is_ok());
    }

    #[test]
    fn test_validate_validator_spec_zero_votes() {
        let spec = ValidatorSpec {
            public_key: ValidatorPublicKey::test_key(0),
            account_key: AccountPublicKey::test_key(0),
            network_address: "grpcs://validator.example.com:443".to_string(),
            votes: 0,
        };

        let result = validate_validator_spec(&spec);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("votes must be greater than 0"));
    }

    #[test]
    fn test_validate_validator_spec_empty_address() {
        let spec = ValidatorSpec {
            public_key: ValidatorPublicKey::test_key(0),
            account_key: AccountPublicKey::test_key(0),
            network_address: String::new(),
            votes: 100,
        };

        let result = validate_validator_spec(&spec);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("network address cannot be empty"));
    }

    #[test]
    fn test_parse_batch_file_valid() {
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

        let batch = ValidatorBatchFile {
            add: vec![spec1],
            modify: vec![spec2],
            remove: vec![ValidatorPublicKey::test_key(2)],
        };

        let json = serde_json::to_string(&batch).unwrap();

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = parse_batch_file(temp_file.path());
        assert!(result.is_ok(), "Failed to parse batch file: {:?}", result.err());

        let parsed_batch = result.unwrap();
        assert_eq!(parsed_batch.add.len(), 1);
        assert_eq!(parsed_batch.modify.len(), 1);
        assert_eq!(parsed_batch.remove.len(), 1);
        assert_eq!(parsed_batch.add[0].votes, 100);
        assert_eq!(parsed_batch.modify[0].votes, 150);
    }

    #[test]
    fn test_parse_batch_file_empty() {
        let json = r#"{}"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = parse_batch_file(temp_file.path());
        assert!(result.is_ok());

        let batch = result.unwrap();
        assert_eq!(batch.add.len(), 0);
        assert_eq!(batch.modify.len(), 0);
        assert_eq!(batch.remove.len(), 0);
    }

    #[test]
    fn test_parse_batch_file_invalid_json() {
        let json = "{ invalid json }";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = parse_batch_file(temp_file.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_batch_file_nonexistent() {
        let result = parse_batch_file(Path::new("/nonexistent/file.json"));
        assert!(result.is_err());
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
        assert!(result.is_ok(), "Failed to parse query batch file: {:?}", result.err());

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
