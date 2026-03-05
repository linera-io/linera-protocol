# Command-Line Help for `linera`

This document contains the help content for the `linera` command-line program.

**Command Overview:**

* [`linera`↴](#linera)
* [`linera transfer`↴](#linera-transfer)
* [`linera open-chain`↴](#linera-open-chain)
* [`linera open-multi-owner-chain`↴](#linera-open-multi-owner-chain)
* [`linera show-ownership`↴](#linera-show-ownership)
* [`linera change-ownership`↴](#linera-change-ownership)
* [`linera set-preferred-owner`↴](#linera-set-preferred-owner)
* [`linera change-application-permissions`↴](#linera-change-application-permissions)
* [`linera close-chain`↴](#linera-close-chain)
* [`linera show-network-description`↴](#linera-show-network-description)
* [`linera local-balance`↴](#linera-local-balance)
* [`linera query-balance`↴](#linera-query-balance)
* [`linera sync-balance`↴](#linera-sync-balance)
* [`linera sync`↴](#linera-sync)
* [`linera process-inbox`↴](#linera-process-inbox)
* [`linera revoke-epochs`↴](#linera-revoke-epochs)
* [`linera resource-control-policy`↴](#linera-resource-control-policy)
* [`linera benchmark`↴](#linera-benchmark)
* [`linera benchmark single`↴](#linera-benchmark-single)
* [`linera benchmark multi`↴](#linera-benchmark-multi)
* [`linera create-genesis-config`↴](#linera-create-genesis-config)
* [`linera watch`↴](#linera-watch)
* [`linera service`↴](#linera-service)
* [`linera faucet`↴](#linera-faucet)
* [`linera publish-module`↴](#linera-publish-module)
* [`linera list-events-from-index`↴](#linera-list-events-from-index)
* [`linera publish-data-blob`↴](#linera-publish-data-blob)
* [`linera read-data-blob`↴](#linera-read-data-blob)
* [`linera create-application`↴](#linera-create-application)
* [`linera publish-and-create`↴](#linera-publish-and-create)
* [`linera keygen`↴](#linera-keygen)
* [`linera assign`↴](#linera-assign)
* [`linera retry-pending-block`↴](#linera-retry-pending-block)
* [`linera wallet`↴](#linera-wallet)
* [`linera wallet show`↴](#linera-wallet-show)
* [`linera wallet set-default`↴](#linera-wallet-set-default)
* [`linera wallet init`↴](#linera-wallet-init)
* [`linera wallet request-chain`↴](#linera-wallet-request-chain)
* [`linera wallet export-genesis`↴](#linera-wallet-export-genesis)
* [`linera wallet follow-chain`↴](#linera-wallet-follow-chain)
* [`linera wallet forget-keys`↴](#linera-wallet-forget-keys)
* [`linera wallet forget-chain`↴](#linera-wallet-forget-chain)
* [`linera chain`↴](#linera-chain)
* [`linera chain show-block`↴](#linera-chain-show-block)
* [`linera chain show-chain-description`↴](#linera-chain-show-chain-description)
* [`linera project`↴](#linera-project)
* [`linera project new`↴](#linera-project-new)
* [`linera project test`↴](#linera-project-test)
* [`linera project publish-and-create`↴](#linera-project-publish-and-create)
* [`linera net`↴](#linera-net)
* [`linera net up`↴](#linera-net-up)
* [`linera net helper`↴](#linera-net-helper)
* [`linera validator`↴](#linera-validator)
* [`linera validator add`↴](#linera-validator-add)
* [`linera validator batch-query`↴](#linera-validator-batch-query)
* [`linera validator update`↴](#linera-validator-update)
* [`linera validator list`↴](#linera-validator-list)
* [`linera validator query`↴](#linera-validator-query)
* [`linera validator remove`↴](#linera-validator-remove)
* [`linera validator sync`↴](#linera-validator-sync)
* [`linera storage`↴](#linera-storage)
* [`linera storage delete-all`↴](#linera-storage-delete-all)
* [`linera storage delete-namespace`↴](#linera-storage-delete-namespace)
* [`linera storage check-existence`↴](#linera-storage-check-existence)
* [`linera storage initialize`↴](#linera-storage-initialize)
* [`linera storage list-namespaces`↴](#linera-storage-list-namespaces)
* [`linera storage list-blob-ids`↴](#linera-storage-list-blob-ids)
* [`linera storage list-chain-ids`↴](#linera-storage-list-chain-ids)
* [`linera completion`↴](#linera-completion)

## `linera`

Client implementation and command-line tool for the Linera blockchain

**Usage:** `linera [OPTIONS] <COMMAND>`

###### **Subcommands:**

* `transfer` — Transfer funds
* `open-chain` — Open (i.e. activate) a new chain deriving the UID from an existing one
* `open-multi-owner-chain` — Open (i.e. activate) a new multi-owner chain deriving the UID from an existing one
* `show-ownership` — Display who owns the chain, and how the owners work together proposing blocks
* `change-ownership` — Change who owns the chain, and how the owners work together proposing blocks
* `set-preferred-owner` — Change the preferred owner of a chain
* `change-application-permissions` — Changes the application permissions configuration
* `close-chain` — Close an existing chain
* `show-network-description` — Print out the network description
* `local-balance` — Read the current native-token balance of the given account directly from the local state
* `query-balance` — Simulate the execution of one block made of pending messages from the local inbox, then read the native-token balance of the account from the local state
* `sync-balance` — (DEPRECATED) Synchronize the local state of the chain with a quorum validators, then query the local balance
* `sync` — Synchronize the local state of the chain with a quorum validators
* `process-inbox` — Process all pending incoming messages from the inbox of the given chain by creating as many blocks as needed to execute all (non-failing) messages. Failing messages will be marked as rejected and may bounce to their sender depending on their configuration
* `revoke-epochs` — Deprecates all committees up to and including the specified one
* `resource-control-policy` — View or update the resource control policy
* `benchmark` — Run benchmarks to test network performance
* `create-genesis-config` — Create genesis configuration for a Linera deployment. Create initial user chains and print information to be used for initialization of validator setup. This will also create an initial wallet for the owner of the initial "root" chains
* `watch` — Watch the network for notifications
* `service` — Run a GraphQL service to explore and extend the chains of the wallet
* `faucet` — Run a GraphQL service that exposes a faucet where users can claim tokens. This gives away the chain's tokens, and is mainly intended for testing
* `publish-module` — Publish module
* `list-events-from-index` — Print events from a specific chain and stream from a specified index
* `publish-data-blob` — Publish a data blob of binary data
* `read-data-blob` — Verify that a data blob is readable
* `create-application` — Create an application
* `publish-and-create` — Create an application, and publish the required module
* `keygen` — Create an unassigned key pair
* `assign` — Link the owner to the chain. Expects that the caller has a private key corresponding to the `public_key`, otherwise block proposals will fail when signing with it
* `retry-pending-block` — Retry a block we unsuccessfully tried to propose earlier
* `wallet` — Show the contents of the wallet
* `chain` — Show the information about a chain
* `project` — Manage Linera projects
* `net` — Manage a local Linera Network
* `validator` — Manage validators in the committee
* `storage` — Operation on the storage
* `completion` — Generate shell completion scripts

###### **Options:**

* `--send-timeout-ms <SEND_TIMEOUT>` — Timeout for sending queries (milliseconds)

  Default value: `4000`
* `--recv-timeout-ms <RECV_TIMEOUT>` — Timeout for receiving responses (milliseconds)

  Default value: `4000`
* `--max-pending-message-bundles <MAX_PENDING_MESSAGE_BUNDLES>` — The maximum number of incoming message bundles to include in a block proposal

  Default value: `300`
* `--max-block-limit-errors <MAX_BLOCK_LIMIT_ERRORS>` — Maximum number of message bundles to discard from a block proposal due to block limit errors before discarding all remaining bundles.

   Discarded bundles can be retried in the next block.

  Default value: `3`
* `--max-new-events-per-block <MAX_NEW_EVENTS_PER_BLOCK>` — The maximum number of new stream events to include in a block proposal

  Default value: `10`
* `--staging-bundles-time-budget-ms <STAGING_BUNDLES_TIME_BUDGET>` — Time budget for staging message bundles in milliseconds. When set, limits bundle execution by wall-clock time, in addition to the count limit from `max_pending_message_bundles`
* `--chain-worker-ttl-ms <CHAIN_WORKER_TTL>` — The duration in milliseconds after which an idle chain worker will free its memory

  Default value: `30000`
* `--sender-chain-worker-ttl-ms <SENDER_CHAIN_WORKER_TTL>` — The duration, in milliseconds, after which an idle sender chain worker will free its memory

  Default value: `1000`
* `--retry-delay-ms <RETRY_DELAY>` — Delay increment for retrying to connect to a validator

  Default value: `1000`
* `--max-retries <MAX_RETRIES>` — Number of times to retry connecting to a validator

  Default value: `10`
* `--max-backoff-ms <MAX_BACKOFF>` — Maximum backoff delay for retrying to connect to a validator

  Default value: `30000`
* `--wait-for-outgoing-messages` — Whether to wait until a quorum of validators has confirmed that all sent cross-chain messages have been delivered
* `--allow-fast-blocks` — Whether to allow creating blocks in the fast round. Fast blocks have lower latency but must be used carefully so that there are never any conflicting fast block proposals
* `--long-lived-services` — (EXPERIMENTAL) Whether application services can persist in some cases between queries
* `--blanket-message-policy <BLANKET_MESSAGE_POLICY>` — The policy for handling incoming messages

  Default value: `accept`

  Possible values:
  - `accept`:
    Automatically accept all incoming messages. Reject them only if execution fails
  - `reject`:
    Automatically reject tracked messages, ignore or skip untracked messages, but accept protected ones
  - `ignore`:
    Don't include any messages in blocks, and don't make any decision whether to accept or reject

* `--restrict-chain-ids-to <RESTRICT_CHAIN_IDS_TO>` — A set of chains to restrict incoming messages from. By default, messages from all chains are accepted. To reject messages from all chains, specify an empty string
* `--reject-message-bundles-without-application-ids <REJECT_MESSAGE_BUNDLES_WITHOUT_APPLICATION_IDS>` — A set of application IDs. If specified, only bundles with at least one message from one of these applications will be accepted
* `--reject-message-bundles-with-other-application-ids <REJECT_MESSAGE_BUNDLES_WITH_OTHER_APPLICATION_IDS>` — A set of application IDs. If specified, only bundles where all messages are from one of these applications will be accepted
* `--timings` — Enable timing reports during operations
* `--timing-interval <TIMING_INTERVAL>` — Interval in seconds between timing reports (defaults to 5)

  Default value: `5`
* `--quorum-grace-period <QUORUM_GRACE_PERIOD>` — An additional delay, after reaching a quorum, to wait for additional validator signatures, as a fraction of time taken to reach quorum

  Default value: `0.2`
* `--blob-download-timeout-ms <BLOB_DOWNLOAD_TIMEOUT>` — The delay when downloading a blob, after which we try a second validator, in milliseconds

  Default value: `1000`
* `--cert-batch-download-timeout-ms <CERTIFICATE_BATCH_DOWNLOAD_TIMEOUT>` — The delay when downloading a batch of certificates, after which we try a second validator, in milliseconds

  Default value: `1000`
* `--certificate-download-batch-size <CERTIFICATE_DOWNLOAD_BATCH_SIZE>` — Maximum number of certificates that we download at a time from one validator when synchronizing one of our chains

  Default value: `500`
* `--sender-certificate-download-batch-size <SENDER_CERTIFICATE_DOWNLOAD_BATCH_SIZE>` — Maximum number of sender certificates we try to download and receive in one go when syncing sender chains

  Default value: `20000`
* `--max-joined-tasks <MAX_JOINED_TASKS>` — Maximum number of tasks that can are joined concurrently in the client

  Default value: `100`
* `--max-accepted-latency-ms <MAX_ACCEPTED_LATENCY_MS>` — Maximum expected latency in milliseconds for score normalization

  Default value: `5000`
* `--cache-ttl-ms <CACHE_TTL_MS>` — Time-to-live for cached responses in milliseconds

  Default value: `2000`
* `--cache-max-size <CACHE_MAX_SIZE>` — Maximum number of entries in the cache

  Default value: `1000`
* `--max-request-ttl-ms <MAX_REQUEST_TTL_MS>` — Maximum latency for an in-flight request before we stop deduplicating it (in milliseconds)

  Default value: `200`
* `--alpha <ALPHA>` — Smoothing factor for Exponential Moving Averages (0 < alpha < 1). Higher values give more weight to recent observations. Typical values are between 0.01 and 0.5. A value of 0.1 means that 10% of the new observation is considered and 90% of the previous average is retained

  Default value: `0.1`
* `--alternative-peers-retry-delay-ms <ALTERNATIVE_PEERS_RETRY_DELAY_MS>` — Delay in milliseconds between starting requests to different peers. This helps to stagger requests and avoid overwhelming the network

  Default value: `150`
* `--listener-skip-process-inbox` — Do not create blocks automatically to receive incoming messages. Instead, wait for an explicit mutation `processInbox`
* `--listener-delay-before-ms <DELAY_BEFORE_MS>` — Wait before processing any notification (useful for testing)

  Default value: `0`
* `--listener-delay-after-ms <DELAY_AFTER_MS>` — Wait after processing any notification (useful for rate limiting)

  Default value: `0`
* `--wallet <WALLET_STATE_PATH>` — Sets the file storing the private state of user chains (an empty one will be created if missing)
* `--keystore <KEYSTORE_PATH>` — Sets the file storing the keystore state
* `-w`, `--with-wallet <WITH_WALLET>` — Given an ASCII alphanumeric parameter `X`, read the wallet state and the wallet storage config from the environment variables `LINERA_WALLET_{X}` and `LINERA_STORAGE_{X}` instead of `LINERA_WALLET` and `LINERA_STORAGE`
* `--storage <STORAGE_CONFIG>` — Storage configuration for the blockchain history
* `--storage-max-concurrent-queries <STORAGE_MAX_CONCURRENT_QUERIES>` — The maximal number of simultaneous queries to the database
* `--storage-max-stream-queries <STORAGE_MAX_STREAM_QUERIES>` — The maximal number of simultaneous stream queries to the database

  Default value: `10`
* `--storage-max-cache-size <STORAGE_MAX_CACHE_SIZE>` — The maximal memory used in the storage cache

  Default value: `10000000`
* `--storage-max-value-entry-size <STORAGE_MAX_VALUE_ENTRY_SIZE>` — The maximal size of a value entry in the storage cache

  Default value: `1000000`
* `--storage-max-find-keys-entry-size <STORAGE_MAX_FIND_KEYS_ENTRY_SIZE>` — The maximal size of a find-keys entry in the storage cache

  Default value: `1000000`
* `--storage-max-find-key-values-entry-size <STORAGE_MAX_FIND_KEY_VALUES_ENTRY_SIZE>` — The maximal size of a find-key-values entry in the storage cache

  Default value: `1000000`
* `--storage-max-cache-entries <STORAGE_MAX_CACHE_ENTRIES>` — The maximal number of entries in the storage cache

  Default value: `1000`
* `--storage-max-cache-value-size <STORAGE_MAX_CACHE_VALUE_SIZE>` — The maximal memory used in the value cache

  Default value: `10000000`
* `--storage-max-cache-find-keys-size <STORAGE_MAX_CACHE_FIND_KEYS_SIZE>` — The maximal memory used in the find_keys_by_prefix cache

  Default value: `10000000`
* `--storage-max-cache-find-key-values-size <STORAGE_MAX_CACHE_FIND_KEY_VALUES_SIZE>` — The maximal memory used in the find_key_values_by_prefix cache

  Default value: `10000000`
* `--storage-replication-factor <STORAGE_REPLICATION_FACTOR>` — The replication factor for the keyspace

  Default value: `1`
* `--wasm-runtime <WASM_RUNTIME>` — The WebAssembly runtime to use
* `--with-application-logs` — Output log messages from contract execution
* `--tokio-threads <TOKIO_THREADS>` — The number of Tokio worker threads to use
* `--tokio-blocking-threads <TOKIO_BLOCKING_THREADS>` — The number of Tokio blocking threads to use
* `--chrome-trace-exporter` — Enable OpenTelemetry Chrome JSON exporter for trace data analysis
* `--chrome-trace-file <CHROME_TRACE_FILE>` — Output file path for Chrome trace JSON format. Can be visualized in chrome://tracing or Perfetto UI
* `--otlp-exporter-endpoint <OTLP_EXPORTER_ENDPOINT>` — OpenTelemetry OTLP exporter endpoint (requires opentelemetry feature)



## `linera transfer`

Transfer funds

**Usage:** `linera transfer --from <SENDER> --to <RECIPIENT> <AMOUNT>`

###### **Arguments:**

* `<AMOUNT>` — Amount to transfer

###### **Options:**

* `--from <SENDER>` — Sending chain ID (must be one of our chains)
* `--to <RECIPIENT>` — Recipient account



## `linera open-chain`

Open (i.e. activate) a new chain deriving the UID from an existing one

**Usage:** `linera open-chain [OPTIONS]`

###### **Options:**

* `--from <CHAIN_ID>` — Chain ID (must be one of our chains)
* `--owner <OWNER>` — The new owner (otherwise create a key pair and remember it)
* `--initial-balance <BALANCE>` — The initial balance of the new chain. This is subtracted from the parent chain's balance

  Default value: `0`
* `--super-owner` — Whether to create a super owner for the new chain



## `linera open-multi-owner-chain`

Open (i.e. activate) a new multi-owner chain deriving the UID from an existing one

**Usage:** `linera open-multi-owner-chain [OPTIONS]`

###### **Options:**

* `--from <CHAIN_ID>` — Chain ID (must be one of our chains)
* `--super-owners <SUPER_OWNERS>` — A JSON list of the new super owners. Absence of the option leaves the current set of super owners unchanged
* `--owners <OWNERS>` — A JSON map of the new owners to their weights. Absence of the option leaves the current set of owners unchanged
* `--multi-leader-rounds <MULTI_LEADER_ROUNDS>` — The number of rounds in which every owner can propose blocks, i.e. the first round number in which only a single designated leader is allowed to propose blocks. "null" is equivalent to 2^32 - 1. Absence of the option leaves the current setting unchanged
* `--open-multi-leader-rounds` — Whether the multi-leader rounds are unrestricted, i.e. not limited to chain owners. This should only be `true` on chains with restrictive application permissions and an application-based mechanism to select block proposers
* `--fast-round-ms <FAST_ROUND_DURATION>` — The duration of the fast round, in milliseconds. "null" means the fast round will not time out. Absence of the option leaves the current setting unchanged
* `--base-timeout-ms <BASE_TIMEOUT>` — The duration of the first single-leader and all multi-leader rounds. Absence of the option leaves the current setting unchanged
* `--timeout-increment-ms <TIMEOUT_INCREMENT>` — The number of milliseconds by which the timeout increases after each single-leader round. Absence of the option leaves the current setting unchanged
* `--fallback-duration-ms <FALLBACK_DURATION>` — The age of an incoming tracked or protected message after which the validators start transitioning the chain to fallback mode, in milliseconds. Absence of the option leaves the current setting unchanged
* `--execute-operations <EXECUTE_OPERATIONS>` — A JSON list of applications allowed to execute operations on this chain. If set to null, all operations will be allowed. Otherwise, only operations from the specified applications are allowed, and no system operations. Absence of the option leaves current permissions unchanged
* `--mandatory-applications <MANDATORY_APPLICATIONS>` — A JSON list of applications, such that at least one operation or incoming message from each of these applications must occur in every block. Absence of the option leaves current mandatory applications unchanged
* `--close-chain <CLOSE_CHAIN>` — A JSON list of applications allowed to close the chain. Absence of the option leaves the current list unchanged
* `--change-application-permissions <CHANGE_APPLICATION_PERMISSIONS>` — A JSON list of applications allowed to change the application permissions on the current chain using the system API. Absence of the option leaves the current list unchanged
* `--call-service-as-oracle <CALL_SERVICE_AS_ORACLE>` — A JSON list of applications that are allowed to call services as oracles on the current chain using the system API. If set to null, all applications will be able to do so. Absence of the option leaves the current value of the setting unchanged
* `--make-http-requests <MAKE_HTTP_REQUESTS>` — A JSON list of applications that are allowed to make HTTP requests on the current chain using the system API. If set to null, all applications will be able to do so. Absence of the option leaves the current value of the setting unchanged
* `--initial-balance <BALANCE>` — The initial balance of the new chain. This is subtracted from the parent chain's balance

  Default value: `0`



## `linera show-ownership`

Display who owns the chain, and how the owners work together proposing blocks

**Usage:** `linera show-ownership [OPTIONS]`

###### **Options:**

* `--chain-id <CHAIN_ID>` — The ID of the chain whose owners will be changed



## `linera change-ownership`

Change who owns the chain, and how the owners work together proposing blocks.

Specify the complete set of new owners, by public key. Existing owners that are not included will be removed.

**Usage:** `linera change-ownership [OPTIONS]`

###### **Options:**

* `--chain-id <CHAIN_ID>` — The ID of the chain whose owners will be changed
* `--super-owners <SUPER_OWNERS>` — A JSON list of the new super owners. Absence of the option leaves the current set of super owners unchanged
* `--owners <OWNERS>` — A JSON map of the new owners to their weights. Absence of the option leaves the current set of owners unchanged
* `--multi-leader-rounds <MULTI_LEADER_ROUNDS>` — The number of rounds in which every owner can propose blocks, i.e. the first round number in which only a single designated leader is allowed to propose blocks. "null" is equivalent to 2^32 - 1. Absence of the option leaves the current setting unchanged
* `--open-multi-leader-rounds` — Whether the multi-leader rounds are unrestricted, i.e. not limited to chain owners. This should only be `true` on chains with restrictive application permissions and an application-based mechanism to select block proposers
* `--fast-round-ms <FAST_ROUND_DURATION>` — The duration of the fast round, in milliseconds. "null" means the fast round will not time out. Absence of the option leaves the current setting unchanged
* `--base-timeout-ms <BASE_TIMEOUT>` — The duration of the first single-leader and all multi-leader rounds. Absence of the option leaves the current setting unchanged
* `--timeout-increment-ms <TIMEOUT_INCREMENT>` — The number of milliseconds by which the timeout increases after each single-leader round. Absence of the option leaves the current setting unchanged
* `--fallback-duration-ms <FALLBACK_DURATION>` — The age of an incoming tracked or protected message after which the validators start transitioning the chain to fallback mode, in milliseconds. Absence of the option leaves the current setting unchanged



## `linera set-preferred-owner`

Change the preferred owner of a chain

**Usage:** `linera set-preferred-owner [OPTIONS] --owner <OWNER>`

###### **Options:**

* `--chain-id <CHAIN_ID>` — The ID of the chain whose preferred owner will be changed
* `--owner <OWNER>` — The new preferred owner



## `linera change-application-permissions`

Changes the application permissions configuration

**Usage:** `linera change-application-permissions [OPTIONS]`

###### **Options:**

* `--chain-id <CHAIN_ID>` — The ID of the chain to which the new permissions will be applied
* `--execute-operations <EXECUTE_OPERATIONS>` — A JSON list of applications allowed to execute operations on this chain. If set to null, all operations will be allowed. Otherwise, only operations from the specified applications are allowed, and no system operations. Absence of the option leaves current permissions unchanged
* `--mandatory-applications <MANDATORY_APPLICATIONS>` — A JSON list of applications, such that at least one operation or incoming message from each of these applications must occur in every block. Absence of the option leaves current mandatory applications unchanged
* `--close-chain <CLOSE_CHAIN>` — A JSON list of applications allowed to close the chain. Absence of the option leaves the current list unchanged
* `--change-application-permissions <CHANGE_APPLICATION_PERMISSIONS>` — A JSON list of applications allowed to change the application permissions on the current chain using the system API. Absence of the option leaves the current list unchanged
* `--call-service-as-oracle <CALL_SERVICE_AS_ORACLE>` — A JSON list of applications that are allowed to call services as oracles on the current chain using the system API. If set to null, all applications will be able to do so. Absence of the option leaves the current value of the setting unchanged
* `--make-http-requests <MAKE_HTTP_REQUESTS>` — A JSON list of applications that are allowed to make HTTP requests on the current chain using the system API. If set to null, all applications will be able to do so. Absence of the option leaves the current value of the setting unchanged



## `linera close-chain`

Close an existing chain.

A closed chain cannot execute operations or accept messages anymore. It can still reject incoming messages, so they bounce back to the sender.

**Usage:** `linera close-chain <CHAIN_ID>`

###### **Arguments:**

* `<CHAIN_ID>` — Chain ID (must be one of our chains)



## `linera show-network-description`

Print out the network description

**Usage:** `linera show-network-description`



## `linera local-balance`

Read the current native-token balance of the given account directly from the local state.

NOTE: The local balance does not reflect messages that are waiting to be picked in the local inbox, or that have not been synchronized from validators yet. Use `linera sync` then either `linera query-balance` or `linera process-inbox && linera local-balance` for a consolidated balance.

**Usage:** `linera local-balance [ACCOUNT]`

###### **Arguments:**

* `<ACCOUNT>` — The account to read, written as `OWNER@CHAIN-ID` or simply `CHAIN-ID` for the chain balance. By default, we read the chain balance of the default chain in the wallet



## `linera query-balance`

Simulate the execution of one block made of pending messages from the local inbox, then read the native-token balance of the account from the local state.

NOTE: The balance does not reflect messages that have not been synchronized from validators yet. Call `linera sync` first to do so.

**Usage:** `linera query-balance [ACCOUNT]`

###### **Arguments:**

* `<ACCOUNT>` — The account to query, written as `OWNER@CHAIN-ID` or simply `CHAIN-ID` for the chain balance. By default, we read the chain balance of the default chain in the wallet



## `linera sync-balance`

(DEPRECATED) Synchronize the local state of the chain with a quorum validators, then query the local balance.

This command is deprecated. Use `linera sync && linera query-balance` instead.

**Usage:** `linera sync-balance [ACCOUNT]`

###### **Arguments:**

* `<ACCOUNT>` — The account to query, written as `OWNER@CHAIN-ID` or simply `CHAIN-ID` for the chain balance. By default, we read the chain balance of the default chain in the wallet



## `linera sync`

Synchronize the local state of the chain with a quorum validators

**Usage:** `linera sync [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — The chain to synchronize with validators. If omitted, synchronizes the default chain of the wallet



## `linera process-inbox`

Process all pending incoming messages from the inbox of the given chain by creating as many blocks as needed to execute all (non-failing) messages. Failing messages will be marked as rejected and may bounce to their sender depending on their configuration

**Usage:** `linera process-inbox [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — The chain to process. If omitted, uses the default chain of the wallet



## `linera revoke-epochs`

Deprecates all committees up to and including the specified one

**Usage:** `linera revoke-epochs <EPOCH>`

###### **Arguments:**

* `<EPOCH>`



## `linera resource-control-policy`

View or update the resource control policy

**Usage:** `linera resource-control-policy [OPTIONS]`

###### **Options:**

* `--wasm-fuel-unit <WASM_FUEL_UNIT>` — Set the price per unit of Wasm fuel
* `--evm-fuel-unit <EVM_FUEL_UNIT>` — Set the price per unit of EVM fuel
* `--read-operation <READ_OPERATION>` — Set the price per read operation
* `--write-operation <WRITE_OPERATION>` — Set the price per write operation
* `--byte-runtime <BYTE_RUNTIME>` — Set the price per byte read from runtime
* `--byte-read <BYTE_READ>` — Set the price per byte read
* `--byte-written <BYTE_WRITTEN>` — Set the price per byte written
* `--blob-read <BLOB_READ>` — Set the base price to read a blob
* `--blob-published <BLOB_PUBLISHED>` — Set the base price to publish a blob
* `--blob-byte-read <BLOB_BYTE_READ>` — Set the price to read a blob, per byte
* `--blob-byte-published <BLOB_BYTE_PUBLISHED>` — The price to publish a blob, per byte
* `--byte-stored <BYTE_STORED>` — Set the price per byte stored
* `--operation <OPERATION>` — Set the base price of sending an operation from a block..
* `--operation-byte <OPERATION_BYTE>` — Set the additional price for each byte in the argument of a user operation
* `--message <MESSAGE>` — Set the base price of sending a message from a block..
* `--message-byte <MESSAGE_BYTE>` — Set the additional price for each byte in the argument of a user message
* `--service-as-oracle-query <SERVICE_AS_ORACLE_QUERY>` — Set the price per query to a service as an oracle
* `--http-request <HTTP_REQUEST>` — Set the price for performing an HTTP request
* `--maximum-wasm-fuel-per-block <MAXIMUM_WASM_FUEL_PER_BLOCK>` — Set the maximum amount of Wasm fuel per block
* `--maximum-evm-fuel-per-block <MAXIMUM_EVM_FUEL_PER_BLOCK>` — Set the maximum amount of EVM fuel per block
* `--maximum-service-oracle-execution-ms <MAXIMUM_SERVICE_ORACLE_EXECUTION_MS>` — Set the maximum time in milliseconds that a block can spend executing services as oracles
* `--maximum-block-size <MAXIMUM_BLOCK_SIZE>` — Set the maximum size of a block, in bytes
* `--maximum-blob-size <MAXIMUM_BLOB_SIZE>` — Set the maximum size of data blobs, compressed bytecode and other binary blobs, in bytes
* `--maximum-published-blobs <MAXIMUM_PUBLISHED_BLOBS>` — Set the maximum number of published blobs per block
* `--maximum-bytecode-size <MAXIMUM_BYTECODE_SIZE>` — Set the maximum size of decompressed contract or service bytecode, in bytes
* `--maximum-block-proposal-size <MAXIMUM_BLOCK_PROPOSAL_SIZE>` — Set the maximum size of a block proposal, in bytes
* `--maximum-bytes-read-per-block <MAXIMUM_BYTES_READ_PER_BLOCK>` — Set the maximum read data per block
* `--maximum-bytes-written-per-block <MAXIMUM_BYTES_WRITTEN_PER_BLOCK>` — Set the maximum write data per block
* `--maximum-oracle-response-bytes <MAXIMUM_ORACLE_RESPONSE_BYTES>` — Set the maximum size of oracle responses
* `--maximum-http-response-bytes <MAXIMUM_HTTP_RESPONSE_BYTES>` — Set the maximum size in bytes of a received HTTP response
* `--http-request-timeout-ms <HTTP_REQUEST_TIMEOUT_MS>` — Set the maximum amount of time allowed to wait for an HTTP response
* `--http-request-allow-list <HTTP_REQUEST_ALLOW_LIST>` — Set the list of hosts that contracts and services can send HTTP requests to.

   Besides hostnames, the following special flags are recognized:

   - `FLAG_ZERO_HASH.linera.network`: Skip hashing of the execution state (return all zeros instead). - `FLAG_FREE_REJECT.linera.network`: Make bouncing messages free of charge. - `FLAG_MANDATORY_APPS_NEED_ACCEPTED_MESSAGE.linera.network`: Require accepted (not rejected) incoming messages to satisfy mandatory application checks. - `FLAG_FREE_APPLICATION_ID_<APP_ID>.linera.network`: Waive all message- and event-related fees for the given application ID (see also `--free-application-ids`).
* `--free-application-ids <FREE_APPLICATION_IDS>` — Set the list of application IDs for which message- and event-related fees are waived.

   This is a convenience flag that adds `FLAG_FREE_APPLICATION_ID_<APP_ID>.linera.network` entries to the HTTP request allow list.



## `linera benchmark`

Run benchmarks to test network performance

**Usage:** `linera benchmark <COMMAND>`

###### **Subcommands:**

* `single` — Start a single benchmark process, maintaining a given TPS
* `multi` — Run multiple benchmark processes in parallel



## `linera benchmark single`

Start a single benchmark process, maintaining a given TPS

**Usage:** `linera benchmark single [OPTIONS]`

###### **Options:**

* `--num-chains <NUM_CHAINS>` — How many chains to use

  Default value: `10`
* `--tokens-per-chain <TOKENS_PER_CHAIN>` — How many tokens to assign to each newly created chain. These need to cover the transaction fees per chain for the benchmark

  Default value: `0.1`
* `--transactions-per-block <TRANSACTIONS_PER_BLOCK>` — How many transactions to put in each block

  Default value: `1`
* `--fungible-application-id <FUNGIBLE_APPLICATION_ID>` — The application ID of a fungible token on the wallet's default chain. If none is specified, the benchmark uses the native token
* `--bps <BPS>` — The fixed BPS (Blocks Per Second) rate that block proposals will be sent at

  Default value: `10`
* `--close-chains` — If provided, will close the chains after the benchmark is finished. Keep in mind that closing the chains might take a while, and will increase the validator latency while they're being closed
* `--health-check-endpoints <HEALTH_CHECK_ENDPOINTS>` — A comma-separated list of host:port pairs to query for health metrics. If provided, the benchmark will check these endpoints for validator health and terminate if any validator is unhealthy. Example: "127.0.0.1:21100,validator-1.some-network.linera.net:21100"
* `--wrap-up-max-in-flight <WRAP_UP_MAX_IN_FLIGHT>` — The maximum number of in-flight requests to validators when wrapping up the benchmark. While wrapping up, this controls the concurrency level when processing inboxes and closing chains

  Default value: `5`
* `--confirm-before-start` — Confirm before starting the benchmark
* `--runtime-in-seconds <RUNTIME_IN_SECONDS>` — How long to run the benchmark for. If not provided, the benchmark will run until it is interrupted
* `--delay-between-chains-ms <DELAY_BETWEEN_CHAINS_MS>` — The delay between chains, in milliseconds. For example, if set to 200ms, the first chain will start, then the second will start 200 ms after the first one, the third 200 ms after the second one, and so on. This is used for slowly ramping up the TPS, so we don't pound the validators with the full TPS all at once
* `--config-path <CONFIG_PATH>` — Path to YAML file containing chain IDs to send transfers to. If not provided, only transfers between chains in the same wallet
* `--single-destination-per-block` — Transaction distribution mode. If false (default), distributes transactions evenly across chains within each block. If true, sends all transactions in each block to a single chain, rotating through chains for subsequent blocks



## `linera benchmark multi`

Run multiple benchmark processes in parallel

**Usage:** `linera benchmark multi [OPTIONS] --faucet <FAUCET>`

###### **Options:**

* `--num-chains <NUM_CHAINS>` — How many chains to use

  Default value: `10`
* `--tokens-per-chain <TOKENS_PER_CHAIN>` — How many tokens to assign to each newly created chain. These need to cover the transaction fees per chain for the benchmark

  Default value: `0.1`
* `--transactions-per-block <TRANSACTIONS_PER_BLOCK>` — How many transactions to put in each block

  Default value: `1`
* `--fungible-application-id <FUNGIBLE_APPLICATION_ID>` — The application ID of a fungible token on the wallet's default chain. If none is specified, the benchmark uses the native token
* `--bps <BPS>` — The fixed BPS (Blocks Per Second) rate that block proposals will be sent at

  Default value: `10`
* `--close-chains` — If provided, will close the chains after the benchmark is finished. Keep in mind that closing the chains might take a while, and will increase the validator latency while they're being closed
* `--health-check-endpoints <HEALTH_CHECK_ENDPOINTS>` — A comma-separated list of host:port pairs to query for health metrics. If provided, the benchmark will check these endpoints for validator health and terminate if any validator is unhealthy. Example: "127.0.0.1:21100,validator-1.some-network.linera.net:21100"
* `--wrap-up-max-in-flight <WRAP_UP_MAX_IN_FLIGHT>` — The maximum number of in-flight requests to validators when wrapping up the benchmark. While wrapping up, this controls the concurrency level when processing inboxes and closing chains

  Default value: `5`
* `--confirm-before-start` — Confirm before starting the benchmark
* `--runtime-in-seconds <RUNTIME_IN_SECONDS>` — How long to run the benchmark for. If not provided, the benchmark will run until it is interrupted
* `--delay-between-chains-ms <DELAY_BETWEEN_CHAINS_MS>` — The delay between chains, in milliseconds. For example, if set to 200ms, the first chain will start, then the second will start 200 ms after the first one, the third 200 ms after the second one, and so on. This is used for slowly ramping up the TPS, so we don't pound the validators with the full TPS all at once
* `--config-path <CONFIG_PATH>` — Path to YAML file containing chain IDs to send transfers to. If not provided, only transfers between chains in the same wallet
* `--single-destination-per-block` — Transaction distribution mode. If false (default), distributes transactions evenly across chains within each block. If true, sends all transactions in each block to a single chain, rotating through chains for subsequent blocks
* `--processes <PROCESSES>` — The number of benchmark processes to run in parallel

  Default value: `1`
* `--faucet <FAUCET>` — The faucet (which implicitly defines the network)
* `--client-state-dir <CLIENT_STATE_DIR>` — If specified, a directory with a random name will be created in this directory, and the client state will be stored there. If not specified, a temporary directory will be used for each client
* `--delay-between-processes <DELAY_BETWEEN_PROCESSES>` — The delay between starting the benchmark processes, in seconds. If --cross-wallet-transfers is true, this will be ignored

  Default value: `10`
* `--cross-wallet-transfers` — Whether to send transfers between chains in different wallets



## `linera create-genesis-config`

Create genesis configuration for a Linera deployment. Create initial user chains and print information to be used for initialization of validator setup. This will also create an initial wallet for the owner of the initial "root" chains

**Usage:** `linera create-genesis-config [OPTIONS] --committee <COMMITTEE_CONFIG_PATH> --genesis <GENESIS_CONFIG_PATH> <NUM_OTHER_INITIAL_CHAINS>`

###### **Arguments:**

* `<NUM_OTHER_INITIAL_CHAINS>` — Number of initial (aka "root") chains to create in addition to the admin chain

###### **Options:**

* `--committee <COMMITTEE_CONFIG_PATH>` — Sets the file describing the public configurations of all validators
* `--genesis <GENESIS_CONFIG_PATH>` — The output config path to be consumed by the server
* `--initial-funding <INITIAL_FUNDING>` — Known initial balance of the chain

  Default value: `0`
* `--start-timestamp <START_TIMESTAMP>` — The start timestamp: no blocks can be created before this time
* `--policy-config <POLICY_CONFIG>` — Configure the resource control policy (notably fees) according to pre-defined settings

  Default value: `no-fees`

  Possible values: `no-fees`, `testnet`

* `--wasm-fuel-unit-price <WASM_FUEL_UNIT_PRICE>` — Set the price per unit of Wasm fuel. (This will overwrite value from `--policy-config`)
* `--evm-fuel-unit-price <EVM_FUEL_UNIT_PRICE>` — Set the price per unit of EVM fuel. (This will overwrite value from `--policy-config`)
* `--read-operation-price <READ_OPERATION_PRICE>` — Set the price per read operation. (This will overwrite value from `--policy-config`)
* `--write-operation-price <WRITE_OPERATION_PRICE>` — Set the price per write operation. (This will overwrite value from `--policy-config`)
* `--byte-runtime-price <BYTE_RUNTIME_PRICE>` — Set the price per byte read from runtime. (This will overwrite value from `--policy-config`)
* `--byte-read-price <BYTE_READ_PRICE>` — Set the price per byte read. (This will overwrite value from `--policy-config`)
* `--byte-written-price <BYTE_WRITTEN_PRICE>` — Set the price per byte written. (This will overwrite value from `--policy-config`)
* `--blob-read-price <BLOB_READ_PRICE>` — Set the base price to read a blob. (This will overwrite value from `--policy-config`)
* `--blob-published-price <BLOB_PUBLISHED_PRICE>` — Set the base price to publish a blob. (This will overwrite value from `--policy-config`)
* `--blob-byte-read-price <BLOB_BYTE_READ_PRICE>` — Set the price to read a blob, per byte. (This will overwrite value from `--policy-config`)
* `--blob-byte-published-price <BLOB_BYTE_PUBLISHED_PRICE>` — Set the price to publish a blob, per byte. (This will overwrite value from `--policy-config`)
* `--byte-stored-price <BYTE_STORED_PRICE>` — Set the price per byte stored. (This will overwrite value from `--policy-config`)
* `--operation-price <OPERATION_PRICE>` — Set the base price of sending an operation from a block.. (This will overwrite value from `--policy-config`)
* `--operation-byte-price <OPERATION_BYTE_PRICE>` — Set the additional price for each byte in the argument of a user operation. (This will overwrite value from `--policy-config`)
* `--message-price <MESSAGE_PRICE>` — Set the base price of sending a message from a block.. (This will overwrite value from `--policy-config`)
* `--message-byte-price <MESSAGE_BYTE_PRICE>` — Set the additional price for each byte in the argument of a user message. (This will overwrite value from `--policy-config`)
* `--service-as-oracle-query-price <SERVICE_AS_ORACLE_QUERY_PRICE>` — Set the price per query to a service as an oracle
* `--http-request-price <HTTP_REQUEST_PRICE>` — Set the price for performing an HTTP request
* `--maximum-wasm-fuel-per-block <MAXIMUM_WASM_FUEL_PER_BLOCK>` — Set the maximum amount of Wasm fuel per block. (This will overwrite value from `--policy-config`)
* `--maximum-evm-fuel-per-block <MAXIMUM_EVM_FUEL_PER_BLOCK>` — Set the maximum amount of EVM fuel per block. (This will overwrite value from `--policy-config`)
* `--maximum-service-oracle-execution-ms <MAXIMUM_SERVICE_ORACLE_EXECUTION_MS>` — Set the maximum time in milliseconds that a block can spend executing services as oracles
* `--maximum-block-size <MAXIMUM_BLOCK_SIZE>` — Set the maximum size of a block. (This will overwrite value from `--policy-config`)
* `--maximum-bytecode-size <MAXIMUM_BYTECODE_SIZE>` — Set the maximum size of decompressed contract or service bytecode, in bytes. (This will overwrite value from `--policy-config`)
* `--maximum-blob-size <MAXIMUM_BLOB_SIZE>` — Set the maximum size of data blobs, compressed bytecode and other binary blobs, in bytes. (This will overwrite value from `--policy-config`)
* `--maximum-published-blobs <MAXIMUM_PUBLISHED_BLOBS>` — Set the maximum number of published blobs per block. (This will overwrite value from `--policy-config`)
* `--maximum-block-proposal-size <MAXIMUM_BLOCK_PROPOSAL_SIZE>` — Set the maximum size of a block proposal, in bytes. (This will overwrite value from `--policy-config`)
* `--maximum-bytes-read-per-block <MAXIMUM_BYTES_READ_PER_BLOCK>` — Set the maximum read data per block. (This will overwrite value from `--policy-config`)
* `--maximum-bytes-written-per-block <MAXIMUM_BYTES_WRITTEN_PER_BLOCK>` — Set the maximum write data per block. (This will overwrite value from `--policy-config`)
* `--maximum-oracle-response-bytes <MAXIMUM_ORACLE_RESPONSE_BYTES>` — Set the maximum size of oracle responses. (This will overwrite value from `--policy-config`)
* `--maximum-http-response-bytes <MAXIMUM_HTTP_RESPONSE_BYTES>` — Set the maximum size in bytes of a received HTTP response
* `--http-request-timeout-ms <HTTP_REQUEST_TIMEOUT_MS>` — Set the maximum amount of time allowed to wait for an HTTP response
* `--http-request-allow-list <HTTP_REQUEST_ALLOW_LIST>` — Set the list of hosts that contracts and services can send HTTP requests to.

   Besides hostnames, the following special flags are recognized:

   - `FLAG_ZERO_HASH.linera.network`: Skip hashing of the execution state (return all zeros instead). - `FLAG_FREE_REJECT.linera.network`: Make bouncing messages free of charge. - `FLAG_MANDATORY_APPS_NEED_ACCEPTED_MESSAGE.linera.network`: Require accepted (not rejected) incoming messages to satisfy mandatory application checks. - `FLAG_FREE_APPLICATION_ID_<APP_ID>.linera.network`: Waive all message- and event-related fees for the given application ID (see also `--free-application-ids`).
* `--free-application-ids <FREE_APPLICATION_IDS>` — Set the list of application IDs for which message- and event-related fees are waived.

   This is a convenience flag that adds `FLAG_FREE_APPLICATION_ID_<APP_ID>.linera.network` entries to the HTTP request allow list.
* `--testing-prng-seed <TESTING_PRNG_SEED>` — Force this wallet to generate keys using a PRNG and a given seed. USE FOR TESTING ONLY
* `--network-name <NETWORK_NAME>` — A unique name to identify this network



## `linera watch`

Watch the network for notifications

**Usage:** `linera watch [OPTIONS] [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — The chain ID to watch

###### **Options:**

* `--raw` — Show all notifications from all validators



## `linera service`

Run a GraphQL service to explore and extend the chains of the wallet

**Usage:** `linera service [OPTIONS] --port <PORT>`

###### **Options:**

* `--listener-skip-process-inbox` — Do not create blocks automatically to receive incoming messages. Instead, wait for an explicit mutation `processInbox`
* `--listener-delay-before-ms <DELAY_BEFORE_MS>` — Wait before processing any notification (useful for testing)

  Default value: `0`
* `--listener-delay-after-ms <DELAY_AFTER_MS>` — Wait after processing any notification (useful for rate limiting)

  Default value: `0`
* `--port <PORT>` — The port on which to run the server
* `--operator-application-ids <OPERATOR_APPLICATION_IDS>` — Application IDs of operator applications to watch. When specified, a task processor is started alongside the node service
* `--controller-id <CONTROLLER_APPLICATION_ID>` — A controller to execute a dynamic set of applications running on a dynamic set of chains
* `--operators <OPERATORS>` — Supported operators and their binary paths. Format: `name=path` or just `name` (uses name as path). Example: `--operators my-operator=/path/to/binary`
* `--task-retry-delay-secs <TASK_RETRY_DELAY_SECS>` — Delay in seconds before retrying a failed operator task batch. Only relevant when operators are configured via `--operator-application-ids` or `--controller-id`

  Default value: `5`
* `--read-only` — Run in read-only mode: disallow mutations and prevent queries from scheduling operations. Use this when exposing the service to untrusted clients
* `--query-cache-size <QUERY_CACHE_SIZE>` — Enable the application query response cache with the given per-chain capacity. Each entry stores a serialized GraphQL response keyed by (application_id, request_bytes). Incompatible with `--long-lived-services`
* `--allow-subscription <ALLOWED_SUBSCRIPTIONS>` — Allow a named GraphQL subscription query. The operation name is extracted from the query string. Repeatable. Example: `--allow-subscription 'query CounterValue { getCounter { value } }'`



## `linera faucet`

Run a GraphQL service that exposes a faucet where users can claim tokens. This gives away the chain's tokens, and is mainly intended for testing

**Usage:** `linera faucet [OPTIONS] --amount <AMOUNT> --storage-path <STORAGE_PATH> [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — The chain that gives away its tokens

###### **Options:**

* `--port <PORT>` — The port on which to run the server

  Default value: `8080`
* `--amount <AMOUNT>` — The number of tokens to send to each new chain
* `--limit-rate-until <LIMIT_RATE_UNTIL>` — The end timestamp: The faucet will rate-limit the token supply so it runs out of money no earlier than this
* `--listener-skip-process-inbox` — Do not create blocks automatically to receive incoming messages. Instead, wait for an explicit mutation `processInbox`
* `--listener-delay-before-ms <DELAY_BEFORE_MS>` — Wait before processing any notification (useful for testing)

  Default value: `0`
* `--listener-delay-after-ms <DELAY_AFTER_MS>` — Wait after processing any notification (useful for rate limiting)

  Default value: `0`
* `--storage-path <STORAGE_PATH>` — Path to the persistent storage file for faucet mappings
* `--max-batch-size <MAX_BATCH_SIZE>` — Maximum number of operations to include in a single block (default: 100)

  Default value: `100`



## `linera publish-module`

Publish module

**Usage:** `linera publish-module [OPTIONS] <CONTRACT> <SERVICE> [PUBLISHER]`

###### **Arguments:**

* `<CONTRACT>` — Path to the Wasm file for the application "contract" bytecode
* `<SERVICE>` — Path to the Wasm file for the application "service" bytecode
* `<PUBLISHER>` — An optional chain ID to publish the module. The default chain of the wallet is used otherwise

###### **Options:**

* `--vm-runtime <VM_RUNTIME>` — The virtual machine runtime to use

  Default value: `wasm`



## `linera list-events-from-index`

Print events from a specific chain and stream from a specified index

**Usage:** `linera list-events-from-index [OPTIONS] --stream-id <STREAM_ID> [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — The chain to query. If omitted, query the default chain of the wallet

###### **Options:**

* `--stream-id <STREAM_ID>` — The stream being considered
* `--start-index <START_INDEX>` — Index of the message to start with

  Default value: `0`



## `linera publish-data-blob`

Publish a data blob of binary data

**Usage:** `linera publish-data-blob <BLOB_PATH> [PUBLISHER]`

###### **Arguments:**

* `<BLOB_PATH>` — Path to data blob file to be published
* `<PUBLISHER>` — An optional chain ID to publish the blob. The default chain of the wallet is used otherwise



## `linera read-data-blob`

Verify that a data blob is readable

**Usage:** `linera read-data-blob <HASH> [READER]`

###### **Arguments:**

* `<HASH>` — The hash of the content
* `<READER>` — An optional chain ID to verify the blob. The default chain of the wallet is used otherwise



## `linera create-application`

Create an application

**Usage:** `linera create-application [OPTIONS] <MODULE_ID> [CREATOR]`

###### **Arguments:**

* `<MODULE_ID>` — The module ID of the application to create
* `<CREATOR>` — An optional chain ID to host the application. The default chain of the wallet is used otherwise

###### **Options:**

* `--json-parameters <JSON_PARAMETERS>` — The shared parameters as JSON string
* `--json-parameters-path <JSON_PARAMETERS_PATH>` — Path to a JSON file containing the shared parameters
* `--json-argument <JSON_ARGUMENT>` — The instantiation argument as a JSON string
* `--json-argument-path <JSON_ARGUMENT_PATH>` — Path to a JSON file containing the instantiation argument
* `--required-application-ids <REQUIRED_APPLICATION_IDS>` — The list of required dependencies of application, if any



## `linera publish-and-create`

Create an application, and publish the required module

**Usage:** `linera publish-and-create [OPTIONS] <CONTRACT> <SERVICE> [PUBLISHER]`

###### **Arguments:**

* `<CONTRACT>` — Path to the Wasm file for the application "contract" bytecode
* `<SERVICE>` — Path to the Wasm file for the application "service" bytecode
* `<PUBLISHER>` — An optional chain ID to publish the module. The default chain of the wallet is used otherwise

###### **Options:**

* `--vm-runtime <VM_RUNTIME>` — The virtual machine runtime to use

  Default value: `wasm`
* `--json-parameters <JSON_PARAMETERS>` — The shared parameters as JSON string
* `--json-parameters-path <JSON_PARAMETERS_PATH>` — Path to a JSON file containing the shared parameters
* `--json-argument <JSON_ARGUMENT>` — The instantiation argument as a JSON string
* `--json-argument-path <JSON_ARGUMENT_PATH>` — Path to a JSON file containing the instantiation argument
* `--required-application-ids <REQUIRED_APPLICATION_IDS>` — The list of required dependencies of application, if any



## `linera keygen`

Create an unassigned key pair

**Usage:** `linera keygen`



## `linera assign`

Link the owner to the chain. Expects that the caller has a private key corresponding to the `public_key`, otherwise block proposals will fail when signing with it

**Usage:** `linera assign --owner <OWNER> --chain-id <CHAIN_ID>`

###### **Options:**

* `--owner <OWNER>` — The owner to assign
* `--chain-id <CHAIN_ID>` — The ID of the chain



## `linera retry-pending-block`

Retry a block we unsuccessfully tried to propose earlier.

As long as a block is pending most other commands will fail, since it is unsafe to propose multiple blocks at the same height.

**Usage:** `linera retry-pending-block [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — The chain with the pending block. If not specified, the wallet's default chain is used



## `linera wallet`

Show the contents of the wallet

**Usage:** `linera wallet <COMMAND>`

###### **Subcommands:**

* `show` — Show the contents of the wallet
* `set-default` — Change the wallet default chain
* `init` — Initialize a wallet from the genesis configuration
* `request-chain` — Request a new chain from a faucet and add it to the wallet
* `export-genesis` — Export the genesis configuration to a JSON file
* `follow-chain` — Add a new followed chain (i.e. a chain without keypair) to the wallet
* `forget-keys` — Forgets the specified chain's keys. The chain will still be followed by the wallet
* `forget-chain` — Forgets the specified chain, including the associated key pair



## `linera wallet show`

Show the contents of the wallet

**Usage:** `linera wallet show [OPTIONS] [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — The chain to show the metadata

###### **Options:**

* `--short` — Only print a non-formatted list of the wallet's chain IDs
* `--owned` — Print only the chains that we have a key pair for



## `linera wallet set-default`

Change the wallet default chain

**Usage:** `linera wallet set-default <CHAIN_ID>`

###### **Arguments:**

* `<CHAIN_ID>`



## `linera wallet init`

Initialize a wallet from the genesis configuration

**Usage:** `linera wallet init [OPTIONS]`

###### **Options:**

* `--genesis <GENESIS_CONFIG_PATH>` — The path to the genesis configuration for a Linera deployment. Either this or `--faucet` must be specified
* `--faucet <FAUCET>` — The address of a faucet
* `--testing-prng-seed <TESTING_PRNG_SEED>` — Force this wallet to generate keys using a PRNG and a given seed. USE FOR TESTING ONLY



## `linera wallet request-chain`

Request a new chain from a faucet and add it to the wallet

**Usage:** `linera wallet request-chain [OPTIONS] --faucet <FAUCET>`

###### **Options:**

* `--faucet <FAUCET>` — The address of a faucet
* `--set-default` — Whether this chain should become the default chain



## `linera wallet export-genesis`

Export the genesis configuration to a JSON file.

By default, exports the genesis config from the current wallet. Alternatively, use `--faucet` to retrieve the genesis config directly from a faucet URL.

**Usage:** `linera wallet export-genesis [OPTIONS] <OUTPUT>`

###### **Arguments:**

* `<OUTPUT>` — Path to save the genesis configuration JSON file

###### **Options:**

* `--faucet <FAUCET>` — The address of a faucet to retrieve the genesis config from. If not specified, the genesis config is read from the current wallet



## `linera wallet follow-chain`

Add a new followed chain (i.e. a chain without keypair) to the wallet

**Usage:** `linera wallet follow-chain [OPTIONS] <CHAIN_ID>`

###### **Arguments:**

* `<CHAIN_ID>` — The chain ID

###### **Options:**

* `--sync` — Synchronize the new chain and download all its blocks from the validators



## `linera wallet forget-keys`

Forgets the specified chain's keys. The chain will still be followed by the wallet

**Usage:** `linera wallet forget-keys <CHAIN_ID>`

###### **Arguments:**

* `<CHAIN_ID>`



## `linera wallet forget-chain`

Forgets the specified chain, including the associated key pair

**Usage:** `linera wallet forget-chain <CHAIN_ID>`

###### **Arguments:**

* `<CHAIN_ID>`



## `linera chain`

Show the information about a chain

**Usage:** `linera chain <COMMAND>`

###### **Subcommands:**

* `show-block` — Show the contents of a block
* `show-chain-description` — Show the chain description of a chain



## `linera chain show-block`

Show the contents of a block

**Usage:** `linera chain show-block <HEIGHT> [CHAIN_ID]`

###### **Arguments:**

* `<HEIGHT>` — The height of the block
* `<CHAIN_ID>` — The chain to show the block (if not specified, the default chain from the wallet is used)



## `linera chain show-chain-description`

Show the chain description of a chain

**Usage:** `linera chain show-chain-description [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — The chain ID to show (if not specified, the default chain from the wallet is used)



## `linera project`

Manage Linera projects

**Usage:** `linera project <COMMAND>`

###### **Subcommands:**

* `new` — Create a new Linera project
* `test` — Test a Linera project
* `publish-and-create` — Build and publish a Linera project



## `linera project new`

Create a new Linera project

**Usage:** `linera project new [OPTIONS] <NAME>`

###### **Arguments:**

* `<NAME>` — The project name. A directory of the same name will be created in the current directory

###### **Options:**

* `--linera-root <LINERA_ROOT>` — Use the given clone of the Linera repository instead of remote crates



## `linera project test`

Test a Linera project.

Equivalent to running `cargo test` with the appropriate test runner.

**Usage:** `linera project test [PATH]`

###### **Arguments:**

* `<PATH>`



## `linera project publish-and-create`

Build and publish a Linera project

**Usage:** `linera project publish-and-create [OPTIONS] [PATH] [NAME] [PUBLISHER]`

###### **Arguments:**

* `<PATH>` — The path of the root of the Linera project. Defaults to current working directory if unspecified
* `<NAME>` — Specify the name of the Linera project. This is used to locate the generated bytecode files. The generated bytecode files should be of the form `<name>_{contract,service}.wasm`.

   Defaults to the package name in Cargo.toml, with dashes replaced by underscores.
* `<PUBLISHER>` — An optional chain ID to publish the module. The default chain of the wallet is used otherwise

###### **Options:**

* `--vm-runtime <VM_RUNTIME>` — The virtual machine runtime to use

  Default value: `wasm`
* `--json-parameters <JSON_PARAMETERS>` — The shared parameters as JSON string
* `--json-parameters-path <JSON_PARAMETERS_PATH>` — Path to a JSON file containing the shared parameters
* `--json-argument <JSON_ARGUMENT>` — The instantiation argument as a JSON string
* `--json-argument-path <JSON_ARGUMENT_PATH>` — Path to a JSON file containing the instantiation argument
* `--required-application-ids <REQUIRED_APPLICATION_IDS>` — The list of required dependencies of application, if any



## `linera net`

Manage a local Linera Network

**Usage:** `linera net <COMMAND>`

###### **Subcommands:**

* `up` — Start a Local Linera Network
* `helper` — Print a bash helper script to make `linera net up` easier to use. The script is meant to be installed in `~/.bash_profile` or sourced when needed



## `linera net up`

Start a Local Linera Network

**Usage:** `linera net up [OPTIONS]`

###### **Options:**

* `--other-initial-chains <OTHER_INITIAL_CHAINS>` — The number of initial "root" chains created in the genesis config on top of the default "admin" chain. All initial chains belong to the first "admin" wallet. It is recommended to use at least one other initial chain for the faucet

  Default value: `2`
* `--initial-amount <INITIAL_AMOUNT>` — The initial amount of native tokens credited in the initial "root" chains, including the default "admin" chain

  Default value: `1000000`
* `--validators <VALIDATORS>` — The number of validators in the local test network

  Default value: `1`
* `--shards <SHARDS>` — The number of shards per validator in the local test network

  Default value: `1`
* `--policy-config <POLICY_CONFIG>` — Configure the resource control policy (notably fees) according to pre-defined settings

  Default value: `no-fees`

  Possible values: `no-fees`, `testnet`

* `--cross-chain-queue-size <QUEUE_SIZE>` — Number of cross-chain messages allowed before dropping them

  Default value: `1000`
* `--cross-chain-max-retries <MAX_RETRIES>` — Maximum number of retries for a cross-chain message

  Default value: `10`
* `--cross-chain-retry-delay-ms <RETRY_DELAY_MS>` — Delay before retrying of cross-chain message

  Default value: `2000`
* `--cross-chain-max-backoff-ms <MAX_BACKOFF_MS>` — Maximum backoff delay for cross-chain message retries

  Default value: `30000`
* `--cross-chain-sender-delay-ms <SENDER_DELAY_MS>` — Introduce a delay before sending every cross-chain message (e.g. for testing purpose)

  Default value: `0`
* `--cross-chain-sender-failure-rate <SENDER_FAILURE_RATE>` — Drop cross-chain messages randomly at the given rate (0 <= rate < 1) (meant for testing)

  Default value: `0.0`
* `--testing-prng-seed <TESTING_PRNG_SEED>` — Force this wallet to generate keys using a PRNG and a given seed. USE FOR TESTING ONLY
* `--path <PATH>` — Run with a specific path where the wallet and validator input files are. If none, then a temporary directory is created
* `--external-protocol <EXTERNAL_PROTOCOL>` — External protocol used, either `grpc` or `grpcs`

  Default value: `grpc`
* `--with-faucet` — If present, a faucet is started using the chain provided by --faucet-chain, or the first non-admin chain if not provided

  Default value: `false`
* `--faucet-chain <FAUCET_CHAIN>` — When using --with-faucet, this specifies the chain on which the faucet will be started. If this is `n`, the `n`-th non-admin chain (lexicographically) in the wallet is selected
* `--faucet-port <FAUCET_PORT>` — The port on which to run the faucet server

  Default value: `8080`
* `--faucet-amount <FAUCET_AMOUNT>` — The number of tokens to send to each new chain created by the faucet

  Default value: `1000`
* `--with-block-exporter` — Whether to start a block exporter for each validator

  Default value: `false`
* `--num-block-exporters <NUM_BLOCK_EXPORTERS>` — The number of block exporters to start

  Default value: `1`
* `--exporter-address <EXPORTER_ADDRESS>` — The address of the block exporter

  Default value: `localhost`
* `--exporter-port <EXPORTER_PORT>` — The port on which to run the block exporter

  Default value: `8081`



## `linera net helper`

Print a bash helper script to make `linera net up` easier to use. The script is meant to be installed in `~/.bash_profile` or sourced when needed

**Usage:** `linera net helper`



## `linera validator`

Manage validators in the committee

**Usage:** `linera validator <COMMAND>`

###### **Subcommands:**

* `add` — Add a validator to the committee
* `batch-query` — Query multiple validators using a JSON specification file
* `update` — Apply multiple validator changes from JSON input
* `list` — List all validators in the committee
* `query` — Query a single validator's state and connectivity
* `remove` — Remove a validator from the committee
* `sync` — Synchronize chain state to a validator



## `linera validator add`

Add a validator to the committee.

Adds a new validator with the specified public key, account key, network address, and voting weight. The validator must not already exist in the committee.

**Usage:** `linera validator add [OPTIONS] --public-key <PUBLIC_KEY> --account-key <ACCOUNT_KEY> --address <ADDRESS>`

###### **Options:**

* `--public-key <PUBLIC_KEY>` — Public key of the validator to add
* `--account-key <ACCOUNT_KEY>` — Account public key for receiving payments and rewards
* `--address <ADDRESS>` — Network address where the validator can be reached (e.g., grpcs://host:port)
* `--votes <VOTES>` — Voting weight for consensus (default: 1)
* `--skip-online-check` — Skip online connectivity verification before adding



## `linera validator batch-query`

Query multiple validators using a JSON specification file.

Reads validator specifications from a JSON file and queries their state. The JSON should contain an array of validator objects with publicKey and networkAddress.

**Usage:** `linera validator batch-query [OPTIONS] <FILE>`

###### **Arguments:**

* `<FILE>` — Path to JSON file containing validator query specifications

###### **Options:**

* `--chain-id <CHAIN_ID>` — Chain ID to query (defaults to default chain)



## `linera validator update`

Apply multiple validator changes from JSON input.

Reads a JSON object mapping validator public keys to their desired state: - Key with state object (address, votes, accountKey): add or modify validator - Key with null: remove validator - Keys not present: unchanged

Input can be provided via file path, stdin pipe, or shell redirect.

**Usage:** `linera validator update [OPTIONS] [FILE]`

###### **Arguments:**

* `<FILE>` — Path to JSON file with validator changes (omit or use "-" for stdin)

###### **Options:**

* `--dry-run` — Preview changes without applying them
* `-y`, `--yes` — Skip confirmation prompt (use with caution)
* `--skip-online-check` — Skip online connectivity checks for validators being added or modified



## `linera validator list`

List all validators in the committee.

Displays the current validator set with their network addresses, voting weights, and connection status. Optionally filter by minimum voting weight.

**Usage:** `linera validator list [OPTIONS]`

###### **Options:**

* `--chain-id <CHAIN_ID>` — Chain ID to query (defaults to default chain)
* `--min-votes <MIN_VOTES>` — Only show validators with at least this many votes



## `linera validator query`

Query a single validator's state and connectivity.

Connects to a validator at the specified network address and queries its view of the blockchain state, including block height and committee information.

**Usage:** `linera validator query [OPTIONS] <ADDRESS>`

###### **Arguments:**

* `<ADDRESS>` — Network address of the validator (e.g., grpcs://host:port)

###### **Options:**

* `--chain-id <CHAIN_ID>` — Chain ID to query about (defaults to default chain)
* `--public-key <PUBLIC_KEY>` — Expected public key of the validator (for verification)



## `linera validator remove`

Remove a validator from the committee.

Removes the validator with the specified public key from the committee. The validator will no longer participate in consensus.

**Usage:** `linera validator remove --public-key <PUBLIC_KEY>`

###### **Options:**

* `--public-key <PUBLIC_KEY>` — Public key of the validator to remove



## `linera validator sync`

Synchronize chain state to a validator.

Pushes the current chain state from local storage to a validator node, ensuring the validator has up-to-date information about specified chains.

**Usage:** `linera validator sync [OPTIONS] <ADDRESS>`

###### **Arguments:**

* `<ADDRESS>` — Network address of the validator to sync (e.g., grpcs://host:port)

###### **Options:**

* `--chains <CHAINS>` — Chain IDs to synchronize (defaults to all chains in wallet)
* `--check-online` — Verify validator is online before syncing



## `linera storage`

Operation on the storage

**Usage:** `linera storage <COMMAND>`

###### **Subcommands:**

* `delete-all` — Delete all the namespaces in the database
* `delete-namespace` — Delete a single namespace from the database
* `check-existence` — Check existence of a namespace in the database
* `initialize` — Initialize a namespace in the database
* `list-namespaces` — List the namespaces in the database
* `list-blob-ids` — List the blob IDs in the database
* `list-chain-ids` — List the chain IDs in the database



## `linera storage delete-all`

Delete all the namespaces in the database

**Usage:** `linera storage delete-all`



## `linera storage delete-namespace`

Delete a single namespace from the database

**Usage:** `linera storage delete-namespace`



## `linera storage check-existence`

Check existence of a namespace in the database

**Usage:** `linera storage check-existence`



## `linera storage initialize`

Initialize a namespace in the database

**Usage:** `linera storage initialize --genesis <GENESIS_CONFIG_PATH>`

###### **Options:**

* `--genesis <GENESIS_CONFIG_PATH>`



## `linera storage list-namespaces`

List the namespaces in the database

**Usage:** `linera storage list-namespaces`



## `linera storage list-blob-ids`

List the blob IDs in the database

**Usage:** `linera storage list-blob-ids`



## `linera storage list-chain-ids`

List the chain IDs in the database

**Usage:** `linera storage list-chain-ids`



## `linera completion`

Generate shell completion scripts

**Usage:** `linera completion <SHELL>`

###### **Arguments:**

* `<SHELL>` — The shell to generate completions for

  Possible values: `bash`, `elvish`, `fish`, `powershell`, `zsh`




<hr/>

<small><i>
    This document was generated automatically by
    <a href="https://crates.io/crates/clap-markdown"><code>clap-markdown</code></a>.
</i></small>

