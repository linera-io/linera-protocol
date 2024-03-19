# Command-Line Help for `linera`

This document contains the help content for the `linera` command-line program.

**Command Overview:**

* [`linera`↴](#linera)
* [`linera transfer`↴](#linera-transfer)
* [`linera open-chain`↴](#linera-open-chain)
* [`linera subscribe`↴](#linera-subscribe)
* [`linera unsubscribe`↴](#linera-unsubscribe)
* [`linera open-multi-owner-chain`↴](#linera-open-multi-owner-chain)
* [`linera change-ownership`↴](#linera-change-ownership)
* [`linera change-application-permissions`↴](#linera-change-application-permissions)
* [`linera close-chain`↴](#linera-close-chain)
* [`linera local-balance`↴](#linera-local-balance)
* [`linera query-balance`↴](#linera-query-balance)
* [`linera sync-balance`↴](#linera-sync-balance)
* [`linera sync`↴](#linera-sync)
* [`linera process-inbox`↴](#linera-process-inbox)
* [`linera query-validators`↴](#linera-query-validators)
* [`linera set-validator`↴](#linera-set-validator)
* [`linera remove-validator`↴](#linera-remove-validator)
* [`linera resource-control-policy`↴](#linera-resource-control-policy)
* [`linera create-genesis-config`↴](#linera-create-genesis-config)
* [`linera watch`↴](#linera-watch)
* [`linera service`↴](#linera-service)
* [`linera faucet`↴](#linera-faucet)
* [`linera publish-bytecode`↴](#linera-publish-bytecode)
* [`linera create-application`↴](#linera-create-application)
* [`linera publish-and-create`↴](#linera-publish-and-create)
* [`linera request-application`↴](#linera-request-application)
* [`linera keygen`↴](#linera-keygen)
* [`linera assign`↴](#linera-assign)
* [`linera retry-pending-block`↴](#linera-retry-pending-block)
* [`linera wallet`↴](#linera-wallet)
* [`linera wallet show`↴](#linera-wallet-show)
* [`linera wallet set-default`↴](#linera-wallet-set-default)
* [`linera wallet init`↴](#linera-wallet-init)
* [`linera project`↴](#linera-project)
* [`linera project new`↴](#linera-project-new)
* [`linera project test`↴](#linera-project-test)
* [`linera project publish-and-create`↴](#linera-project-publish-and-create)
* [`linera net`↴](#linera-net)
* [`linera net up`↴](#linera-net-up)
* [`linera net helper`↴](#linera-net-helper)

## `linera`

A Byzantine-fault tolerant sidechain with low-latency finality and high throughput

**Usage:** `linera [OPTIONS] <COMMAND>`

###### **Subcommands:**

* `transfer` — Transfer funds
* `open-chain` — Open (i.e. activate) a new chain deriving the UID from an existing one
* `subscribe` — Subscribe to a system channel
* `unsubscribe` — Unsubscribe from a system channel
* `open-multi-owner-chain` — Open (i.e. activate) a new multi-owner chain deriving the UID from an existing one
* `change-ownership` — Change who owns the chain, and how the owners work together proposing blocks
* `change-application-permissions` — Changes the application permissions configuration
* `close-chain` — Close an existing chain
* `local-balance` — Read the current native-token balance of the given account directly from the local state
* `query-balance` — Simulate the execution of one block made of pending messages from the local inbox, then read the native-token balance of the account from the local state
* `sync-balance` — (DEPRECATED) Synchronize the local state of the chain with a quorum validators, then query the local balance
* `sync` — Synchronize the local state of the chain with a quorum validators
* `process-inbox` — Process all pending incoming messages from the inbox of the given chain by creating as many blocks as needed to execute all (non-failing) messages. Failing messages will be marked as rejected and may bounce to their sender depending on their configuration
* `query-validators` — Show the current set of validators for a chain
* `set-validator` — Add or modify a validator (admin only)
* `remove-validator` — Remove a validator (admin only)
* `resource-control-policy` — View or update the resource control policy
* `create-genesis-config` — Create genesis configuration for a Linera deployment. Create initial user chains and print information to be used for initialization of validator setup. This will also create an initial wallet for the owner of the initial "root" chains
* `watch` — Watch the network for notifications
* `service` — Run a GraphQL service to explore and extend the chains of the wallet
* `faucet` — Run a GraphQL service that exposes a faucet where users can claim tokens. This gives away the chain's tokens, and is mainly intended for testing
* `publish-bytecode` — Publish bytecode
* `create-application` — Create an application
* `publish-and-create` — Create an application, and publish the required bytecode
* `request-application` — Request an application from another chain, so it can be used on this one
* `keygen` — Create an unassigned key-pair
* `assign` — Link a key owned by the wallet to a chain that was just created for that key
* `retry-pending-block` — Retry a block we unsuccessfully tried to propose earlier
* `wallet` — Show the contents of the wallet
* `project` — Manage Linera projects
* `net` — Manage a local Linera Network

###### **Options:**

* `--wallet <WALLET_STATE_PATH>` — Sets the file storing the private state of user chains (an empty one will be created if missing)
* `--storage <STORAGE_CONFIG>` — Storage configuration for the blockchain history
* `-w`, `--with-wallet <WITH_WALLET>` — Given an integer value N, read the wallet state and the wallet storage config from the environment variables LINERA_WALLET_{N} and LINERA_STORAGE_{N} instead of LINERA_WALLET and LINERA_STORAGE
* `--send-timeout-ms <SEND_TIMEOUT>` — Timeout for sending queries (milliseconds)

  Default value: `4000`
* `--recv-timeout-ms <RECV_TIMEOUT>` — Timeout for receiving responses (milliseconds)

  Default value: `4000`
* `--max-pending-messages <MAX_PENDING_MESSAGES>`

  Default value: `10`
* `--wasm-runtime <WASM_RUNTIME>` — The WebAssembly runtime to use
* `--max-concurrent-queries <MAX_CONCURRENT_QUERIES>` — The maximal number of simultaneous queries to the database
* `--max-stream-queries <MAX_STREAM_QUERIES>` — The maximal number of simultaneous stream queries to the database

  Default value: `10`
* `--cache-size <CACHE_SIZE>` — The maximal number of entries in the storage cache

  Default value: `1000`
* `--notification-retry-delay-ms <NOTIFICATION_RETRY_DELAY>` — Delay increment for retrying to connect to a validator for notifications

  Default value: `1000`
* `--notification-retries <NOTIFICATION_RETRIES>` — Number of times to retry connecting to a validator for notifications

  Default value: `10`
* `--wait-for-outgoing-messages` — Whether to wait until a quorum of validators has confirmed that all sent cross-chain messages have been delivered
* `--tokio-threads <TOKIO_THREADS>` — The number of Tokio worker threads to use



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
* `--to-public-key <PUBLIC_KEY>` — Public key of the new owner (otherwise create a key pair and remember it)
* `--initial-balance <BALANCE>` — The initial balance of the new chain. This is subtracted from the parent chain's balance

  Default value: `0`



## `linera subscribe`

Subscribe to a system channel

**Usage:** `linera subscribe [OPTIONS] --channel <CHANNEL>`

###### **Options:**

* `--subscriber <SUBSCRIBER>` — Chain ID (must be one of our chains)
* `--publisher <PUBLISHER>` — Chain ID
* `--channel <CHANNEL>` — System channel available in the system application

  Possible values:
  - `admin`:
    Channel used to broadcast reconfigurations
  - `published-bytecodes`:
    Channel used to broadcast new published bytecodes




## `linera unsubscribe`

Unsubscribe from a system channel

**Usage:** `linera unsubscribe [OPTIONS] --channel <CHANNEL>`

###### **Options:**

* `--subscriber <SUBSCRIBER>` — Chain ID (must be one of our chains)
* `--publisher <PUBLISHER>` — Chain ID
* `--channel <CHANNEL>` — System channel available in the system application

  Possible values:
  - `admin`:
    Channel used to broadcast reconfigurations
  - `published-bytecodes`:
    Channel used to broadcast new published bytecodes




## `linera open-multi-owner-chain`

Open (i.e. activate) a new multi-owner chain deriving the UID from an existing one

**Usage:** `linera open-multi-owner-chain [OPTIONS]`

###### **Options:**

* `--from <CHAIN_ID>` — Chain ID (must be one of our chains)
* `--super-owner-public-keys <SUPER_OWNER_PUBLIC_KEYS>` — Public keys of the new super owners
* `--owner-public-keys <OWNER_PUBLIC_KEYS>` — Public keys of the new regular owners
* `--owner-weights <OWNER_WEIGHTS>` — Weights for the new owners
* `--multi-leader-rounds <MULTI_LEADER_ROUNDS>` — The number of rounds in which every owner can propose blocks, i.e. the first round number in which only a single designated leader is allowed to propose blocks
* `--fast-round-ms <FAST_ROUND_DURATION>` — The duration of the fast round, in milliseconds
* `--base-timeout-ms <BASE_TIMEOUT>` — The duration of the first single-leader and all multi-leader rounds

  Default value: `10000`
* `--timeout-increment-ms <TIMEOUT_INCREMENT>` — The number of milliseconds by which the timeout increases after each single-leader round

  Default value: `1000`
* `--initial-balance <BALANCE>` — The initial balance of the new chain. This is subtracted from the parent chain's balance

  Default value: `0`



## `linera change-ownership`

Change who owns the chain, and how the owners work together proposing blocks.

Specify the complete set of new owners, by public key. Existing owners that are not included will be removed.

**Usage:** `linera change-ownership [OPTIONS]`

###### **Options:**

* `--chain-id <CHAIN_ID>` — The ID of the chain whose owners will be changed
* `--super-owner-public-keys <SUPER_OWNER_PUBLIC_KEYS>` — Public keys of the new super owners
* `--owner-public-keys <OWNER_PUBLIC_KEYS>` — Public keys of the new regular owners
* `--owner-weights <OWNER_WEIGHTS>` — Weights for the new owners
* `--multi-leader-rounds <MULTI_LEADER_ROUNDS>` — The number of rounds in which every owner can propose blocks, i.e. the first round number in which only a single designated leader is allowed to propose blocks
* `--fast-round-ms <FAST_ROUND_DURATION>` — The duration of the fast round, in milliseconds
* `--base-timeout-ms <BASE_TIMEOUT>` — The duration of the first single-leader and all multi-leader rounds

  Default value: `10000`
* `--timeout-increment-ms <TIMEOUT_INCREMENT>` — The number of milliseconds by which the timeout increases after each single-leader round

  Default value: `1000`



## `linera change-application-permissions`

Changes the application permissions configuration

**Usage:** `linera change-application-permissions [OPTIONS]`

###### **Options:**

* `--chain-id <CHAIN_ID>` — The ID of the chain to which the new permissions will be applied
* `--execute-operations <EXECUTE_OPERATIONS>` — If this is not set, all system operations and application operations are allowed. If it is set, only operations from the specified applications are allowed, and no system operations
* `--close-chain <CLOSE_CHAIN>` — These applications are allowed to close the current chain using the system API



## `linera close-chain`

Close an existing chain.

A closed chain cannot execute operations or accept messages anymore. It can still reject incoming messages, so they bounce back to the sender.

**Usage:** `linera close-chain --from <CHAIN_ID>`

###### **Options:**

* `--from <CHAIN_ID>` — Chain ID (must be one of our chains)



## `linera local-balance`

Read the current native-token balance of the given account directly from the local state.

NOTE: The local balance does not reflect messages that are waiting to be picked in the local inbox, or that have not been synchronized from validators yet. Use `linera sync` then either `linera query-balance` or `linera process-inbox && linera local-balance` for a consolidated balance.

**Usage:** `linera local-balance [ACCOUNT]`

###### **Arguments:**

* `<ACCOUNT>` — The account to read, written as `CHAIN-ID:OWNER` or simply `CHAIN-ID` for the chain balance. By defaults, we read the chain balance of the default chain in the wallet



## `linera query-balance`

Simulate the execution of one block made of pending messages from the local inbox, then read the native-token balance of the account from the local state.

NOTE: The balance does not reflect messages that have not been synchronized from validators yet. Call `linera sync` first to do so.

**Usage:** `linera query-balance [ACCOUNT]`

###### **Arguments:**

* `<ACCOUNT>` — The account to query, written as `CHAIN-ID:OWNER` or simply `CHAIN-ID` for the chain balance. By defaults, we read the chain balance of the default chain in the wallet



## `linera sync-balance`

(DEPRECATED) Synchronize the local state of the chain with a quorum validators, then query the local balance.

This command is deprecated. Use `linera sync && linera query-balance` instead.

**Usage:** `linera sync-balance [ACCOUNT]`

###### **Arguments:**

* `<ACCOUNT>` — The account to query, written as `CHAIN-ID:OWNER` or simply `CHAIN-ID` for the chain balance. By defaults, we read the chain balance of the default chain in the wallet



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



## `linera query-validators`

Show the current set of validators for a chain

**Usage:** `linera query-validators [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — The chain to query. If omitted, query the default chain of the wallet



## `linera set-validator`

Add or modify a validator (admin only)

**Usage:** `linera set-validator [OPTIONS] --name <NAME> --address <ADDRESS>`

###### **Options:**

* `--name <NAME>` — The public key of the validator
* `--address <ADDRESS>` — Network address
* `--votes <VOTES>` — Voting power

  Default value: `1`



## `linera remove-validator`

Remove a validator (admin only)

**Usage:** `linera remove-validator --name <NAME>`

###### **Options:**

* `--name <NAME>` — The public key of the validator



## `linera resource-control-policy`

View or update the resource control policy

**Usage:** `linera resource-control-policy [OPTIONS]`

###### **Options:**

* `--block <BLOCK>` — Set the base price for creating a block
* `--fuel-unit <FUEL_UNIT>` — Set the price per unit of fuel
* `--read-operation <READ_OPERATION>` — Set the price per read operation
* `--write-operation <WRITE_OPERATION>` — Set the price per write operation
* `--byte-read <BYTE_READ>` — Set the price per byte read
* `--byte-written <BYTE_WRITTEN>` — Set the price per byte written
* `--byte-stored <BYTE_STORED>` — Set the price per byte stored
* `--operation <OPERATION>` — Set the base price of sending a operation from a block..
* `--operation-byte <OPERATION_BYTE>` — Set the additional price for each byte in the argument of a user operation
* `--message <MESSAGE>` — Set the base price of sending a message from a block..
* `--message-byte <MESSAGE_BYTE>` — Set the additional price for each byte in the argument of a user message
* `--maximum-bytes-read-per-block <MAXIMUM_BYTES_READ_PER_BLOCK>` — Set the maximum read data per block
* `--maximum-bytes-written-per-block <MAXIMUM_BYTES_WRITTEN_PER_BLOCK>` — Set the maximum write data per block



## `linera create-genesis-config`

Create genesis configuration for a Linera deployment. Create initial user chains and print information to be used for initialization of validator setup. This will also create an initial wallet for the owner of the initial "root" chains

**Usage:** `linera create-genesis-config [OPTIONS] --committee <COMMITTEE_CONFIG_PATH> --genesis <GENESIS_CONFIG_PATH> <NUM_OTHER_INITIAL_CHAINS>`

###### **Arguments:**

* `<NUM_OTHER_INITIAL_CHAINS>` — Number of initial (aka "root") chains to create in addition to the admin chain

###### **Options:**

* `--committee <COMMITTEE_CONFIG_PATH>` — Sets the file describing the public configurations of all validators
* `--genesis <GENESIS_CONFIG_PATH>` — The output config path to be consumed by the server
* `--admin-root <ADMIN_ROOT>` — Index of the admin chain in the genesis config

  Default value: `0`
* `--initial-funding <INITIAL_FUNDING>` — Known initial balance of the chain

  Default value: `0`
* `--start-timestamp <START_TIMESTAMP>` — The start timestamp: no blocks can be created before this time
* `--block-price <BLOCK_PRICE>` — Set the base price for creating a block

  Default value: `0`
* `--fuel-unit-price <FUEL_UNIT_PRICE>` — Set the price per unit of fuel

  Default value: `0`
* `--read-operation-price <READ_OPERATION_PRICE>` — Set the price per read operation

  Default value: `0`
* `--write-operation-price <WRITE_OPERATION_PRICE>` — Set the price per write operation

  Default value: `0`
* `--byte-read-price <BYTE_READ_PRICE>` — Set the price per byte read

  Default value: `0`
* `--byte-written-price <BYTE_WRITTEN_PRICE>` — Set the price per byte written

  Default value: `0`
* `--byte-stored-price <BYTE_STORED_PRICE>` — Set the price per byte stored

  Default value: `0`
* `--operation-price <OPERATION_PRICE>` — Set the base price of sending a operation from a block..

  Default value: `0`
* `--operation-byte-price <OPERATION_BYTE_PRICE>` — Set the additional price for each byte in the argument of a user operation

  Default value: `0`
* `--message-price <MESSAGE_PRICE>` — Set the base price of sending a message from a block..

  Default value: `0`
* `--message-byte-price <MESSAGE_BYTE_PRICE>` — Set the additional price for each byte in the argument of a user message

  Default value: `0`
* `--maximum-bytes-read-per-block <MAXIMUM_BYTES_READ_PER_BLOCK>` — Set the maximum read data per block
* `--maximum-bytes-written-per-block <MAXIMUM_BYTES_WRITTEN_PER_BLOCK>` — Set the maximum write data per block
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

**Usage:** `linera service [OPTIONS]`

###### **Options:**

* `--listener-delay-before-ms <DELAY_BEFORE_MS>` — Wait before processing any notification (useful for testing)

  Default value: `0`
* `--listener-delay-after-ms <DELAY_AFTER_MS>` — Wait after processing any notification (useful for rate limiting)

  Default value: `0`
* `--port <PORT>` — The port on which to run the server

  Default value: `8080`



## `linera faucet`

Run a GraphQL service that exposes a faucet where users can claim tokens. This gives away the chain's tokens, and is mainly intended for testing

**Usage:** `linera faucet [OPTIONS] --amount <AMOUNT> [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — The chain that gives away its tokens

###### **Options:**

* `--port <PORT>` — The port on which to run the server

  Default value: `8080`
* `--amount <AMOUNT>` — The number of tokens to send to each new chain
* `--limit-rate-until <LIMIT_RATE_UNTIL>` — The end timestamp: The faucet will rate-limit the token supply so it runs out of money no earlier than this



## `linera publish-bytecode`

Publish bytecode

**Usage:** `linera publish-bytecode <CONTRACT> <SERVICE> [PUBLISHER]`

###### **Arguments:**

* `<CONTRACT>` — Path to the Wasm file for the application "contract" bytecode
* `<SERVICE>` — Path to the Wasm file for the application "service" bytecode
* `<PUBLISHER>` — An optional chain ID to publish the bytecode. The default chain of the wallet is used otherwise



## `linera create-application`

Create an application

**Usage:** `linera create-application [OPTIONS] <BYTECODE_ID> [CREATOR]`

###### **Arguments:**

* `<BYTECODE_ID>` — The bytecode ID of the application to create
* `<CREATOR>` — An optional chain ID to host the application. The default chain of the wallet is used otherwise

###### **Options:**

* `--json-parameters <JSON_PARAMETERS>` — The shared parameters as JSON string
* `--json-parameters-path <JSON_PARAMETERS_PATH>` — Path to a JSON file containing the shared parameters
* `--json-argument <JSON_ARGUMENT>` — The initialization argument as a JSON string
* `--json-argument-path <JSON_ARGUMENT_PATH>` — Path to a JSON file containing the initialization argument
* `--required-application-ids <REQUIRED_APPLICATION_IDS>` — The list of required dependencies of application, if any



## `linera publish-and-create`

Create an application, and publish the required bytecode

**Usage:** `linera publish-and-create [OPTIONS] <CONTRACT> <SERVICE> [PUBLISHER]`

###### **Arguments:**

* `<CONTRACT>` — Path to the Wasm file for the application "contract" bytecode
* `<SERVICE>` — Path to the Wasm file for the application "service" bytecode
* `<PUBLISHER>` — An optional chain ID to publish the bytecode. The default chain of the wallet is used otherwise

###### **Options:**

* `--json-parameters <JSON_PARAMETERS>` — The shared parameters as JSON string
* `--json-parameters-path <JSON_PARAMETERS_PATH>` — Path to a JSON file containing the shared parameters
* `--json-argument <JSON_ARGUMENT>` — The initialization argument as a JSON string
* `--json-argument-path <JSON_ARGUMENT_PATH>` — Path to a JSON file containing the initialization argument
* `--required-application-ids <REQUIRED_APPLICATION_IDS>` — The list of required dependencies of application, if any



## `linera request-application`

Request an application from another chain, so it can be used on this one

**Usage:** `linera request-application [OPTIONS] <APPLICATION_ID>`

###### **Arguments:**

* `<APPLICATION_ID>` — The ID of the application to request

###### **Options:**

* `--target-chain-id <TARGET_CHAIN_ID>` — The target chain on which the application is already registered. If not specified, the chain on which the application was created is used
* `--requester-chain-id <REQUESTER_CHAIN_ID>` — The owned chain on which the application is missing



## `linera keygen`

Create an unassigned key-pair

**Usage:** `linera keygen`



## `linera assign`

Link a key owned by the wallet to a chain that was just created for that key

**Usage:** `linera assign --key <KEY> --message-id <MESSAGE_ID>`

###### **Options:**

* `--key <KEY>` — The public key to assign
* `--message-id <MESSAGE_ID>` — The ID of the message that created the chain. (This uniquely describes the chain and where it was created.)



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



## `linera wallet show`

Show the contents of the wallet

**Usage:** `linera wallet show [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>`



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
* `--with-new-chain` — Request a new chain from the faucet, credited with tokens. This requires `--faucet`
* `--with-other-chains <WITH_OTHER_CHAINS>` — Other chains to follow
* `--testing-prng-seed <TESTING_PRNG_SEED>` — Force this wallet to generate keys using a PRNG and a given seed. USE FOR TESTING ONLY



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
* `<NAME>` — Specify the name of the Linera project. This is used to locate the generated bytecode. The generated bytecode should be of the form `<name>_{contract,service}.wasm`
* `<PUBLISHER>` — An optional chain ID to publish the bytecode. The default chain of the wallet is used otherwise

###### **Options:**

* `--json-parameters <JSON_PARAMETERS>` — The shared parameters as JSON string
* `--json-parameters-path <JSON_PARAMETERS_PATH>` — Path to a JSON file containing the shared parameters
* `--json-argument <JSON_ARGUMENT>` — The initialization argument as a JSON string
* `--json-argument-path <JSON_ARGUMENT_PATH>` — Path to a JSON file containing the initialization argument
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

* `--extra-wallets <EXTRA_WALLETS>` — The number of extra wallets and user chains to initialise. Default is 0
* `--other-initial-chains <OTHER_INITIAL_CHAINS>` — The number of initial "root" chains created in the genesis config on top of the default "admin" chain. All initial chains belong to the first "admin" wallet

  Default value: `10`
* `--initial-amount <INITIAL_AMOUNT>` — The initial amount of native tokens credited in the initial "root" chains, including the default "admin" chain

  Default value: `1000000`
* `--validators <VALIDATORS>` — The number of validators in the local test network. Default is 1

  Default value: `1`
* `--shards <SHARDS>` — The number of shards per validator in the local test network. Default is 1

  Default value: `1`
* `--testing-prng-seed <TESTING_PRNG_SEED>` — Force this wallet to generate keys using a PRNG and a given seed. USE FOR TESTING ONLY
* `--table-name <TABLE_NAME>` — The name for the database table to store the chain data in

  Default value: `table_default`



## `linera net helper`

Print a bash helper script to make `linera net up` easier to use. The script is meant to be installed in `~/.bash_profile` or sourced when needed

**Usage:** `linera net helper`



<hr/>

<small><i>
    This document was generated automatically by
    <a href="https://crates.io/crates/clap-markdown"><code>clap-markdown</code></a>.
</i></small>

