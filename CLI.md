# Command-Line Help for `Linera client tool`

This document contains the help content for the `Linera client tool` command-line program.

**Command Overview:**

* [`Linera client tool`↴](#Linera client tool)
* [`Linera client tool transfer`↴](#Linera client tool-transfer)
* [`Linera client tool open-chain`↴](#Linera client tool-open-chain)
* [`Linera client tool open-multi-owner-chain`↴](#Linera client tool-open-multi-owner-chain)
* [`Linera client tool close-chain`↴](#Linera client tool-close-chain)
* [`Linera client tool query-balance`↴](#Linera client tool-query-balance)
* [`Linera client tool sync-balance`↴](#Linera client tool-sync-balance)
* [`Linera client tool query-validators`↴](#Linera client tool-query-validators)
* [`Linera client tool set-validator`↴](#Linera client tool-set-validator)
* [`Linera client tool remove-validator`↴](#Linera client tool-remove-validator)
* [`Linera client tool resource-control-policy`↴](#Linera client tool-resource-control-policy)
* [`Linera client tool create-genesis-config`↴](#Linera client tool-create-genesis-config)
* [`Linera client tool watch`↴](#Linera client tool-watch)
* [`Linera client tool service`↴](#Linera client tool-service)
* [`Linera client tool faucet`↴](#Linera client tool-faucet)
* [`Linera client tool publish-bytecode`↴](#Linera client tool-publish-bytecode)
* [`Linera client tool create-application`↴](#Linera client tool-create-application)
* [`Linera client tool publish-and-create`↴](#Linera client tool-publish-and-create)
* [`Linera client tool request-application`↴](#Linera client tool-request-application)
* [`Linera client tool keygen`↴](#Linera client tool-keygen)
* [`Linera client tool assign`↴](#Linera client tool-assign)
* [`Linera client tool retry-pending-block`↴](#Linera client tool-retry-pending-block)
* [`Linera client tool wallet`↴](#Linera client tool-wallet)
* [`Linera client tool wallet show`↴](#Linera client tool-wallet-show)
* [`Linera client tool wallet set-default`↴](#Linera client tool-wallet-set-default)
* [`Linera client tool wallet init`↴](#Linera client tool-wallet-init)
* [`Linera client tool project`↴](#Linera client tool-project)
* [`Linera client tool project new`↴](#Linera client tool-project-new)
* [`Linera client tool project test`↴](#Linera client tool-project-test)
* [`Linera client tool project publish-and-create`↴](#Linera client tool-project-publish-and-create)
* [`Linera client tool net`↴](#Linera client tool-net)
* [`Linera client tool net up`↴](#Linera client tool-net-up)
* [`Linera client tool net helper`↴](#Linera client tool-net-helper)

## `Linera client tool`

A Byzantine-fault tolerant sidechain with low-latency finality and high throughput

**Usage:** `Linera client tool [OPTIONS] <COMMAND>`

###### **Subcommands:**

* `transfer` — Transfer funds
* `open-chain` — Open (i.e. activate) a new chain deriving the UID from an existing one
* `open-multi-owner-chain` — Open (i.e. activate) a new multi-owner chain deriving the UID from an existing one
* `close-chain` — Close (i.e. deactivate) an existing chain
* `query-balance` — Read the balance of the chain from the local state of the client
* `sync-balance` — Synchronize the local state of the chain (including a conservative estimation of the available balance) with a quorum validators
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
* `--send-timeout-us <SEND_TIMEOUT_US>` — Timeout for sending queries (us)

  Default value: `4000000`
* `--recv-timeout-us <RECV_TIMEOUT_US>` — Timeout for receiving responses (us)

  Default value: `4000000`
* `--max-pending-messages <MAX_PENDING_MESSAGES>`

  Default value: `10`
* `--wasm-runtime <WASM_RUNTIME>` — The WebAssembly runtime to use
* `--max-concurrent-queries <MAX_CONCURRENT_QUERIES>` — The maximal number of simultaneous queries to the database
* `--max-stream-queries <MAX_STREAM_QUERIES>` — The maximal number of simultaneous stream queries to the database

  Default value: `10`
* `--cache-size <CACHE_SIZE>` — The maximal number of entries in the storage cache

  Default value: `1000`
* `--notification-retry-delay-us <NOTIFICATION_RETRY_DELAY_US>` — Delay increment for retrying to connect to a validator for notifications

  Default value: `1000000`
* `--notification-retries <NOTIFICATION_RETRIES>` — Number of times to retry connecting to a validator for notifications

  Default value: `10`
* `--wait-for-outgoing-messages` — Whether to wait until a quorum of validators has confirmed that all sent cross-chain messages have been delivered
* `--tokio-threads <TOKIO_THREADS>` — The number of Tokio worker threads to use



## `Linera client tool transfer`

Transfer funds

**Usage:** `Linera client tool transfer --from <SENDER> --to <RECIPIENT> <AMOUNT>`

###### **Arguments:**

* `<AMOUNT>` — Amount to transfer

###### **Options:**

* `--from <SENDER>` — Sending chain id (must be one of our chains)
* `--to <RECIPIENT>` — Recipient account



## `Linera client tool open-chain`

Open (i.e. activate) a new chain deriving the UID from an existing one

**Usage:** `Linera client tool open-chain [OPTIONS]`

###### **Options:**

* `--from <CHAIN_ID>` — Chain id (must be one of our chains)
* `--to-public-key <PUBLIC_KEY>` — Public key of the new owner (otherwise create a key pair and remember it)
* `--initial-balance <BALANCE>` — The initial balance of the new chain. This is subtracted from the parent chain's balance

  Default value: `0`



## `Linera client tool open-multi-owner-chain`

Open (i.e. activate) a new multi-owner chain deriving the UID from an existing one

**Usage:** `Linera client tool open-multi-owner-chain [OPTIONS]`

###### **Options:**

* `--from <CHAIN_ID>` — Chain id (must be one of our chains)
* `--to-public-keys <PUBLIC_KEYS>` — Public keys of the new owners
* `--weights <WEIGHTS>` — Weights for the new owners
* `--multi-leader-rounds <MULTI_LEADER_ROUNDS>` — The number of rounds in which every owner can propose blocks, i.e. the first round number in which only a single designated leader is allowed to propose blocks
* `--initial-balance <BALANCE>` — The initial balance of the new chain. This is subtracted from the parent chain's balance

  Default value: `0`



## `Linera client tool close-chain`

Close (i.e. deactivate) an existing chain

**Usage:** `Linera client tool close-chain --from <CHAIN_ID>`

###### **Options:**

* `--from <CHAIN_ID>` — Chain id (must be one of our chains)



## `Linera client tool query-balance`

Read the balance of the chain from the local state of the client

**Usage:** `Linera client tool query-balance [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — Chain id



## `Linera client tool sync-balance`

Synchronize the local state of the chain (including a conservative estimation of the available balance) with a quorum validators

**Usage:** `Linera client tool sync-balance [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — Chain id



## `Linera client tool query-validators`

Show the current set of validators for a chain

**Usage:** `Linera client tool query-validators [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — Chain id



## `Linera client tool set-validator`

Add or modify a validator (admin only)

**Usage:** `Linera client tool set-validator [OPTIONS] --name <NAME> --address <ADDRESS>`

###### **Options:**

* `--name <NAME>` — The public key of the validator
* `--address <ADDRESS>` — Network address
* `--votes <VOTES>` — Voting power

  Default value: `1`



## `Linera client tool remove-validator`

Remove a validator (admin only)

**Usage:** `Linera client tool remove-validator --name <NAME>`

###### **Options:**

* `--name <NAME>` — The public key of the validator



## `Linera client tool resource-control-policy`

View or update the resource control policy

**Usage:** `Linera client tool resource-control-policy [OPTIONS]`

###### **Options:**

* `--certificate <CERTIFICATE>` — Set the base price for each certificate
* `--fuel <FUEL>` — Set the price per unit of fuel when executing user messages and operations
* `--storage-num-reads <STORAGE_NUM_READS>` — Set the price per byte to read data per operation
* `--storage-bytes-read <STORAGE_BYTES_READ>` — Set the price per byte to read data per byte
* `--storage-bytes-written <STORAGE_BYTES_WRITTEN>` — Set the price per byte to write data per byte
* `--storage-bytes-stored <STORAGE_BYTES_STORED>` — Set the price per byte stored
* `--maximum-bytes-read-per-block <MAXIMUM_BYTES_READ_PER_BLOCK>` — Set the maximum quantity of data to read per block
* `--maximum-bytes-written-per-block <MAXIMUM_BYTES_WRITTEN_PER_BLOCK>` — Set the maximum quantity of data to write per block
* `--messages <MESSAGES>` — Set the price per byte to store and send outgoing cross-chain messages



## `Linera client tool create-genesis-config`

Create genesis configuration for a Linera deployment. Create initial user chains and print information to be used for initialization of validator setup. This will also create an initial wallet for the owner of the initial "root" chains

**Usage:** `Linera client tool create-genesis-config [OPTIONS] --committee <COMMITTEE_CONFIG_PATH> --genesis <GENESIS_CONFIG_PATH> <NUM>`

###### **Arguments:**

* `<NUM>` — Number of additional chains to create

###### **Options:**

* `--committee <COMMITTEE_CONFIG_PATH>` — Sets the file describing the public configurations of all validators
* `--genesis <GENESIS_CONFIG_PATH>` — The output config path to be consumed by the server
* `--admin-root <ADMIN_ROOT>` — Index of the admin chain in the genesis config

  Default value: `0`
* `--initial-funding <INITIAL_FUNDING>` — Known initial balance of the chain

  Default value: `0`
* `--start-timestamp <START_TIMESTAMP>` — The start timestamp: no blocks can be created before this time
* `--certificate-price <CERTIFICATE_PRICE>` — Set the base price for each certificate

  Default value: `0`
* `--fuel-price <FUEL_PRICE>` — Set the price per unit of fuel when executing user messages and operations

  Default value: `0`
* `--storage-num-reads-price <STORAGE_NUM_READS_PRICE>` — Set the price per operation to read data

  Default value: `0`
* `--storage-bytes-read-price <STORAGE_BYTES_READ_PRICE>` — Set the price per byte to read data

  Default value: `0`
* `--storage-bytes-written-price <STORAGE_BYTES_WRITTEN_PRICE>` — Set the price per byte to write data

  Default value: `0`
* `--storage-bytes-stored-price <STORAGE_BYTES_STORED_PRICE>` — Set the price per byte stored

  Default value: `0`
* `--maximum-bytes-read-per-block <MAXIMUM_BYTES_READ_PER_BLOCK>` — Set the maximum read data per block
* `--maximum-bytes-written-per-block <MAXIMUM_BYTES_WRITTEN_PER_BLOCK>` — Set the maximum write data per block
* `--messages-price <MESSAGES_PRICE>` — Set the price per byte to store and send outgoing cross-chain messages

  Default value: `0`
* `--testing-prng-seed <TESTING_PRNG_SEED>` — Force this wallet to generate keys using a PRNG and a given seed. USE FOR TESTING ONLY
* `--network-name <NETWORK_NAME>` — A unique name to identify this network



## `Linera client tool watch`

Watch the network for notifications

**Usage:** `Linera client tool watch [OPTIONS] [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — The chain id to watch

###### **Options:**

* `--raw` — Show all notifications from all validators



## `Linera client tool service`

Run a GraphQL service to explore and extend the chains of the wallet

**Usage:** `Linera client tool service [OPTIONS]`

###### **Options:**

* `--listener-delay-before-ms <DELAY_BEFORE_MS>` — Wait before processing any notification (useful for testing)

  Default value: `0`
* `--listener-delay-after-ms <DELAY_AFTER_MS>` — Wait after processing any notification (useful for rate limiting)

  Default value: `0`
* `--port <PORT>` — The port on which to run the server

  Default value: `8080`



## `Linera client tool faucet`

Run a GraphQL service that exposes a faucet where users can claim tokens. This gives away the chain's tokens, and is mainly intended for testing

**Usage:** `Linera client tool faucet [OPTIONS] --amount <AMOUNT> [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — The chain that gives away its tokens

###### **Options:**

* `--port <PORT>` — The port on which to run the server

  Default value: `8080`
* `--amount <AMOUNT>` — The number of tokens to send to each new chain
* `--limit-rate-until <LIMIT_RATE_UNTIL>` — The end timestamp: The faucet will rate-limit the token supply so it runs out of money no earlier than this



## `Linera client tool publish-bytecode`

Publish bytecode

**Usage:** `Linera client tool publish-bytecode <CONTRACT> <SERVICE> [PUBLISHER]`

###### **Arguments:**

* `<CONTRACT>` — Path to the Wasm file for the application "contract" bytecode
* `<SERVICE>` — Path to the Wasm file for the application "service" bytecode
* `<PUBLISHER>` — An optional chain ID to publish the bytecode. The default chain of the wallet is used otherwise



## `Linera client tool create-application`

Create an application

**Usage:** `Linera client tool create-application [OPTIONS] <BYTECODE_ID> [CREATOR]`

###### **Arguments:**

* `<BYTECODE_ID>` — The bytecode ID of the application to create
* `<CREATOR>` — An optional chain ID to host the application. The default chain of the wallet is used otherwise

###### **Options:**

* `--json-parameters <JSON_PARAMETERS>` — The shared parameters as JSON string
* `--json-parameters-path <JSON_PARAMETERS_PATH>` — Path to a JSON file containing the shared parameters
* `--json-argument <JSON_ARGUMENT>` — The initialization argument as a JSON string
* `--json-argument-path <JSON_ARGUMENT_PATH>` — Path to a JSON file containing the initialization argument
* `--required-application-ids <REQUIRED_APPLICATION_IDS>` — The list of required dependencies of application, if any



## `Linera client tool publish-and-create`

Create an application, and publish the required bytecode

**Usage:** `Linera client tool publish-and-create [OPTIONS] <CONTRACT> <SERVICE> [PUBLISHER]`

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



## `Linera client tool request-application`

Request an application from another chain, so it can be used on this one

**Usage:** `Linera client tool request-application [OPTIONS] <APPLICATION_ID>`

###### **Arguments:**

* `<APPLICATION_ID>` — The ID of the application to request

###### **Options:**

* `--target-chain-id <TARGET_CHAIN_ID>` — The target chain on which the application is already registered. If not specified, the chain on which the application was created is used
* `--requester-chain-id <REQUESTER_CHAIN_ID>` — The owned chain on which the application is missing



## `Linera client tool keygen`

Create an unassigned key-pair

**Usage:** `Linera client tool keygen`



## `Linera client tool assign`

Link a key owned by the wallet to a chain that was just created for that key

**Usage:** `Linera client tool assign --key <KEY> --message-id <MESSAGE_ID>`

###### **Options:**

* `--key <KEY>` — The public key to assign
* `--message-id <MESSAGE_ID>` — The ID of the message that created the chain. (This uniquely describes the chain and where it was created.)



## `Linera client tool retry-pending-block`

Retry a block we unsuccessfully tried to propose earlier.

As long as a block is pending most other commands will fail, since it is unsafe to propose multiple blocks at the same height.

**Usage:** `Linera client tool retry-pending-block [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>` — The chain with the pending block. If not specified, the wallet's default chain is used



## `Linera client tool wallet`

Show the contents of the wallet

**Usage:** `Linera client tool wallet <COMMAND>`

###### **Subcommands:**

* `show` — Show the contents of the wallet
* `set-default` — Change the wallet default chain
* `init` — Initialize a wallet from the genesis configuration



## `Linera client tool wallet show`

Show the contents of the wallet

**Usage:** `Linera client tool wallet show [CHAIN_ID]`

###### **Arguments:**

* `<CHAIN_ID>`



## `Linera client tool wallet set-default`

Change the wallet default chain

**Usage:** `Linera client tool wallet set-default <CHAIN_ID>`

###### **Arguments:**

* `<CHAIN_ID>`



## `Linera client tool wallet init`

Initialize a wallet from the genesis configuration

**Usage:** `Linera client tool wallet init [OPTIONS]`

###### **Options:**

* `--genesis <GENESIS_CONFIG_PATH>` — The path to the genesis configuration for a Linera deployment. Either this or `--faucet` must be specified
* `--faucet <FAUCET>` — The address of a faucet. If this is specified, the default chain will be newly created, and credited with tokens
* `--with-other-chains <WITH_OTHER_CHAINS>` — Other chains to follow
* `--testing-prng-seed <TESTING_PRNG_SEED>` — Force this wallet to generate keys using a PRNG and a given seed. USE FOR TESTING ONLY



## `Linera client tool project`

Manage Linera projects

**Usage:** `Linera client tool project <COMMAND>`

###### **Subcommands:**

* `new` — Create a new Linera project
* `test` — Test a Linera project
* `publish-and-create` — Build and publish a Linera project



## `Linera client tool project new`

Create a new Linera project

**Usage:** `Linera client tool project new [OPTIONS] <NAME>`

###### **Arguments:**

* `<NAME>` — The project name. A directory of the same name will be created in the current directory

###### **Options:**

* `--linera-root <LINERA_ROOT>` — Use the given clone of the Linera repository instead of remote crates



## `Linera client tool project test`

Test a Linera project.

Equivalent to running `cargo test` with the appropriate test runner.

**Usage:** `Linera client tool project test [PATH]`

###### **Arguments:**

* `<PATH>`



## `Linera client tool project publish-and-create`

Build and publish a Linera project

**Usage:** `Linera client tool project publish-and-create [OPTIONS] [PATH] [NAME] [PUBLISHER]`

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



## `Linera client tool net`

Manage a local Linera Network

**Usage:** `Linera client tool net <COMMAND>`

###### **Subcommands:**

* `up` — Start a Local Linera Network
* `helper` — Print a bash helper script to make `linera net up` easier to use. The script is meant to be installed in `~/.bash_profile` or sourced when needed



## `Linera client tool net up`

Start a Local Linera Network

**Usage:** `Linera client tool net up [OPTIONS]`

###### **Options:**

* `--extra-wallets <EXTRA_WALLETS>` — The number of extra wallets and user chains to initialise. Default is 0
* `--validators <VALIDATORS>` — The number of validators in the local test network. Default is 1

  Default value: `1`
* `--shards <SHARDS>` — The number of shards per validator in the local test network. Default is 1

  Default value: `1`
* `--testing-prng-seed <TESTING_PRNG_SEED>` — Force this wallet to generate keys using a PRNG and a given seed. USE FOR TESTING ONLY
* `--table-name <TABLE_NAME>` — The name for the database table to store the chain data in

  Default value: `table_default`



## `Linera client tool net helper`

Print a bash helper script to make `linera net up` easier to use. The script is meant to be installed in `~/.bash_profile` or sourced when needed

**Usage:** `Linera client tool net helper`



<hr/>

<small><i>
    This document was generated automatically by
    <a href="https://crates.io/crates/clap-markdown"><code>clap-markdown</code></a>.
</i></small>

