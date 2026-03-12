# Glossary

- **Address**: A unique public alphanumeric identifier used to designate the
  identity of an entity on the Linera network.

- **Admin Chain**: The Linera Network has one designated _admin chain_ where
  validators can join or leave and where new epochs are defined.

- **Application**: Similar to a smart-contract on Ethereum, an application is
  code deployed on the Linera network which is executed by all validators. An
  application has a metered _contract_ which executes 'business logic' and
  modifies state and an unmetered 'service' which is a read-only view into an
  application's state.

- **Byzantine Fault-Tolerant (BFT)**: A system which can operate correctly and
  achieve consensus even if components of the system fail or act maliciously.

- **Block Height**: The number of blocks preceding a given block on a specific
  microchain.

- **Block Proposal**: A candidate block proposed by a chain owner which may be
  selected at the next block height.

- **Bytecode**: A collection of bytes corresponding to a program that can be run
  by the virtual machine. This is either a Wasm or EVM bytecode.

- **Client**: The `linera` program, which is a local node and wallet operated by
  users to make requests to the network. In Linera, clients drive the network by
  proposing new blocks and validators are mostly reactive.

- **Certificate**: A value with signatures from a quorum of validators. Values
  can be confirmed blocks, meaning that the block has been added to the chain
  and is final. There are other values that are used for reaching consensus,
  before certifying a confirmed block.

- **Committee**: The set of all validators for a particular _epoch_, together
  with their voting weights.

- **Chain Owner**: The owner of a _user chain_ or _multi-user chain_. This is
  represented as the alphanumeric identifier derived from the hash of the
  owner's public key.

- **Channel**: A broadcast mechanism enabling publish-subscribe behavior across
  chains.

- **Contract**: The metered part of an application which executes business logic
  and can modify the application's state.

- **Cross-Application Call**: A call from one application to another on the
  _same chain_.

- **Cross-Chain Message**: A message containing a data payload which is sent
  from one chain to another. Cross-Chain messages are the asynchronous
  communication primitive which enable communication on the same application
  running on different chains.

- **Devnet**: An experimental deployment of the Linera protocol meant for
  testing and development. In a Devnet, the validator nodes are often run by the
  same operator for simplicity. Devnets may be shut down and restarted from a
  genesis configuration any time. Devnets do not handle real assets.

- **Epoch**: A period of time when a particular set of validators with
  particular voting weights can certify new blocks. Since each chain has to
  transition explicitly from one epoch to the next, epochs can overlap.

- **Genesis Configuration**: The configuration determining the state of a newly
  created network; the voting weights of the initial set of validators, the
  initial fee structure, and initial chains that the network starts with.

- **Inbox**: A commutative data structure storing incoming messages for a given
  chain.

- **Mainnet**: A deployment meant to be used in production, with real assets.

- **Message**: See 'Cross-Chain Message'.

- **Microchain**: A lightweight chain of blocks holding a subset of the
  network's state running on every validator. This is used interchangeably with
  'chain'. _All_ Linera chains are microchains.

- **Network**: The totality of all protocol participants. A network is the
  combination of committee, clients and auditors.

- **Operation**: Operations are either transactions directly added to a block by
  the creator (and signer) of the block, or calls to an application from
  another. Users typically use operations to start interacting with an
  application on their own chain.

- **Multi-user Chain**: A microchain which is owned by more than one user. Users
  take turns proposing blocks and the likelihood of selection is proportional to
  their _weight_.

- **Project**: The collection of files and dependencies which are built into the
  bytecode which is instantiated as an application on the Linera Network.

- **Public Chain**: A microchain with full BFT consensus with a strict set of
  permissions relied on for the operation of the network.

- **Quorum**: A set of validators representing > ⅔ of the total stake. A quorum
  is required to create a certificate.

- **Single-Owner Chain**: See 'User Chain'.

- **Service**: An unmetered read-only view into an application's state.

- **Shard**: A logical subset of all microchains on a given validator. This
  corresponds directly to a physical _worker_.

- **Stake**: An amount of tokens pledged by a validator or auditor, as a
  collateral to guarantee their honest and correct participation in the network.

- **Testnet**: A deployment of the Linera protocol meant for testing and
  development. In a Testnet, the validator nodes are operated by multiple
  operators. Testnets will gain in stability and decentralization over time in
  preparation of the mainnet launch. Testnets do not handle real assets.

- **User Chain**: Used interchangeably with _Single-Owner Chain_. User chains
  are chains which are owned by a single user on the network. Only the chain
  owner can propose blocks, and therefore only the chain owner can forcibly
  advance the state of a user chain.

- **Validator**: Validators run the servers that allow users to download and
  create blocks. They validate, execute and cryptographically certify the blocks
  of all the chains.

- **View**: Views are like an Object-Relational Mapping (ORM) for mapping
  complex types onto key-value stores. Views group complex state changes into a
  set of elementary operations and commit them atomically. They are full or
  partial in-memory representations of complex types saved on disk in a
  key-value store

- **Wallet**: A file containing a user's public and private keys along with
  configuration and information regarding the chains they own.

- **WebAssembly (Wasm)**: A binary compilation target and instruction format
  that runs on a stack-based VM. Linera applications are compiled to Wasm and
  run on Wasm VMs inside validators and clients.

- **Web3**: A natural evolution of the internet focusing on decentralization by
  leveraging blockchains and smart contracts.

- **Worker**: A process which runs a subset of all microchains on a given
  validator. This corresponds directly to a logical _shard_.
