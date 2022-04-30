# Reconfiguration

This goal of this milestone is to support changes in the set of Zefchain validators.

## Requirements

### Chains

* Each chain is separately tracking the current set of validators

* An chain is migrated to a new set of validator using a specific *migration command*.

    - **Rational:** For a given chain and sequence number, there should be no ambiguity on the notion of quorum for a valid certificate

    - Migration must re-certify previous certificates sent by the chain (TODO: and received?)

* Initially, we assume active participation of chain owners to issue migration commands.

* Later, we will leverage the protocol for [multi-owner chains](multi_owner.md) to allow "system agents" to transition chains automatically.

* TODO: Should certificates have a TTL to force clients to migrate their chains?

### Management of configurations

* A special chain is introduced to issue commands that create new configurations.

* To get started, we may assume that the chain is owned by a single entity.

* Eventually, this chain should become a proper "beacon" blockchain where anybody can submit commands through a [mempool](mempools.md).

* TODO: Should configurations have a TTL to force the creation of new configurations at a minimal pace?

### New validators

* To get started, reconfigurations may be only about key rotations and changes in the voting rights of existing validators.

* Eventually, new validators should be able to acquire a working state from existing validators for a reasonable synchronization time and cost.

    - TODO: Should validators validate the imported chain state by replaying transactions (possibly lazily?) or by comparing states from other validators (seems difficult in theory due to eventual consistency but perhaps still doable in practice given that TPS per chains may be limited)
