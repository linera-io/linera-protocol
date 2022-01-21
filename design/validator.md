# Reference implementation of a Zefchain validator


## Overview

The reference Zefchain validator aims to provide a fully *elastic* implementation of
the Zefchain protocol.

```
Internet ||     Zefchain validator        || Cloud Provider(s)
         ||                               ||
Client <-||-> load balancer <-> workers <-||-> cloud storage
```

### Primary design goals

* The full Zefchain protocol should be supported with excellent performance and cost
  efficiency in mind.

* Important variations in client traffic should lead to appropriate up-scaling and
  down-scaling of service workers.

* Operational costs charged by a cloud provider should be covered by the transaction fees
  (pre-)paid by Zefchain clients.

* The system should tolerate crashes in workers.

* Cryptographic secrets should be protected.

* Cloud storage should support large-scale read-only accesses by auditors and new validators.

### Secondary design goals

* Workers should be as simple as possible. They may be also hosted by a cloud provider.

* Service logs should be created by workers and easily aggregated for consumption by the
  account owner (aka the operator).

* (MAYBE?) Physical shards should use different signing keys (PKI).

* Several cloud providers should be eventually supported (storage + workers).

* New validators should be able to verify data consistency lazily.


## Zefchain Concepts

* UID: unique identifier of a replicated account
--> Invariant: past UIDs can never be reassigned to a new account

* Operation: command sent to an account by an authorized entity

* Certified operation: an operation validated and signed by a quorum of validators

* Local v.s. remote effects: a command always has a *local effect* on
  its account and may have *remote effects* on other accounts.
  -->
  Invariant: when executing a set of (valid) operations, remote
  effects can be shuffled/delayed without affecting the final result.

* Sequence number: counter associated to the successive operations processed by an account
--> Invariant: there is at most one certified operation per UID and per sequence number

-------
TODO:
* specify all the admissible orderings for local/remote effects
--> Some local operations may depend on remote effects (e.g. account creation)
* current client is probably incomplete w.r.t confirmation of creation operations
-------


-------TODO------
* User account
* Protocol instance
* Account locks / revertible ownership delegation 
-----------------


## Storage Layout

### (Compressed) Certified operations

* S3 bucket is optionally public-readable for fast validator sync-ing

* Operations are chained -> only need to store the signatures for the last one (although information on past voters is useful -> perhaps compressed ?)

TODO:
* rollbacks for target accounts ?
* account creation seen as predecessor ("parent process") for both local and remote accounts?

### Account states

* S3 bucket optionally public-readable for fast auditing

### Account locking

* Private S3 bucket

* 
