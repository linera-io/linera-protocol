# Reference AWS implementation of a Zefchain validator


## Overview

The reference Zefchain-AWS validator aims to provide a fully *elastic*
implementation of the Zefchain protocol in AWS.

```
Internet || AWS instance
         ||
Client <-||-> service workers <-> storage
         ||
```

### Primary design goals

* The full Zefchain protocol should be supported with excellent
  performance and cost efficiency in mind.

* Important variations in client traffic should lead to appropriate
  up-scaling and down-scaling of service workers managed by AWS.

* The corresponding variable fees charged AWS should be always
  strictly covered by the transaction fees (pre-)paid by Zefchain clients.

* The system should tolerate crashes in workers.

* Cryptographic secrets should be protected using AWS abstractions.

### Secondary design goals

* AWS components should be kept as simple as possible, ideally using
  only Serveless applications and S3 storage.

* Fast synchronization with other AWS validator instances should be
  supported (e.g. public S3 buckets).

* Service logs should be created by workers and aggregated for
  consumption by the AWS account owner (aka the operator).

* (MAYBE?) Physical shards should use different signing keys (PKI).

### Non-goals

* Decentralization will require translating this design to additional
  service providers.


## Zefchain Concepts

* UID: unique identifier of a replicated account
--> Invariant: past UIDs can never be reassigned to a new account

* Operation: command sent to an account by an authorized entity

* Certified operation: an operation validated and signed by a quorum of validators

* Local v.s. remote effects: a command always has a *local effect* on
  its account and may have *remote effects* on other accounts.
  -->
  Invariant: when executing a set of (valid) operations, remote
  effects can be shuffled without affecting the final result.

* Sequence number: counter associated to the successive operations processed by an account
--> Invariant: there is at most one certified operation per UID and per sequence number

-------
TODO:
* specify all the admissible orderings for local/remote effects (causality dependencies with account creation)
* current client is probably incomplete w.r.t confirmation of creation operations
-------


-------TODO------
* User account
* Protocol instance
* Account locks / revertible ownership delegation 
-----------------


## Storage Layout

### Certified operations

* S3 bucket is optionally public-readable for fast validator sync-ing

* 

### Account states

* S3 bucket optionally public-readable for fast auditing

### Account locking

* Private S3 bucket

* 