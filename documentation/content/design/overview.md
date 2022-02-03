+++
title = "Overview"
insert_anchor_links = "left"
weight = 0
+++

```
Internet ||     Zefchain validator        || Cloud Provider(s)
         ||                               ||
Client <-||-> load balancer <-> workers <-||-> cloud storage
```


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

