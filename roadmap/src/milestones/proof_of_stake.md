# Proof of Stake

This milestones aims to implement the management of stakes (possibly delegated) to ensure trust in the validators.

Stakes will eventually affect the redistribution of user fees (aka *yield*).

## Requirements

* Validators may change their voting rights in the next configuration of system by adding or decreasing their *stake*, that is, some unit of values that are escrowed in the beacon chain.

* Stakes may come from user delegation.

* Eventually, the proof-of-Stake layer should allow full *open membership*, i.e. the fact that validators can join or leave without human intervention.
