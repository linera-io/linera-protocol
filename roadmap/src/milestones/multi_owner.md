# Multi-Owner Chains

The goal of this milestone is to allow certain chains to be configured so that multiple stakeholders can safely issue commands on the same chain.

* Stakeholder may involuntarily post conflicting commands.

* Initially, we will assume a common purpose between stakeholders so that nobody wishes to actively block liveness of the chain.

* Used together with a special type of chains, this is the basis to support atomic-swaps.

* Even single-owner chains may require this feature so that system agents may carry automated tasks (e.g. [reconfiguration](reconfiguration.md), [code migration](extensibility.md), [trade confirmation](order_book.md)).

## Requirements

* The authentication of commands is generalized to allow a minimal one-shot BFT protocol to take place. (See [draft](https://arxiv.org/abs/2201.05073) on atomic swaps)

* Optionally, a simple leader rotation and timeouts are implemented to enforce liveness in the usual BFT sense.

* A malicious authority cannot stall the chain.


## Proposal: Verifiable Random Functions (VRFs)

VRFs were initially proposed in [this paper](https://dash.harvard.edu/bitstream/handle/1/5028196/Vadhan_VerifRandomFunction.pdf). They are used for leader election in [Algorand](https://dl.acm.org/doi/10.1145/3132747.3132757) (see Sec. 5 "CRYPTOGRAPHIC SORTITION"). VRFs require each user to have a keypair containing a public key `pk` and a secret key `sk`. Informally, on any input string `x`, `VRF(sk, x)` returns two values: a hash and a proof. The hash is a `hashlen` bit-long value that is uniquely determined by `sk` and `x`, but is indistinguishable from random to anyone that does not know `sk`. The proof `π` enables anyone that knows `pk` to check that the hash indeed corresponds to `x`, without having to know `sk`. For security, the VRF provides these properties even if `pk` and `sk` are chosen by an attacker.

### Pros
* VRFs do not require a trusted setup.
* VRFs are currently used by [multiple blockchains in production](https://en.wikipedia.org/wiki/Verifiable_random_function#In_cryptocurrency).
* Lightweight in terms of CPU usage.
* The elected node is unknown until it reveals itself.

### Cons
* May elect less or more than one leader.
* Requires publicly available randomness to work correctly. A VRF can be used to create secure randomness for other VRFs (as done in the [Algorand paper](https://dl.acm.org/doi/10.1145/3132747.3132757)). However, it is secure only if an attacker cannot easily change its key pair. In Algorand, the authors ensure that only a key available in a system for a while can be used to generate a random seed.


### TODOs
* check the binary consensus from the Algorand paper.

## Proposal: Verifiable Random Functions (VDFs)

### Pros
* Relatively simple
* Ensure that at least one leader is elected
* Relatively low CPU usage (limited to one core)

### Cons
* May select more than one leader.
* Constantly uses some CPU which might be wasteful. This might be problematic if we don't need to elect a leader very often.

### TODOs:
* Check ways of adapting VDFs complexity based on the stake of each authority.
