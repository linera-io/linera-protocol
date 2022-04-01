# Overview

The goal of this roadmap document is to describe the software-engineering milestones of
Zefchain Labs on our way to a testnet.

In general, the highest priority should be given to the milestones and tasks that de-risk the project:

1. by confirming technical assumptions ("big bets") in our designs,

2. by validating the expected benefits of a Minimum Viable Product (MVP),

3. by making it easier to communicate about our work and build a community around us.

## Zefchain Labs

Our mission at Zefchain Labs is **to build and promote a new generation of decentralized systems that
operate efficiently and fairly at Internet-scale**.

Possible use cases:

* Trading (**this roadmap**)

* Fair and scalable NFT airdrops

* User rewards / Human work

* ...

Our core technology:

* Elastic validators (decentralized cloud-friendly sharded services)

* Multi-chain auditing and synchronization at scale

* Decentralized trading

Our expertise:

* BFT protocols (between validators)

* Crash-tolerant protocols (inside validators, between shards)

* Cryptography

* DeFI (*)

* Crypto-economics (*)

* Validity rollups (*)

(*) = need additional investments

## Current Target Product

The current MVP under consideration (aka *the Zefchain protocol*) includes

* a **layer-1 infrastructure ("blockchain")** secured by Delegated Proof-of-Stake with its
  own crypto-currency,

* a **decentralized exchange (DEX)** based on an order book,

* **comprehensive economic incentives** for stakeholders such as validators and delegators
  (PoS) and market makers (DeX).

## Initial State (Jan 2022)

The current codebase corresponds to the public research prototype
([branch `experimental`](https://github.com/ma2bd/fastpay/tree/experimental)) with a number of
simplifications and cleanups:

* Off-chain coins (transparent and opaque) presented in the [CCS'22
  submission](https://zefchain.com/papers/zef.pdf) and the corresponding cryptographic
  libraries were removed.

* FastPay was renamed consistently into "Zef".

The result covers the [draft on atomic swaps](https://arxiv.org/pdf/2201.05073.pdf) except
that client support and CLI commands for atomic swaps are missing.

## Tentative Timeline

* Projects starts Q1 2022

* Core **elastic validator** demo ready by end of Q2 2022

* Design for auditable chains is finished

* Mathieu's presentation at SBC'22 in Aug/Sep

* First version of Order book / DEX software ready by Q3 2022

* First version of Governance / PoS / Beacon chain ready Q4 2022

* Software ready for testnet Q4 2022 **(this roadmap)**

* Testnet/DEX launched by EOY 2022

* Prodnet/DEX launched by EOY 2023

## Caveat

This roadmap should be seen as a live document to be adjusted as we learn about the
product and the market.
