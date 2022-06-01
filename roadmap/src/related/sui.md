# SUI

## Economics
Sui recently released the [economics whitepaper](https://github.com/MystenLabs/sui/blob/main/doc/paper/tokenomics.pdf). 

* The transactions fees are voted by authorities (do the fee negotiations take into account the stake/delegated stake?)
* seperate fees for execution and storage   
* The reliable broadcast model allows for smoother distribution of rewards (as there's no single leader that gets the entire block reward)


### Pros
* Charging users for storage
* Avoids overcharging users as in 1st price auctions (similar to EIP-1559 in Ethereum)
* Preassure on the validators to keep prices low (is it secure?)

### Cons 
* there's a risk a centralization, where users migrate to an authority with lowest latency/fees (Sec. 6.4 is not convincing in proving decentralization)
* also, as they use PoS, users are incentivized to talk to authorities with a lot of stake to get the quorum votes faster/cheaper.
* They say "Finality is achieved once a quorum  of validators has executed  the certificate". How one knows that that happenned without an additional round of communication? 
* There's a preassure on the validators to keep the gas prices low (if not, they're penalized), it may again lead to centralization (only the most performant will persist)
* The tallying rule (Sec. 4.1.2) is based on a subjective view of the peers performance. Why would validators not lie/collude? It is kind of offset by th median rule in Sec 4.1.3, but is it fully secure?
* The storage price relies on SUI/$ ratio, so based on an oracle
* Validators can increase storage gas prices to increase the storage fund and get more rewards over delegators.
* We can only delete data that's not used for validation. It's unclear what percentage of the data will it be. 
* A user storing a lot of data and then ditching the system influences the fees for everyone else


### Weird
* "While a larger stake share lets validators reap more stake rewards, large validators are also more likely to be prioritized by clients during regular  network operations. Consequently, larger rewards are partially offset by the increased costs of scaling operations; thus ensuring all validators enjoy viable business models regardless of their delegated stake size"
* More and more data will be stored in the fund (not everything can be deleted), while the supply of SUI is fixed - the community can actually change some parameters to control that.