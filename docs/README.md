# Linera documentation

## Markdown syntax

We use a number of markdown extensions supported by the `mdbook` generator
(configured in the directory `mdbook/`).

## Release constants

- RELEASE_BRANCH: the name of the branch on github (e.g. `testnet_conway`)
- RELEASE_DOMAIN: the domain name of the network (e.g. `testnet-conway`)
- RELEASE_HASH: the hash of the git commit being released
- RELEASE_VERSION: the SDK version being released (e.g. 0.x.y)

## Formatting

This directory is formatted with prettier. To install prettier run
`npm install -g prettier`. To use prettier run `prettier --write docs` from the
root of the repository.
