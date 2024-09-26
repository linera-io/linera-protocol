## Motivation

<!--
Briefly describe the goal(s) of this PR.
-->

## Proposal

<!--
Summarize the proposed changes and how they address the goal(s) stated above.
-->

## Test Plan

<!--
Explain how you made sure that the changes are correct and that they perform as intended.

Please describe testing protocols (CI, manual tests, benchmarks, etc) in a way that others
can reproduce the results.
-->

## Release Plan

<!--
If this PR targets the `main` branch, **keep the applicable lines** to indicate if your
recommend the changes to be picked in release branches, SDKs, and hotfixes.

This generally concerns only bug fixes.

Note that altering the public protocol (e.g. transaction format, WASM syscalls) or storage
formats requires a new deployment.
-->
- Nothing to do / These changes follow the usual release cycle.
- These changes should be backported to the latest `devnet` branch, then
    - be released in a new SDK,
    - be released in a validator hotfix.
- These changes should be backported to the latest `testnet` branch, then
    - be released in a new SDK,
    - be released in a validator hotfix.
- Backporting is not possible but we may want to deploy a new `devnet` and release a new
      SDK soon.

## Links

<!--
Optional section for related PRs, related issues, and other references.

If needed, please create issues to track future improvements and link them here.
-->
- [reviewer checklist](https://github.com/linera-io/linera-protocol/blob/main/CONTRIBUTING.md#reviewer-checklist)
