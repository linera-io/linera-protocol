// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generic representation of a function the host imports from a guest Wasm instance.
//!
//! This helps determining the actual signature of the imported guest Wasm function based on host
//! types for the parameters and the results.
//!
//! The signature depends on the number of flat types used to represent the parameters and the
//! results, as specified in the [canonical
//! ABI](https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md#flattening).

mod parameters;
mod results;
