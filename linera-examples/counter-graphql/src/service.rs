// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::Counter;
use linera_sdk::service::system_api::ReadableWasmContext;

/// TODO(#434): Remove the type alias
type ReadableCounter = Counter<ReadableWasmContext>;
linera_sdk::service!(ReadableCounter);
