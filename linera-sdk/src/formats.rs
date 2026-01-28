// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support the declaration of the binary formats used by an application.

use serde::{Deserialize, Serialize};
use serde_reflection::{Format, Registry};

/// The serde formats used by an application. The exact serde encoding in use must be
/// known separately.
#[derive(Serialize, Deserialize, Debug, Eq, Clone, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub struct Formats {
    /// The registry of container definitions.
    pub registry: Registry,
    /// The format of operations.
    pub operation: Format,
    /// The format of operation responses.
    pub response: Format,
    /// The format of messages.
    pub message: Format,
    /// The format of events.
    pub event_value: Format,
}

/// An application using BCS as binary encoding.
pub trait BcsApplication {
    /// Link the public Abi of application for good measure.
    type Abi;

    /// Returns the serde formats for this application's ABI types.
    fn formats() -> serde_reflection::Result<Formats>;
}
