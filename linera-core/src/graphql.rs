// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::worker::Notification;
use linera_base::doc_scalar;

doc_scalar!(
    Notification,
    "Notify that a chain has a new certified block or a new message"
);
