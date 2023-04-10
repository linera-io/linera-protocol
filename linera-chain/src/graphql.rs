// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{Certificate, ChannelFullName, Event, Medium, Origin, Target},
    ChainManager,
};
use async_graphql::scalar;

scalar!(Certificate);
scalar!(ChainManager);
scalar!(ChannelFullName);
scalar!(Event);
scalar!(Medium);
scalar!(Origin);
scalar!(Target);
