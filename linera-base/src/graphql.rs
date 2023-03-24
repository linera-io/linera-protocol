// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    crypto::{CryptoHash, PublicKey, Signature},
    data_types::{Amount, Balance, BlockHeight, Timestamp},
    identifiers::{ApplicationId, BytecodeId, ChainDescription, ChainId, ChannelName, Owner},
};
use async_graphql::scalar;

scalar!(Amount);
scalar!(ApplicationId);
scalar!(Balance);
scalar!(BlockHeight);
scalar!(BytecodeId);
scalar!(ChainDescription);
scalar!(ChainId);
scalar!(ChannelName);
scalar!(CryptoHash);
scalar!(Owner);
scalar!(PublicKey);
scalar!(Signature);
scalar!(Timestamp);
