// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{Certificate, ChannelFullName, Event, Medium, Origin, Target},
    ChainManager,
};
use linera_base::{bcs_scalar, doc_scalar};

bcs_scalar!(Certificate, "A certified statement from the committee");
doc_scalar!(ChainManager, "How to produce new blocks");
doc_scalar!(
    ChannelFullName,
    "A channel name together with its application id"
);
doc_scalar!(
    Event,
    "An effect together with non replayable information to ensure uniqueness in a particular inbox"
);
doc_scalar!(
    Medium,
    "The origin of a message coming from a particular chain. Used to identify each inbox."
);
doc_scalar!(
    Origin,
    "The origin of a message, relative to a particular application. Used to identify each inbox."
);
doc_scalar!(
    Target,
    "The target of a message, relative to a particular application. Used to identify each outbox."
);
