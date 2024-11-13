mod confirmed;
mod generic;
mod hashed;
mod timeout;
mod validated;

pub use generic::GenericCertificate;
pub use hashed::Hashed;

use crate::types::{ConfirmedBlock, Timeout, ValidatedBlock};

/// Certificate for a [`ValidatedBlock`]` instance.
pub type ValidatedBlockCertificate = GenericCertificate<ValidatedBlock>;

/// Certificate for a [`ConfirmedBlock`] instance.
pub type ConfirmedBlockCertificate = GenericCertificate<ConfirmedBlock>;

/// Certificate for a [`Timeout`] instance.
pub type TimeoutCertificate = GenericCertificate<Timeout>;
