// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod r#type;
pub use r#type::*;
use serde::{de::Deserialize, ser::Serialize};

impl<T, Repr> From<T> for Pretty<T, Repr> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T, Repr> Serialize for Pretty<T, Repr>
where
    T: Serialize + Clone,
    Repr: Serialize + From<T>,
{
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            Repr::from(self.value.clone()).serialize(serializer)
        } else {
            self.value.serialize(serializer)
        }
    }
}

impl<'de, T, Repr> serde::de::Deserialize<'de> for Pretty<T, Repr>
where
    T: Deserialize<'de> + From<Repr>,
    Repr: Deserialize<'de>,
{
    fn deserialize<D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            Ok(Pretty::new(Repr::deserialize(deserializer)?.into()))
        } else {
            Ok(Pretty::new(T::deserialize(deserializer)?))
        }
    }
}
