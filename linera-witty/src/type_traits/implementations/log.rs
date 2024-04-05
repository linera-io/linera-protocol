// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for types from the [`log`] crate.

use std::borrow::Cow;

use frunk::{hlist_pat, HList};
use log::Level;

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
    WitLoad, WitStore, WitType,
};

impl WitType for Level {
    const SIZE: u32 = 1;

    type Layout = HList![i8];
    type Dependencies = HList![];

    fn wit_type_name() -> Cow<'static, str> {
        "log-level".into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        concat!(
            "    enum log-level {\n",
            "        error,\n",
            "        warn,\n",
            "        info,\n",
            "        debug,\n",
            "        trace,\n",
            "    }\n",
        )
        .into()
    }
}

impl WitLoad for Level {
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        match u8::load(memory, location)? {
            0 => Ok(Level::Error),
            1 => Ok(Level::Warn),
            2 => Ok(Level::Info),
            3 => Ok(Level::Debug),
            4 => Ok(Level::Trace),
            _ => unreachable!("Invalid log level"),
        }
    }

    fn lift_from<Instance>(
        hlist_pat![discriminant]: <Self::Layout as Layout>::Flat,
        _memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        match discriminant {
            0 => Ok(Level::Error),
            1 => Ok(Level::Warn),
            2 => Ok(Level::Info),
            3 => Ok(Level::Debug),
            4 => Ok(Level::Trace),
            _ => unreachable!("Invalid log level"),
        }
    }
}

impl WitStore for Level {
    fn store<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<(), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let discriminant: i8 = match self {
            Level::Error => 0,
            Level::Warn => 1,
            Level::Info => 2,
            Level::Debug => 3,
            Level::Trace => 4,
        };

        discriminant.store(memory, location)
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let discriminant: i8 = match self {
            Level::Error => 0,
            Level::Warn => 1,
            Level::Info => 2,
            Level::Debug => 3,
            Level::Trace => 4,
        };

        discriminant.lower(memory)
    }
}
