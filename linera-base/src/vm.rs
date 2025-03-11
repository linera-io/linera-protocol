// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The virtual machines being supported.

use std::str::FromStr;
use std::borrow::Cow;
use linera_witty::{
    GuestPointer,
    HList,
    InstanceWithMemory,
    Runtime,
    Layout,
    Memory,
    RuntimeError,
    RuntimeMemory,
};

#[cfg(with_testing)]
use {
    alloy_primitives::FixedBytes,
    proptest::{
        collection::{vec, VecStrategy},
        prelude::{Arbitrary, Strategy},
        strategy,
    },
    std::ops::RangeInclusive,
};


use alloy::primitives::Address;
use async_graphql::scalar;
use derive_more::Display;
use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Encapsulation of address type so that we can implement WitType, WitLoad, WitStore
#[derive(Clone, Copy, Debug, Display, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize, PartialEq)]
pub struct EncapsulateAddress {
    /// The encapsulated address
    pub address: Address,
}

impl From<[u32; 5]> for EncapsulateAddress {
    fn from(integers: [u32; 5]) -> Self {
        let mut vec = [0_u8; 20];
        let mut pos = 0;
        for val in integers {
            let val = val.to_le_bytes();
            for item in val {
                vec[pos] = item;
                pos += 1;
            }
        }
        let address = Address::new(vec);
        Self { address }
    }
}

impl From<EncapsulateAddress> for [u32; 5] {
    fn from(address: EncapsulateAddress) -> Self {
        let data = address.address.into_array();
        let vec = data.chunks_exact(4)
            .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<_>>();
        vec.try_into().unwrap()
    }
}

impl WitType for EncapsulateAddress {
    const SIZE: u32 = <(u32, u32, u32, u32, u32) as WitType>::SIZE;
    type Layout = <(u32, u32, u32, u32, u32) as WitType>::Layout;
    type Dependencies = HList![];

    fn wit_type_name() -> Cow<'static, str> {
        "address".into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        concat!(
            "    record address {\n",
            "        part1: u32,\n",
            "        part2: u32,\n",
            "        part3: u32,\n",
            "        part4: u32,\n",
            "        part5: u32,\n",
            "    }\n",
        )
        .into()
    }
}

impl WitLoad for EncapsulateAddress {
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (part1, part2, part3, part4, part5) = WitLoad::load(memory, location)?;
        Ok(EncapsulateAddress::from([part1, part2, part3, part4, part5]))
    }

    fn lift_from<Instance>(
        flat_layout: <Self::Layout as linera_witty::Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
     	let (part1, part2, part3, part4, part5) = WitLoad::lift_from(flat_layout, memory)?;
        Ok(EncapsulateAddress::from([part1, part2, part3, part4, part5]))
    }
}

impl WitStore for EncapsulateAddress {
    fn store<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<(), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let [part1, part2, part3, part4, part5] = (*self).into();
        (part1, part2, part3, part4, part5).store(memory, location)
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let [part1, part2, part3, part4, part5] = (*self).into();
        (part1, part2, part3, part4, part5).lower(memory)
    }
}

#[cfg(with_testing)]
impl Arbitrary for EncapsulateAddress {
    type Parameters = ();
    type Strategy = strategy::Map<VecStrategy<RangeInclusive<u8>>, fn(Vec<u8>) -> EncapsulateAddress>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        vec(u8::MIN..=u8::MAX, FixedBytes::<20>::len_bytes())
            .prop_map(|vector| EncapsulateAddress { address: Address::new(vector.try_into().unwrap())})
    }
}

#[derive(
    Clone,
    Copy,
    Default,
    Display,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    WitType,
    WitStore,
    WitLoad,
    Debug,
)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary))]
/// The virtual machine runtime
pub enum VmRuntime {
    /// The Wasm virtual machine
    #[default]
    Wasm,
    /// The Evm virtual machine
    Evm(EncapsulateAddress),
}


impl FromStr for VmRuntime {
    type Err = VmRuntimeError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        if let Some(address) = string.strip_prefix("evm") {
            let address = Address::parse_checksummed(address, None)
                .map_err(|_| VmRuntimeError::InvalidAddress(address.to_string()))?;
            let address = EncapsulateAddress { address };
            return Ok(VmRuntime::Evm(address));
        }
        match string {
            "wasm" => Ok(VmRuntime::Wasm),
            unknown => Err(VmRuntimeError::InvalidVmRuntime(unknown.to_owned())),
        }
    }
}

scalar!(VmRuntime);

/// Error caused by invalid VM runtimes
#[derive(Clone, Debug, Error)]
#[error("{0:?} is not a valid virtual machine runtime")]
pub enum VmRuntimeError {
    /// The runtime is invalid
    InvalidVmRuntime(String),
    /// The address is invalid
    InvalidAddress(String),
}
