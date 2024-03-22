// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for types from the standard library.

use std::{borrow::Cow, ops::Add};

use frunk::{HCons, HList, HNil};

use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory, Split,
    WitLoad, WitStore, WitType,
};

impl WitType for HNil {
    const SIZE: u32 = 0;

    type Layout = HNil;
    type Dependencies = HNil;

    fn wit_type_name() -> Cow<'static, str> {
        "hnil".into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        "type hnil = unit".into()
    }
}

impl WitLoad for HNil {
    fn load<Instance>(
        _memory: &Memory<'_, Instance>,
        _location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
    {
        Ok(HNil)
    }

    fn lift_from<Instance>(
        HNil: <Self::Layout as Layout>::Flat,
        _memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
    {
        Ok(HNil)
    }
}

impl WitStore for HNil {
    fn store<Instance>(
        &self,
        _memory: &mut Memory<'_, Instance>,
        _location: GuestPointer,
    ) -> Result<(), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Ok(())
    }

    fn lower<Instance>(
        &self,
        _memory: &mut Memory<'_, Instance>,
    ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Ok(HNil)
    }
}

impl<Head, Tail> WitType for HCons<Head, Tail>
where
    Head: WitType,
    Tail: WitType + SizeCalculation,
    Head::Layout: Add<Tail::Layout>,
    <Head::Layout as Add<Tail::Layout>>::Output: Layout,
{
    const SIZE: u32 = {
        let packed_size = Self::SIZE_STARTING_AT_BYTE_BOUNDARIES[0];
        let aligned_size = GuestPointer(packed_size).after_padding_for::<Self>();

        aligned_size.0
    };

    type Layout = <Head::Layout as Add<Tail::Layout>>::Output;
    type Dependencies = HList![Head, Tail];

    fn wit_type_name() -> Cow<'static, str> {
        format!("hcons-{}-{}", Head::wit_type_name(), Tail::wit_type_name()).into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        let head = Head::wit_type_name();
        let tail = Tail::wit_type_name();

        format!("type hcons-{head}-{tail} = tuple<{head}, {tail}>").into()
    }
}

impl<Head, Tail> WitLoad for HCons<Head, Tail>
where
    Head: WitLoad,
    Tail: WitLoad + SizeCalculation,
    Head::Layout: Add<Tail::Layout>,
    <Head::Layout as Add<Tail::Layout>>::Output: Layout,
    <Self::Layout as Layout>::Flat:
        Split<<Head::Layout as Layout>::Flat, Remainder = <Tail::Layout as Layout>::Flat>,
{
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Ok(HCons {
            head: Head::load(memory, location)?,
            tail: Tail::load(
                memory,
                location
                    .after::<Head>()
                    .after_padding_for::<Tail::FirstElement>(),
            )?,
        })
    }

    fn lift_from<Instance>(
        layout: <Self::Layout as Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (head_layout, tail_layout) = layout.split();

        Ok(HCons {
            head: Head::lift_from(head_layout, memory)?,
            tail: Tail::lift_from(tail_layout, memory)?,
        })
    }
}

impl<Head, Tail> WitStore for HCons<Head, Tail>
where
    Head: WitStore,
    Tail: WitStore + SizeCalculation,
    Head::Layout: Add<Tail::Layout>,
    <Head::Layout as Add<Tail::Layout>>::Output: Layout,
    <Head::Layout as Layout>::Flat: Add<<Tail::Layout as Layout>::Flat>,
    Self::Layout: Layout<
        Flat = <<Head::Layout as Layout>::Flat as Add<<Tail::Layout as Layout>::Flat>>::Output,
    >,
{
    fn store<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<(), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        self.head.store(memory, location)?;
        self.tail.store(
            memory,
            location
                .after::<Head>()
                .after_padding_for::<Tail::FirstElement>(),
        )?;

        Ok(())
    }

    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let head_layout = self.head.lower(memory)?;
        let tail_layout = self.tail.lower(memory)?;

        Ok(head_layout + tail_layout)
    }
}

/// Helper trait used to calculate the size of a heterogeneous list considering internal alignment.
///
/// Assumes the maximum alignment necessary for any type is 8 bytes, which is the alignment for the
/// largest flat types (`i64` and `f64`).
trait SizeCalculation {
    /// The size of the list considering the current size calculation starts at different offsets
    /// inside an 8-byte window.
    const SIZE_STARTING_AT_BYTE_BOUNDARIES: [u32; 8];

    /// The type of the first element of the list, used to determine the current necessary
    /// alignment.
    type FirstElement: WitType;
}

impl SizeCalculation for HNil {
    const SIZE_STARTING_AT_BYTE_BOUNDARIES: [u32; 8] = [0; 8];

    type FirstElement = ();
}

/// Unrolls a `for`-like loop so that it runs in a `const` context.
macro_rules! unroll_for {
    ($binding:ident in [ $($elements:expr),* $(,)? ] $body:tt) => {
        $(
            let $binding = $elements;
            $body
        )*
    };
}

impl<Head, Tail> SizeCalculation for HCons<Head, Tail>
where
    Head: WitType,
    Tail: SizeCalculation,
{
    const SIZE_STARTING_AT_BYTE_BOUNDARIES: [u32; 8] = {
        let mut size_at_boundaries = [0; 8];

        unroll_for!(boundary_offset in [0, 1, 2, 3, 4, 5, 6, 7] {
            let memory_location = GuestPointer(boundary_offset)
                .after_padding_for::<Head>()
                .after::<Head>();

            let tail_size = Tail::SIZE_STARTING_AT_BYTE_BOUNDARIES[memory_location.0 as usize % 8];

            size_at_boundaries[boundary_offset as usize] =
                memory_location.0 - boundary_offset + tail_size;
        });

        size_at_boundaries
    };

    type FirstElement = Head;
}
