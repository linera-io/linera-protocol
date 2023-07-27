// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the `WitStore` derive macro.

#![cfg(test)]

use super::derive_for_struct;
use quote::quote;
use syn::{parse_quote, Fields, ItemStruct};

/// Check the generated code for the body of the implementation of `WitStore` for a unit struct.
#[test]
fn zero_sized_type() {
    let input = Fields::Unit;
    let output = derive_for_struct(&input);

    let expected = quote! {
        fn store<Instance>(
            &self,
            memory: &mut linera_witty::Memory<'_, Instance>,
            mut location: linera_witty::GuestPointer,
        ) -> Result<(), linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let Self = self;
            Ok(())
        }

        fn lower<Instance>(
            &self,
            memory: &mut linera_witty::Memory<'_, Instance>,
        ) -> Result<<Self::Layout as linera_witty::Layout>::Flat, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let flat_layout = linera_witty::HList![];
            Ok(flat_layout)
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Check the generated code for the body of the implementation of `WitStore` for a named struct.
#[test]
fn named_struct() {
    let input: ItemStruct = parse_quote! {
        struct Type {
            first: u8,
            second: CustomType,
        }
    };
    let output = derive_for_struct(&input.fields);

    let expected = quote! {
        fn store<Instance>(
            &self,
            memory: &mut linera_witty::Memory<'_, Instance>,
            mut location: linera_witty::GuestPointer,
        ) -> Result<(), linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let Self { first, second } = self;

            location = location.after_padding_for::<u8>();
            WitStore::store(first, memory, location)?;
            location = location.after::<u8>();

            location = location.after_padding_for::<CustomType>();
            WitStore::store(second, memory, location)?;
            location = location.after::<CustomType>();

            Ok(())
        }

        fn lower<Instance>(
            &self,
            memory: &mut linera_witty::Memory<'_, Instance>,
        ) -> Result<<Self::Layout as linera_witty::Layout>::Flat, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let flat_layout = linera_witty::HList![];

            let field_layout = WitStore::lower(&self.first, memory)?;
            let flat_layout = flat_layout + field_layout;

            let field_layout = WitStore::lower(&self.second, memory)?;
            let flat_layout = flat_layout + field_layout;

            Ok(flat_layout)
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Check the generated code for the body of the implementation of `WitStore` for a tuple struct.
#[test]
fn tuple_struct() {
    let input: ItemStruct = parse_quote! {
        struct Type(String, Vec<CustomType>, i64);
    };
    let output = derive_for_struct(&input.fields);

    let expected = quote! {
        fn store<Instance>(
            &self,
            memory: &mut linera_witty::Memory<'_, Instance>,
            mut location: linera_witty::GuestPointer,
        ) -> Result<(), linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let Self(field0, field1, field2) = self;

            location = location.after_padding_for::<String>();
            WitStore::store(field0, memory, location)?;
            location = location.after::<String>();

            location = location.after_padding_for::<Vec<CustomType> >();
            WitStore::store(field1, memory, location)?;
            location = location.after::<Vec<CustomType> >();

            location = location.after_padding_for::<i64>();
            WitStore::store(field2, memory, location)?;
            location = location.after::<i64>();

            Ok(())
        }

        fn lower<Instance>(
            &self,
            memory: &mut linera_witty::Memory<'_, Instance>,
        ) -> Result<<Self::Layout as linera_witty::Layout>::Flat, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let flat_layout = linera_witty::HList![];

            let field_layout = WitStore::lower(&self.0, memory)?;
            let flat_layout = flat_layout + field_layout;

            let field_layout = WitStore::lower(&self.1, memory)?;
            let flat_layout = flat_layout + field_layout;

            let field_layout = WitStore::lower(&self.2, memory)?;
            let flat_layout = flat_layout + field_layout;

            Ok(flat_layout)
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}
