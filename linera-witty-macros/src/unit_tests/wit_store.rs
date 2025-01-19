// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the `WitStore` derive macro.

#![cfg(test)]

use quote::quote;
use syn::{parse_quote, Fields, ItemEnum, ItemStruct};

use super::{derive_for_enum, derive_for_struct};

/// Checks the generated code for the body of the implementation of `WitStore` for a unit struct.
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
            linera_witty::hlist![].store(memory, location)
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
            let Self = self;
            linera_witty::hlist![].lower(memory)
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitStore` for a named struct.
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
            let Self { first, second, } = self;
            linera_witty::hlist![first, second].store(memory, location)
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
            let Self { first, second, } = self;
            linera_witty::hlist![first, second].lower(memory)
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitStore` for a tuple struct.
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
            linera_witty::hlist![field0, field1, field2].store(memory, location)
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
            let Self(field0, field1, field2) = self;
            linera_witty::hlist![field0, field1, field2].lower(memory)
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitStore` for a enum.
#[test]
fn enum_type() {
    let input: ItemEnum = parse_quote! {
        enum Enum {
            Empty,
            Tuple(i8, CustomType),
            Struct {
                first: (),
                second: String,
            },
        }
    };
    let output = derive_for_enum(&input.ident, input.variants.iter());

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
            match self {
                Enum::Empty => {
                    0_u8.store(memory, location)?;
                    location = location
                        .after::<u8>()
                        .after_padding_for::<linera_witty::HList![]>()
                        .after_padding_for::<linera_witty::HList![i8, CustomType]>()
                        .after_padding_for::<linera_witty::HList![(), String]>();

                    linera_witty::hlist![].store(memory, location)
                }
                Enum::Tuple(field0, field1) => {
                    1_u8.store(memory, location)?;
                    location = location
                        .after::<u8>()
                        .after_padding_for::<linera_witty::HList![]>()
                        .after_padding_for::<linera_witty::HList![i8, CustomType]>()
                        .after_padding_for::<linera_witty::HList![(), String]>();

                    linera_witty::hlist![field0, field1].store(memory, location)
                }
                Enum::Struct { first, second, } => {
                    2_u8.store(memory, location)?;
                    location = location
                        .after::<u8>()
                        .after_padding_for::<linera_witty::HList![]>()
                        .after_padding_for::<linera_witty::HList![i8, CustomType]>()
                        .after_padding_for::<linera_witty::HList![(), String]>();

                    linera_witty::hlist![first, second].store(memory, location)
                }
            }
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
            match self {
                Enum::Empty => {
                    let variant_flat_layout = linera_witty::hlist![].lower(memory)?;
                    let flat_layout: <Self::Layout as linera_witty::Layout>::Flat =
                        linera_witty::JoinFlatLayouts::into_joined(
                            0_u8.lower(memory)? + variant_flat_layout,
                        );

                    Ok(flat_layout)
                }
                Enum::Tuple(field0, field1) => {
                    let variant_flat_layout = linera_witty::hlist![field0, field1].lower(memory)?;
                    let flat_layout: <Self::Layout as linera_witty::Layout>::Flat =
                        linera_witty::JoinFlatLayouts::into_joined(
                            1_u8.lower(memory)? + variant_flat_layout,
                        );

                    Ok(flat_layout)
                }
                Enum::Struct { first, second, } => {
                    let variant_flat_layout = linera_witty::hlist![first, second].lower(memory)?;
                    let flat_layout: <Self::Layout as linera_witty::Layout>::Flat =
                        linera_witty::JoinFlatLayouts::into_joined(
                            2_u8.lower(memory)? + variant_flat_layout,
                        );

                    Ok(flat_layout)
                }
            }
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitStore` for a named struct
/// with a single ignored fields.
#[test]
fn named_struct_with_one_skipped_field() {
    let input: ItemStruct = parse_quote! {
        struct Type {
            first: u8,
            #[witty(skip)]
            ignored: i128,
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
            let Self { first, second, .. } = self;
            linera_witty::hlist![first, second].store(memory, location)
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
            let Self { first, second, .. } = self;
            linera_witty::hlist![first, second].lower(memory)
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitStore` for a named struct
/// with some ignored fields.
#[test]
fn named_struct_with_skipped_fields() {
    let input: ItemStruct = parse_quote! {
        struct Type {
            #[witty(skip)]
            ignored1: u8,
            first: u8,
            #[witty(skip)]
            ignored2: i128,
            #[witty(skip)]
            ignored3: String,
            second: CustomType,
            #[witty(skip)]
            ignored4: Vec<()>,
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
            let Self { first, second, .. } = self;
            linera_witty::hlist![first, second].store(memory, location)
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
            let Self { first, second, .. } = self;
            linera_witty::hlist![first, second].lower(memory)
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitStore` for a tuple struct
/// with some ignored fields.
#[test]
fn tuple_struct_with_skipped_fields() {
    let input: ItemStruct = parse_quote! {
        struct Type(
            #[witty(skip)]
            PhantomData<T>,
            String,
            Vec<CustomType>,
            #[witty(skip)]
            bool,
            i64,
        );
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
            let Self(_, field1, field2, _, field4) = self;
            linera_witty::hlist![field1, field2, field4].store(memory, location)
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
            let Self(_, field1, field2, _, field4) = self;
            linera_witty::hlist![field1, field2, field4].lower(memory)
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitStore` for a enum with some
/// ignored fields.
#[test]
fn enum_type_with_skipped_fields() {
    let input: ItemEnum = parse_quote! {
        enum Enum {
            Empty,
            Tuple(i8, CustomType, #[witty(skip)] u128),
            Struct {
                first: (),
                #[witty(skip)]
                ignored1: (u8, u16),
                #[witty(skip)]
                ignored2: String,
                second: String,
                #[witty(skip)]
                ignored3: Option<String>,
            },
        }
    };
    let output = derive_for_enum(&input.ident, input.variants.iter());

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
            match self {
                Enum::Empty => {
                    0_u8.store(memory, location)?;
                    location = location
                        .after::<u8>()
                        .after_padding_for::<linera_witty::HList![]>()
                        .after_padding_for::<linera_witty::HList![i8, CustomType]>()
                        .after_padding_for::<linera_witty::HList![(), String]>();

                    linera_witty::hlist![].store(memory, location)
                }
                Enum::Tuple(field0, field1, _) => {
                    1_u8.store(memory, location)?;
                    location = location
                        .after::<u8>()
                        .after_padding_for::<linera_witty::HList![]>()
                        .after_padding_for::<linera_witty::HList![i8, CustomType]>()
                        .after_padding_for::<linera_witty::HList![(), String]>();

                    linera_witty::hlist![field0, field1].store(memory, location)
                }
                Enum::Struct { first, second, .. } => {
                    2_u8.store(memory, location)?;
                    location = location
                        .after::<u8>()
                        .after_padding_for::<linera_witty::HList![]>()
                        .after_padding_for::<linera_witty::HList![i8, CustomType]>()
                        .after_padding_for::<linera_witty::HList![(), String]>();

                    linera_witty::hlist![first, second].store(memory, location)
                }
            }
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
            match self {
                Enum::Empty => {
                    let variant_flat_layout = linera_witty::hlist![].lower(memory)?;
                    let flat_layout: <Self::Layout as linera_witty::Layout>::Flat =
                        linera_witty::JoinFlatLayouts::into_joined(
                            0_u8.lower(memory)? + variant_flat_layout,
                        );

                    Ok(flat_layout)
                }
                Enum::Tuple(field0, field1, _) => {
                    let variant_flat_layout = linera_witty::hlist![field0, field1].lower(memory)?;
                    let flat_layout: <Self::Layout as linera_witty::Layout>::Flat =
                        linera_witty::JoinFlatLayouts::into_joined(
                            1_u8.lower(memory)? + variant_flat_layout,
                        );

                    Ok(flat_layout)
                }
                Enum::Struct { first, second, .. } => {
                    let variant_flat_layout = linera_witty::hlist![first, second].lower(memory)?;
                    let flat_layout: <Self::Layout as linera_witty::Layout>::Flat =
                        linera_witty::JoinFlatLayouts::into_joined(
                            2_u8.lower(memory)? + variant_flat_layout,
                        );

                    Ok(flat_layout)
                }
            }
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}
