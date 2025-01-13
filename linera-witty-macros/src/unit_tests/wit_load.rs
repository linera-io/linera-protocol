// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the `WitLoad` derive macro.

#![cfg(test)]

use quote::quote;
use syn::{parse_quote, Fields, ItemEnum, ItemStruct};

use super::{derive_for_enum, derive_for_struct};

/// Checks the generated code for the body of the implementation of `WitLoad` for a unit struct.
#[test]
fn zero_sized_type() {
    let input = Fields::Unit;
    let output = derive_for_struct(&input);

    let expected = quote! {
        fn load<Instance>(
            memory: &linera_witty::Memory<'_, Instance>,
            mut location: linera_witty::GuestPointer,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let linera_witty::hlist_pat![] =
                <linera_witty::HList![] as linera_witty::WitLoad>::load(memory, location)?;

            Ok(Self)
        }

        fn lift_from<Instance>(
            flat_layout: <Self::Layout as linera_witty::Layout>::Flat,
            memory: &linera_witty::Memory<'_, Instance>,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let linera_witty::hlist_pat![] =
                <linera_witty::HList![] as linera_witty::WitLoad>::lift_from(flat_layout, memory)?;

            Ok(Self)
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitLoad` for a named struct.
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
        fn load<Instance>(
            memory: &linera_witty::Memory<'_, Instance>,
            mut location: linera_witty::GuestPointer,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let linera_witty::hlist_pat![first, second] =
                <linera_witty::HList![u8, CustomType] as linera_witty::WitLoad>::load(
                    memory,
                    location
                )?;

            Ok(Self { first, second })
        }

        fn lift_from<Instance>(
            flat_layout: <Self::Layout as linera_witty::Layout>::Flat,
            memory: &linera_witty::Memory<'_, Instance>,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let linera_witty::hlist_pat![first, second] =
                <linera_witty::HList![u8, CustomType] as linera_witty::WitLoad>::lift_from(
                    flat_layout,
                    memory
                )?;

            Ok(Self { first, second })
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitLoad` for a tuple struct.
#[test]
fn tuple_struct() {
    let input: ItemStruct = parse_quote! {
        struct Type(String, Vec<CustomType>, i64);
    };
    let output = derive_for_struct(&input.fields);

    let expected = quote! {
        fn load<Instance>(
            memory: &linera_witty::Memory<'_, Instance>,
            mut location: linera_witty::GuestPointer,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let linera_witty::hlist_pat![field0, field1, field2] = <linera_witty::HList![
                String,
                Vec<CustomType>,
                i64
            ] as linera_witty::WitLoad>::load(memory, location)?;

            Ok(Self(field0, field1, field2))
        }

        fn lift_from<Instance>(
            flat_layout: <Self::Layout as linera_witty::Layout>::Flat,
            memory: &linera_witty::Memory<'_, Instance>,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let linera_witty::hlist_pat![field0, field1, field2] = <linera_witty::HList![
                String,
                Vec<CustomType>,
                i64
            ] as linera_witty::WitLoad>::lift_from(flat_layout, memory)?;

            Ok(Self(field0, field1, field2))
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitType` for an enum.
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
        fn load<Instance>(
            memory: &linera_witty::Memory<'_, Instance>,
            mut location: linera_witty::GuestPointer,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let discriminant = <u8 as linera_witty::WitLoad>::load(memory, location,)?;
            location = location
                .after::<u8>()
                .after_padding_for::<linera_witty::HList![]>()
                .after_padding_for::<linera_witty::HList![i8, CustomType]>()
                .after_padding_for::<linera_witty::HList![(), String]>();

            match discriminant {
                0 => {
                    let linera_witty::hlist_pat![] =
                        <linera_witty::HList![] as linera_witty::WitLoad>::load(memory, location)?;

                    Ok(Enum::Empty)
                }
                1 => {
                    let linera_witty::hlist_pat![field0, field1] =
                        <linera_witty::HList![i8, CustomType] as linera_witty::WitLoad>::load(
                            memory,
                            location
                        )?;

                    Ok(Enum::Tuple(field0, field1))
                }
                2 => {
                    let linera_witty::hlist_pat![first, second] =
                        <linera_witty::HList![(), String] as linera_witty::WitLoad>::load(
                            memory,
                            location
                        )?;

                    Ok(Enum::Struct { first, second })
                }
                discriminant => Err(linera_witty::RuntimeError::InvalidVariant {
                    type_name: ::std::any::type_name::<Self>(),
                    discriminant: discriminant.into(),
                }),
            }
        }

        fn lift_from<Instance>(
            linera_witty::hlist_pat![discriminant_flat_type, ...flat_layout]:
                <Self::Layout as linera_witty::Layout>::Flat,
            memory: &linera_witty::Memory<'_, Instance>,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let discriminant = <u8 as linera_witty::WitLoad>::lift_from(
                linera_witty::hlist![discriminant_flat_type],
                memory,
            )?;

            match discriminant {
                0 => {
                    let linera_witty::hlist_pat![] =
                        <linera_witty::HList![] as linera_witty::WitLoad>::lift_from(
                            linera_witty::JoinFlatLayouts::from_joined(flat_layout),
                            memory,
                        )?;

                    Ok(Enum::Empty)
                }
                1 => {
                    let linera_witty::hlist_pat![field0, field1] =
                        <linera_witty::HList![i8, CustomType] as linera_witty::WitLoad>::lift_from(
                            linera_witty::JoinFlatLayouts::from_joined(flat_layout),
                            memory,
                        )?;

                    Ok(Enum::Tuple(field0, field1))
                }
                2 => {
                    let linera_witty::hlist_pat![first, second] =
                        <linera_witty::HList![(), String] as linera_witty::WitLoad>::lift_from(
                            linera_witty::JoinFlatLayouts::from_joined(flat_layout),
                            memory,
                        )?;

                    Ok(Enum::Struct { first, second })
                }
                discriminant => Err(linera_witty::RuntimeError::InvalidVariant {
                    type_name: ::std::any::type_name::<Self>(),
                    discriminant: discriminant.into(),
                }),
            }
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitLoad` for a named struct
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
        fn load<Instance>(
            memory: &linera_witty::Memory<'_, Instance>,
            mut location: linera_witty::GuestPointer,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let linera_witty::hlist_pat![first, second] =
                <linera_witty::HList![u8, CustomType] as linera_witty::WitLoad>::load(
                    memory,
                    location
                )?;

            let ignored1 = Default::default();
            let ignored2 = Default::default();
            let ignored3 = Default::default();
            let ignored4 = Default::default();

            Ok(Self {
                ignored1,
                first,
                ignored2,
                ignored3,
                second,
                ignored4
            })
        }

        fn lift_from<Instance>(
            flat_layout: <Self::Layout as linera_witty::Layout>::Flat,
            memory: &linera_witty::Memory<'_, Instance>,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let linera_witty::hlist_pat![first, second] =
                <linera_witty::HList![u8, CustomType] as linera_witty::WitLoad>::lift_from(
                    flat_layout,
                    memory
                )?;

            let ignored1 = Default::default();
            let ignored2 = Default::default();
            let ignored3 = Default::default();
            let ignored4 = Default::default();

            Ok(Self {
                ignored1,
                first,
                ignored2,
                ignored3,
                second,
                ignored4
            })
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitLoad` for a tuple struct
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
        fn load<Instance>(
            memory: &linera_witty::Memory<'_, Instance>,
            mut location: linera_witty::GuestPointer,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let linera_witty::hlist_pat![field1, field2, field4] = <linera_witty::HList![
                String,
                Vec<CustomType>,
                i64
            ] as linera_witty::WitLoad>::load(memory, location)?;

            let field0 = Default::default();
            let field3 = Default::default();

            Ok(Self(field0, field1, field2, field3, field4))
        }

        fn lift_from<Instance>(
            flat_layout: <Self::Layout as linera_witty::Layout>::Flat,
            memory: &linera_witty::Memory<'_, Instance>,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let linera_witty::hlist_pat![field1, field2, field4] = <linera_witty::HList![
                String,
                Vec<CustomType>,
                i64
            ] as linera_witty::WitLoad>::lift_from(flat_layout, memory)?;

            let field0 = Default::default();
            let field3 = Default::default();

            Ok(Self(field0, field1, field2, field3, field4))
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitType` for an enum with some
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
        fn load<Instance>(
            memory: &linera_witty::Memory<'_, Instance>,
            mut location: linera_witty::GuestPointer,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let discriminant = <u8 as linera_witty::WitLoad>::load(memory, location,)?;
            location = location
                .after::<u8>()
                .after_padding_for::<linera_witty::HList![]>()
                .after_padding_for::<linera_witty::HList![i8, CustomType]>()
                .after_padding_for::<linera_witty::HList![(), String]>();

            match discriminant {
                0 => {
                    let linera_witty::hlist_pat![] =
                        <linera_witty::HList![] as linera_witty::WitLoad>::load(memory, location)?;

                    Ok(Enum::Empty)
                }
                1 => {
                    let linera_witty::hlist_pat![field0, field1] =
                        <linera_witty::HList![i8, CustomType] as linera_witty::WitLoad>::load(
                            memory,
                            location
                        )?;

                    let field2 = Default::default();

                    Ok(Enum::Tuple(field0, field1, field2))
                }
                2 => {
                    let linera_witty::hlist_pat![first, second] =
                        <linera_witty::HList![(), String] as linera_witty::WitLoad>::load(
                            memory,
                            location
                        )?;

                    let ignored1 = Default::default();
                    let ignored2 = Default::default();
                    let ignored3 = Default::default();

                    Ok(Enum::Struct {
                        first,
                        ignored1,
                        ignored2,
                        second,
                        ignored3
                    })
                }
                discriminant => Err(linera_witty::RuntimeError::InvalidVariant {
                    type_name: ::std::any::type_name::<Self>(),
                    discriminant: discriminant.into(),
                }),
            }
        }

        fn lift_from<Instance>(
            linera_witty::hlist_pat![discriminant_flat_type, ...flat_layout]:
                <Self::Layout as linera_witty::Layout>::Flat,
            memory: &linera_witty::Memory<'_, Instance>,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let discriminant = <u8 as linera_witty::WitLoad>::lift_from(
                linera_witty::hlist![discriminant_flat_type],
                memory,
            )?;

            match discriminant {
                0 => {
                    let linera_witty::hlist_pat![] =
                        <linera_witty::HList![] as linera_witty::WitLoad>::lift_from(
                            linera_witty::JoinFlatLayouts::from_joined(flat_layout),
                            memory,
                        )?;

                    Ok(Enum::Empty)
                }
                1 => {
                    let linera_witty::hlist_pat![field0, field1] =
                        <linera_witty::HList![i8, CustomType] as linera_witty::WitLoad>::lift_from(
                            linera_witty::JoinFlatLayouts::from_joined(flat_layout),
                            memory,
                        )?;

                    let field2 = Default::default();

                    Ok(Enum::Tuple(field0, field1, field2))
                }
                2 => {
                    let linera_witty::hlist_pat![first, second] =
                        <linera_witty::HList![(), String] as linera_witty::WitLoad>::lift_from(
                            linera_witty::JoinFlatLayouts::from_joined(flat_layout),
                            memory,
                        )?;

                    let ignored1 = Default::default();
                    let ignored2 = Default::default();
                    let ignored3 = Default::default();

                    Ok(Enum::Struct {
                        first,
                        ignored1,
                        ignored2,
                        second,
                        ignored3
                    })
                }
                discriminant => Err(linera_witty::RuntimeError::InvalidVariant {
                    type_name: ::std::any::type_name::<Self>(),
                    discriminant: discriminant.into(),
                }),
            }
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}
