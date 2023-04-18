// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Defines a GraphQL scalar with a description string.
///
/// This is equivalent to `scalar!` but always uses the stringified identifier as the name.
#[macro_export]
macro_rules! doc_scalar {
    ($ty:ty, $desc:literal) => {
        $crate::async_graphql::scalar_internal!(
            $ty,
            ::std::stringify!($ty),
            ::std::option::Option::Some(::std::string::ToString::to_string($desc)),
            ::std::option::Option::None
        );
    };
}

/// An error trying to parse the hex-digits of a BCS-encoded value.
#[derive(thiserror::Error, Debug)]
pub enum BcsHexParseError {
    #[error("Invalid BCS: {0}")]
    Bcs(#[from] bcs::Error),
    #[error("Invalid hexadecimal: {0}")]
    Hex(#[from] hex::FromHexError),
}

/// Defines a GraphQL scalar type using the hex-representation of the value's BCS-serialized form.
///
/// This is a modified implementation of [`async_graphql::scalar`].
/// In addition, it implements `Display` and `FromStr`, also using hex-representation.
#[macro_export]
macro_rules! bcs_scalar {
    ($ty:ty, $desc:literal) => {
        impl $crate::async_graphql::ScalarType for $ty {
            fn parse(
                value: $crate::async_graphql::Value,
            ) -> $crate::async_graphql::InputValueResult<Self> {
                let hex: String = $crate::async_graphql::from_value(value)?;
                let bytes = $crate::hex::decode(&hex)?;
                let result = $crate::bcs::from_bytes(&bytes)?;
                ::std::result::Result::Ok(result)
            }

            fn to_value(&self) -> $crate::async_graphql::Value {
                let ::std::result::Result::Ok(bytes) = $crate::bcs::to_bytes(self) else {
                                                    return $crate::async_graphql::Value::Null;
                                                };
                let hex = $crate::hex::encode(&bytes);
                $crate::async_graphql::to_value(hex)
                    .unwrap_or_else(|_| $crate::async_graphql::Value::Null)
            }
        }

        impl $crate::async_graphql::InputType for $ty {
            type RawValueType = Self;

            fn type_name() -> ::std::borrow::Cow<'static, ::std::primitive::str> {
                ::std::borrow::Cow::Borrowed(::std::stringify!($ty))
            }

            fn create_type_info(
                registry: &mut $crate::async_graphql::registry::Registry,
            ) -> ::std::string::String {
                registry.create_input_type::<$ty, _>(
                    $crate::async_graphql::registry::MetaTypeId::Scalar,
                    |_| $crate::async_graphql::registry::MetaType::Scalar {
                        name: ::std::borrow::ToOwned::to_owned(::std::stringify!($ty)),
                        description: ::std::option::Option::Some(
                            ::std::string::ToString::to_string($desc)
                        ),
                        is_valid: ::std::option::Option::Some(::std::sync::Arc::new(|value| {
                            <$ty as $crate::async_graphql::ScalarType>::is_valid(value)
                        })),
                        visible: ::std::option::Option::None,
                        inaccessible: false,
                        tags: ::std::default::Default::default(),
                        specified_by_url: ::std::option::Option::None,
                    },
                )
            }

            fn parse(
                value: ::std::option::Option<$crate::async_graphql::Value>,
            ) -> $crate::async_graphql::InputValueResult<Self> {
                <$ty as $crate::async_graphql::ScalarType>::parse(value.unwrap_or_default())
            }

            fn to_value(&self) -> $crate::async_graphql::Value {
                <$ty as $crate::async_graphql::ScalarType>::to_value(self)
            }

            fn as_raw_value(&self) -> ::std::option::Option<&Self::RawValueType> {
                ::std::option::Option::Some(self)
            }
        }

        #[$crate::async_graphql::async_trait::async_trait]
        impl $crate::async_graphql::OutputType for $ty {
            fn type_name() -> ::std::borrow::Cow<'static, ::std::primitive::str> {
                ::std::borrow::Cow::Borrowed(::std::stringify!($ty))
            }

            fn create_type_info(
                registry: &mut $crate::async_graphql::registry::Registry,
            ) -> ::std::string::String {
                registry.create_output_type::<$ty, _>(
                    $crate::async_graphql::registry::MetaTypeId::Scalar,
                    |_| $crate::async_graphql::registry::MetaType::Scalar {
                        name: ::std::borrow::ToOwned::to_owned(::std::stringify!($ty)),
                        description: ::std::option::Option::Some(
                            ::std::string::ToString::to_string($desc)
                        ),
                        is_valid: ::std::option::Option::Some(::std::sync::Arc::new(|value| {
                            <$ty as $crate::async_graphql::ScalarType>::is_valid(value)
                        })),
                        visible: ::std::option::Option::None,
                        inaccessible: false,
                        tags: ::std::default::Default::default(),
                        specified_by_url: ::std::option::Option::None,
                    },
                )
            }

            async fn resolve(
                &self,
                _: &$crate::async_graphql::ContextSelectionSet<'_>,
                _field: &$crate::async_graphql::Positioned<
                    $crate::async_graphql::parser::types::Field,
                >,
            ) -> $crate::async_graphql::ServerResult<$crate::async_graphql::Value> {
                ::std::result::Result::Ok($crate::async_graphql::ScalarType::to_value(self))
            }
        }

        impl ::std::fmt::Display for $ty {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>)
                -> ::std::result::Result<(), ::std::fmt::Error>
            {
                match $crate::bcs::to_bytes(self) {
                    ::std::result::Result::Ok(bytes) => {
                        ::std::fmt::Display::fmt(&$crate::hex::encode(&bytes), f)
                    }
                    ::std::result::Result::Err(_) => {
                        ::std::write!(f, "invalid {}", ::std::stringify!($ty))
                    }
                }
            }
        }

        impl ::std::str::FromStr for $ty {
            type Err = $crate::BcsHexParseError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let bytes = $crate::hex::decode(s)?;
                ::std::result::Result::Ok($crate::bcs::from_bytes(&bytes)?)
            }
        }
    };
}
