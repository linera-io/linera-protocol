// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

// TODO(#1858): come up with a better name-mangling scheme
/// Mangle a GraphQL type into something that can be interpolated into a GraphQL type name
pub(crate) fn mangle(type_name: impl AsRef<str>) -> String {
    let mut mangled = String::new();

    for c in type_name.as_ref().chars() {
        match c {
            '!' | '[' => (),
            ']' => mangled.push_str("_Array"),
            c => mangled.push(c),
        }
    }

    mangled
}

pub(crate) fn hash_name<T: ?Sized>() -> u32 {
    use sha3::Digest as _;
    u32::from_le_bytes(
        sha3::Sha3_256::new_with_prefix(std::any::type_name::<T>()).finalize()[..4]
            .try_into()
            .unwrap(),
    )
}

/// A GraphQL-visible map item, complete with key.
#[derive(async_graphql::SimpleObject)]
#[graphql(name_type)]
pub struct Entry<
    K: async_graphql::OutputType + Send + Sync,
    V: async_graphql::OutputType + Send + Sync,
> {
    pub key: K,
    pub value: V,
}

impl<K: async_graphql::OutputType, V: async_graphql::OutputType> async_graphql::TypeName
    for Entry<K, V>
{
    fn type_name() -> Cow<'static, str> {
        format!(
            "Entry_{}_{}_{:08x}",
            mangle(K::type_name()),
            mangle(V::type_name()),
            hash_name::<(K, V)>(),
        )
        .into()
    }
}

/// A struct to use to filter map values via GraphQL.
pub struct MapFilters<K: async_graphql::InputType> {
    pub keys: Option<Vec<K>>,
}

/// The inputs given when inspecting a map value via GraphQL.
pub struct MapInput<K: async_graphql::InputType> {
    pub filters: Option<MapFilters<K>>,
}

impl<K: async_graphql::InputType> async_graphql::InputType for MapFilters<K> {
    type RawValueType = Self;
    fn type_name() -> Cow<'static, str> {
        Cow::Owned(format!(
            "MapFilters_{}_{:08x}",
            mangle(K::type_name()),
            hash_name::<K>(),
        ))
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        registry.create_input_type::<Self, _>(
            async_graphql::registry::MetaTypeId::InputObject,
            |registry| async_graphql::registry::MetaType::InputObject {
                name: Self::type_name().into_owned(),
                description: None,
                input_fields: [(
                    "keys".to_owned(),
                    async_graphql::registry::MetaInputValue {
                        name: "keys".to_string(),
                        description: None,
                        ty: Option::<Vec<K>>::create_type_info(registry),
                        default_value: None,
                        visible: None,
                        inaccessible: false,
                        tags: Vec::new(),
                        is_secret: false,
                    },
                )]
                .into_iter()
                .collect(),
                visible: None,
                inaccessible: false,
                tags: Vec::new(),
                rust_typename: Some(std::any::type_name::<Self>()),
                oneof: false,
            },
        )
    }

    fn parse(value: Option<async_graphql::Value>) -> async_graphql::InputValueResult<Self> {
        let Some(async_graphql::Value::Object(obj)) = value else {
            return Err(async_graphql::InputValueError::expected_type(
                value.unwrap_or_default(),
            ));
        };
        Ok(Self {
            keys: async_graphql::InputType::parse(obj.get("keys").cloned())
                .map_err(async_graphql::InputValueError::propagate)?,
        })
    }

    fn to_value(&self) -> async_graphql::Value {
        let mut map = async_graphql::indexmap::IndexMap::new();
        map.insert(async_graphql::Name::new("keys"), self.keys.to_value());
        async_graphql::Value::Object(map)
    }

    fn federation_fields() -> Option<String> {
        Some(format!(
            // for each field
            "{{ {0} }}",
            if let Some(fields) = Vec::<K>::federation_fields() {
                format!("{0} {1}", "keys", fields)
            } else {
                "keys".to_string()
            }
        ))
    }

    fn as_raw_value(&self) -> Option<&Self::RawValueType> {
        Some(self)
    }
}

impl<K: async_graphql::InputType> async_graphql::InputType for MapInput<K> {
    type RawValueType = Self;
    fn type_name() -> Cow<'static, str> {
        Cow::Owned(format!(
            "MapInput_{}_{:08x}",
            mangle(K::type_name()),
            hash_name::<K>(),
        ))
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        registry.create_input_type::<Self, _>(
            async_graphql::registry::MetaTypeId::InputObject,
            |registry| async_graphql::registry::MetaType::InputObject {
                name: Self::type_name().into_owned(),
                description: None,
                input_fields: [(
                    "filters".to_owned(),
                    async_graphql::registry::MetaInputValue {
                        name: "filters".to_string(),
                        description: None,
                        ty: Option::<MapFilters<K>>::create_type_info(registry),
                        default_value: None,
                        visible: None,
                        inaccessible: false,
                        tags: Vec::new(),
                        is_secret: false,
                    },
                )]
                .into_iter()
                .collect(),
                visible: None,
                inaccessible: false,
                tags: Vec::new(),
                rust_typename: Some(std::any::type_name::<Self>()),
                oneof: false,
            },
        )
    }

    fn parse(value: Option<async_graphql::Value>) -> async_graphql::InputValueResult<Self> {
        let Some(async_graphql::Value::Object(obj)) = value else {
            return Err(async_graphql::InputValueError::expected_type(
                value.unwrap_or_default(),
            ));
        };
        Ok(Self {
            filters: async_graphql::InputType::parse(obj.get("filters").cloned())
                .map_err(async_graphql::InputValueError::propagate)?,
        })
    }

    fn to_value(&self) -> async_graphql::Value {
        let mut map = async_graphql::indexmap::IndexMap::new();
        map.insert(async_graphql::Name::new("filters"), self.filters.to_value());
        async_graphql::Value::Object(map)
    }

    fn federation_fields() -> Option<String> {
        Some(format!(
            // for each field
            "{{ {0} }}",
            if let Some(fields) = MapFilters::<K>::federation_fields() {
                format!("{0} {1}", "filters", fields)
            } else {
                "filters".to_string()
            }
        ))
    }

    fn as_raw_value(&self) -> Option<&Self::RawValueType> {
        Some(self)
    }
}

pub(crate) fn missing_key_error(key: &impl std::fmt::Debug) -> async_graphql::Error {
    async_graphql::Error {
        message: format!("The key={:?} is missing in collection", key),
        source: None,
        extensions: None,
    }
}
