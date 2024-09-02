// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

use crate::{context::Context, views::View};

// TODO(#1326): fix the proliferation of constraints from this module

// TODO(#1858): come up with a better name-mangling scheme
/// Mangle a GraphQL type into something that can be interpolated into a GraphQL type name
fn mangle(type_name: impl AsRef<str>) -> String {
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

fn hash_name<T: ?Sized>() -> u32 {
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
struct Entry<K: async_graphql::OutputType + Send + Sync, V: async_graphql::OutputType + Send + Sync>
{
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

use crate::map_view::ByteMapView;
impl<C: Send + Sync, V: async_graphql::OutputType> async_graphql::TypeName for ByteMapView<C, V> {
    fn type_name() -> Cow<'static, str> {
        format!(
            "ByteMapView_{}_{:08x}",
            mangle(V::type_name()),
            hash_name::<V>()
        )
        .into()
    }
}
#[async_graphql::Object(cache_control(no_cache), name_type)]
impl<C, V> ByteMapView<C, V>
where
    C: Context + Send + Sync,
    V: async_graphql::OutputType
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + Send
        + Sync
        + 'static,
{
    #[graphql(derived(name = "keys"))]
    async fn keys_(&self, count: Option<usize>) -> Result<Vec<Vec<u8>>, async_graphql::Error> {
        let keys = self.keys().await?;
        let it = keys.iter().cloned();
        Ok(if let Some(count) = count {
            it.take(count).collect()
        } else {
            it.collect()
        })
    }

    async fn entry(&self, key: Vec<u8>) -> Result<Entry<Vec<u8>, Option<V>>, async_graphql::Error> {
        Ok(Entry {
            value: self.get(&key).await?,
            key,
        })
    }

    async fn entries(
        &self,
        input: Option<MapInput<Vec<u8>>>,
    ) -> Result<Vec<Entry<Vec<u8>, Option<V>>>, async_graphql::Error> {
        let keys = input
            .and_then(|input| input.filters)
            .and_then(|filters| filters.keys);
        let keys = if let Some(keys) = keys {
            keys
        } else {
            self.keys().await?
        };

        let mut entries = vec![];
        for key in keys {
            entries.push(Entry {
                value: self.get(&key).await?,
                key,
            })
        }

        Ok(entries)
    }
}

use crate::map_view::MapView;
impl<C: Send + Sync, I: async_graphql::OutputType, V: async_graphql::OutputType>
    async_graphql::TypeName for MapView<C, I, V>
{
    fn type_name() -> Cow<'static, str> {
        format!(
            "MapView_{}_{}_{:08x}",
            mangle(I::type_name()),
            mangle(V::type_name()),
            hash_name::<(I, V)>(),
        )
        .into()
    }
}
#[async_graphql::Object(cache_control(no_cache), name_type)]
impl<C, I, V> MapView<C, I, V>
where
    C: Context + Send + Sync,
    I: async_graphql::OutputType
        + async_graphql::InputType
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::fmt::Debug
        + Clone
        + Send
        + Sync
        + 'static,
    V: async_graphql::OutputType
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + Send
        + Sync
        + 'static,
{
    async fn keys(&self, count: Option<usize>) -> Result<Vec<I>, async_graphql::Error> {
        let indices = self.indices().await?;
        let it = indices.iter().cloned();
        Ok(if let Some(count) = count {
            it.take(count).collect()
        } else {
            it.collect()
        })
    }

    async fn entry(&self, key: I) -> Result<Entry<I, Option<V>>, async_graphql::Error> {
        Ok(Entry {
            value: self.get(&key).await?,
            key,
        })
    }

    async fn entries(
        &self,
        input: Option<MapInput<I>>,
    ) -> Result<Vec<Entry<I, Option<V>>>, async_graphql::Error> {
        let keys = input
            .and_then(|input| input.filters)
            .and_then(|filters| filters.keys);
        let keys = if let Some(keys) = keys {
            keys
        } else {
            self.indices().await?
        };

        let mut values = vec![];
        for key in keys {
            values.push(Entry {
                value: self.get(&key).await?,
                key,
            })
        }

        Ok(values)
    }
}

use crate::map_view::CustomMapView;
impl<C: Send + Sync, I: async_graphql::OutputType, V: async_graphql::OutputType>
    async_graphql::TypeName for CustomMapView<C, I, V>
{
    fn type_name() -> Cow<'static, str> {
        format!("CustomMapView_{}_{}", I::type_name(), V::type_name()).into()
    }
}
#[async_graphql::Object(cache_control(no_cache), name_type)]
impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context + Send + Sync,
    I: async_graphql::OutputType
        + async_graphql::InputType
        + crate::common::CustomSerialize
        + std::fmt::Debug
        + Clone
        + Send
        + Sync
        + 'static,
    V: async_graphql::OutputType
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + Send
        + Sync
        + 'static,
{
    async fn keys(&self, count: Option<usize>) -> Result<Vec<I>, async_graphql::Error> {
        let indices = self.indices().await?;
        let it = indices.iter().cloned();
        Ok(if let Some(count) = count {
            it.take(count).collect()
        } else {
            it.collect()
        })
    }

    async fn entry(&self, key: I) -> Result<Entry<I, Option<V>>, async_graphql::Error> {
        Ok(Entry {
            value: self.get(&key).await?,
            key,
        })
    }

    async fn entries(
        &self,
        input: Option<MapInput<I>>,
    ) -> Result<Vec<Entry<I, Option<V>>>, async_graphql::Error> {
        let keys = input
            .and_then(|input| input.filters)
            .and_then(|filters| filters.keys);
        let keys = if let Some(keys) = keys {
            keys
        } else {
            self.indices().await?
        };

        let mut values = vec![];
        for key in keys {
            values.push(Entry {
                value: self.get(&key).await?,
                key,
            })
        }

        Ok(values)
    }
}

use crate::register_view::RegisterView;
impl<C, T> async_graphql::OutputType for RegisterView<C, T>
where
    C: Context + Send + Sync,
    T: async_graphql::OutputType + Send + Sync,
{
    fn type_name() -> Cow<'static, str> {
        T::type_name()
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        T::create_type_info(registry)
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    async fn resolve(
        &self,
        ctx: &async_graphql::ContextSelectionSet<'_>,
        field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
    ) -> async_graphql::ServerResult<async_graphql::Value> {
        self.get().resolve(ctx, field).await
    }
}

use crate::collection_view::ReadGuardedView;
impl<'value, T: async_graphql::OutputType> async_graphql::OutputType
    for ReadGuardedView<'value, T>
{
    fn type_name() -> Cow<'static, str> {
        T::type_name()
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        T::create_type_info(registry)
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    async fn resolve(
        &self,
        ctx: &async_graphql::ContextSelectionSet<'_>,
        field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
    ) -> async_graphql::ServerResult<async_graphql::Value> {
        (**self).resolve(ctx, field).await
    }
}

use crate::collection_view::CollectionView;
impl<C: Send + Sync, K: async_graphql::OutputType, V: async_graphql::OutputType>
    async_graphql::TypeName for CollectionView<C, K, V>
{
    fn type_name() -> Cow<'static, str> {
        format!(
            "CollectionView_{}_{}_{:08x}",
            mangle(K::type_name()),
            mangle(V::type_name()),
            hash_name::<(K, V)>(),
        )
        .into()
    }
}
#[async_graphql::Object(cache_control(no_cache), name_type)]
impl<C, K, V> CollectionView<C, K, V>
where
    C: Send + Sync + Context,
    K: async_graphql::InputType
        + async_graphql::OutputType
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::fmt::Debug
        + Clone,
    V: View<C> + async_graphql::OutputType,
    MapInput<K>: async_graphql::InputType,
    MapFilters<K>: async_graphql::InputType,
{
    async fn keys(&self) -> Result<Vec<K>, async_graphql::Error> {
        Ok(self.indices().await?)
    }

    async fn entry(&self, key: K) -> Result<Entry<K, ReadGuardedView<V>>, async_graphql::Error> {
        let value = self
            .try_load_entry(&key)
            .await?
            .ok_or_else(|| missing_key_error(&key))?;
        Ok(Entry { value, key })
    }

    async fn entries(
        &self,
        input: Option<MapInput<K>>,
    ) -> Result<Vec<Entry<K, ReadGuardedView<V>>>, async_graphql::Error> {
        let keys = if let Some(keys) = input
            .and_then(|input| input.filters)
            .and_then(|filters| filters.keys)
        {
            keys
        } else {
            self.indices().await?
        };

        let mut values = vec![];
        for key in keys {
            let value = self
                .try_load_entry(&key)
                .await?
                .ok_or_else(|| missing_key_error(&key))?;
            values.push(Entry { value, key })
        }

        Ok(values)
    }
}

use crate::collection_view::CustomCollectionView;
impl<C: Send + Sync, K: async_graphql::OutputType, V: async_graphql::OutputType>
    async_graphql::TypeName for CustomCollectionView<C, K, V>
{
    fn type_name() -> Cow<'static, str> {
        format!(
            "CustomCollectionView_{}_{}_{:08x}",
            mangle(K::type_name()),
            mangle(V::type_name()),
            hash_name::<(K, V)>(),
        )
        .into()
    }
}
#[async_graphql::Object(cache_control(no_cache), name_type)]
impl<C, K, V> CustomCollectionView<C, K, V>
where
    C: Send + Sync + Context,
    K: async_graphql::InputType
        + async_graphql::OutputType
        + crate::common::CustomSerialize
        + std::fmt::Debug,
    V: View<C> + async_graphql::OutputType,
    MapInput<K>: async_graphql::InputType,
    MapFilters<K>: async_graphql::InputType,
{
    async fn keys(&self) -> Result<Vec<K>, async_graphql::Error> {
        Ok(self.indices().await?)
    }

    async fn entry(&self, key: K) -> Result<Entry<K, ReadGuardedView<V>>, async_graphql::Error> {
        let value = self
            .try_load_entry(&key)
            .await?
            .ok_or_else(|| missing_key_error(&key))?;
        Ok(Entry { value, key })
    }

    async fn entries(
        &self,
        input: Option<MapInput<K>>,
    ) -> Result<Vec<Entry<K, ReadGuardedView<V>>>, async_graphql::Error> {
        let keys = if let Some(keys) = input
            .and_then(|input| input.filters)
            .and_then(|filters| filters.keys)
        {
            keys
        } else {
            self.indices().await?
        };

        let mut values = vec![];
        for key in keys {
            let value = self
                .try_load_entry(&key)
                .await?
                .ok_or_else(|| missing_key_error(&key))?;
            values.push(Entry { value, key })
        }

        Ok(values)
    }
}

use crate::reentrant_collection_view as reentrant;
impl<T: async_graphql::OutputType> async_graphql::OutputType for reentrant::ReadGuardedView<T> {
    fn type_name() -> Cow<'static, str> {
        T::type_name()
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        T::create_type_info(registry)
    }

    async fn resolve(
        &self,
        ctx: &async_graphql::ContextSelectionSet<'_>,
        field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
    ) -> async_graphql::ServerResult<async_graphql::Value> {
        (**self).resolve(ctx, field).await
    }
}

use crate::reentrant_collection_view::ReentrantCollectionView;
impl<C: Send + Sync, K: async_graphql::OutputType, V: async_graphql::OutputType>
    async_graphql::TypeName for ReentrantCollectionView<C, K, V>
{
    fn type_name() -> Cow<'static, str> {
        format!(
            "ReentrantCollectionView_{}_{}_{}",
            mangle(K::type_name()),
            mangle(V::type_name()),
            hash_name::<(K, V)>(),
        )
        .into()
    }
}

fn missing_key_error(key: &impl std::fmt::Debug) -> async_graphql::Error {
    async_graphql::Error {
        message: format!("The key={:?} is missing in collection", key),
        source: None,
        extensions: None,
    }
}

#[async_graphql::Object(cache_control(no_cache), name_type)]
impl<C, K, V> ReentrantCollectionView<C, K, V>
where
    C: Send + Sync + Context,
    K: async_graphql::InputType
        + async_graphql::OutputType
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::fmt::Debug
        + Clone,
    V: View<C> + async_graphql::OutputType,
    MapInput<K>: async_graphql::InputType,
    MapFilters<K>: async_graphql::InputType,
{
    async fn keys(&self) -> Result<Vec<K>, async_graphql::Error> {
        Ok(self.indices().await?)
    }

    async fn entry(
        &self,
        key: K,
    ) -> Result<Entry<K, reentrant::ReadGuardedView<V>>, async_graphql::Error> {
        let value = self
            .try_load_entry(&key)
            .await?
            .ok_or_else(|| missing_key_error(&key))?;
        Ok(Entry { value, key })
    }

    async fn entries(
        &self,
        input: Option<MapInput<K>>,
    ) -> Result<Vec<Entry<K, reentrant::ReadGuardedView<V>>>, async_graphql::Error> {
        let keys = if let Some(keys) = input
            .and_then(|input| input.filters)
            .and_then(|filters| filters.keys)
        {
            keys
        } else {
            self.indices().await?
        };

        let mut values = vec![];
        for key in keys {
            let value = self
                .try_load_entry(&key)
                .await?
                .ok_or_else(|| missing_key_error(&key))?;
            values.push(Entry { value, key })
        }

        Ok(values)
    }
}

use crate::reentrant_collection_view::ReentrantCustomCollectionView;
impl<C: Send + Sync, K: async_graphql::OutputType, V: async_graphql::OutputType>
    async_graphql::TypeName for ReentrantCustomCollectionView<C, K, V>
{
    fn type_name() -> Cow<'static, str> {
        format!(
            "ReentrantCustomCollectionView_{}_{}_{:08x}",
            mangle(K::type_name()),
            mangle(V::type_name()),
            hash_name::<(K, V)>(),
        )
        .into()
    }
}
#[async_graphql::Object(cache_control(no_cache), name_type)]
impl<C, K, V> ReentrantCustomCollectionView<C, K, V>
where
    C: Send + Sync + Context,
    K: async_graphql::InputType
        + async_graphql::OutputType
        + crate::common::CustomSerialize
        + std::fmt::Debug
        + Clone,
    V: View<C> + async_graphql::OutputType,
    MapInput<K>: async_graphql::InputType,
    MapFilters<K>: async_graphql::InputType,
{
    async fn keys(&self) -> Result<Vec<K>, async_graphql::Error> {
        Ok(self.indices().await?)
    }

    async fn entry(
        &self,
        key: K,
    ) -> Result<Entry<K, reentrant::ReadGuardedView<V>>, async_graphql::Error> {
        let value = self
            .try_load_entry(&key)
            .await?
            .ok_or_else(|| missing_key_error(&key))?;
        Ok(Entry { value, key })
    }

    async fn entries(
        &self,
        input: Option<MapInput<K>>,
    ) -> Result<Vec<Entry<K, reentrant::ReadGuardedView<V>>>, async_graphql::Error> {
        let keys = if let Some(keys) = input
            .and_then(|input| input.filters)
            .and_then(|filters| filters.keys)
        {
            keys
        } else {
            self.indices().await?
        };

        let mut values = vec![];
        for key in keys {
            let value = self
                .try_load_entry(&key)
                .await?
                .ok_or_else(|| missing_key_error(&key))?;
            values.push(Entry { value, key })
        }

        Ok(values)
    }
}

use crate::set_view::SetView;
impl<C: Context, I: async_graphql::OutputType> async_graphql::OutputType for SetView<C, I>
where
    C: Send + Sync,
    I: serde::ser::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync,
{
    fn type_name() -> Cow<'static, str> {
        format!("[{}]", I::qualified_type_name()).into()
    }

    fn qualified_type_name() -> String {
        format!("[{}]!", I::qualified_type_name())
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        I::create_type_info(registry);
        Self::qualified_type_name()
    }

    async fn resolve(
        &self,
        ctx: &async_graphql::ContextSelectionSet<'_>,
        field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
    ) -> async_graphql::ServerResult<async_graphql::Value> {
        let indices = self
            .indices()
            .await
            .map_err(|e| async_graphql::Error::from(e).into_server_error(ctx.item.pos))?;
        let indices_len = indices.len();
        async_graphql::resolver_utils::resolve_list(ctx, field, indices, Some(indices_len)).await
    }
}

use crate::set_view::CustomSetView;
impl<C: Context, I: async_graphql::OutputType> async_graphql::OutputType for CustomSetView<C, I>
where
    C: Send + Sync,
    I: crate::common::CustomSerialize + Clone + Send + Sync,
{
    fn type_name() -> Cow<'static, str> {
        format!("[{}]", I::qualified_type_name()).into()
    }

    fn qualified_type_name() -> String {
        format!("[{}]!", I::qualified_type_name())
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        I::create_type_info(registry);
        Self::qualified_type_name()
    }

    async fn resolve(
        &self,
        ctx: &async_graphql::ContextSelectionSet<'_>,
        field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
    ) -> async_graphql::ServerResult<async_graphql::Value> {
        let indices = self
            .indices()
            .await
            .map_err(|e| async_graphql::Error::from(e).into_server_error(ctx.item.pos))?;
        let indices_len = indices.len();
        async_graphql::resolver_utils::resolve_list(ctx, field, indices, Some(indices_len)).await
    }
}

use crate::log_view::LogView;
impl<C: Send + Sync, T: async_graphql::OutputType> async_graphql::TypeName for LogView<C, T> {
    fn type_name() -> Cow<'static, str> {
        format!(
            "LogView_{}_{:08x}",
            mangle(T::type_name()),
            hash_name::<T>()
        )
        .into()
    }
}
#[async_graphql::Object(cache_control(no_cache), name_type)]
impl<C: Context, T: async_graphql::OutputType> LogView<C, T>
where
    C: Send + Sync,
    T: serde::ser::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync,
{
    async fn entries(
        &self,
        start: Option<usize>,
        end: Option<usize>,
    ) -> async_graphql::Result<Vec<T>> {
        Ok(self
            .read(start.unwrap_or_default()..end.unwrap_or_else(|| self.count()))
            .await?)
    }
}

use crate::hashable_wrapper::WrappedHashableContainerView;
impl<C, W, O> async_graphql::OutputType for WrappedHashableContainerView<C, W, O>
where
    C: Context + Send + Sync,
    W: async_graphql::OutputType + Send + Sync,
    O: Send + Sync,
{
    fn type_name() -> Cow<'static, str> {
        W::type_name()
    }

    fn qualified_type_name() -> String {
        W::qualified_type_name()
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        W::create_type_info(registry)
    }

    async fn resolve(
        &self,
        ctx: &async_graphql::ContextSelectionSet<'_>,
        field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
    ) -> async_graphql::ServerResult<async_graphql::Value> {
        (**self).resolve(ctx, field).await
    }
}

use crate::queue_view::QueueView;
impl<C: Send + Sync, T: async_graphql::OutputType> async_graphql::TypeName for QueueView<C, T> {
    fn type_name() -> Cow<'static, str> {
        format!(
            "QueueView_{}_{:08x}",
            mangle(T::type_name()),
            hash_name::<T>()
        )
        .into()
    }
}
#[async_graphql::Object(cache_control(no_cache), name_type)]
impl<C: Context, T: async_graphql::OutputType> QueueView<C, T>
where
    C: Send + Sync,
    T: serde::ser::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync,
{
    async fn entries(&self, count: Option<usize>) -> async_graphql::Result<Vec<T>> {
        Ok(self
            .read_front(count.unwrap_or_else(|| self.count()))
            .await?)
    }
}
