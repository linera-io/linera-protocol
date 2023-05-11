// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A custom `ScalarType` implementation for `Balance`.
//!
//! GraphQL doesn't support `u128`, so we represent a balance as a list with two elements, the most
//! and least significant 64 bits.
//!
//! This is mostly adapted from the implementation of the `async_graphql::scalar!` macro.

use std::{borrow::Cow, iter, sync::Arc};

use async_graphql::{
    parser::types::Field,
    registry::{MetaType, MetaTypeId, Registry},
    ContextSelectionSet, InputType, InputValueError, InputValueResult, Name, OutputType,
    Positioned, ScalarType, ServerResult, Value,
};

use super::Balance;

const BALANCE_DESCRIPTION: &str = "The balance of a chain";
const UPPER: &str = "upper";
const LOWER: &str = "lower";

impl ScalarType for Balance {
    fn parse(value: Value) -> InputValueResult<Self> {
        let Value::Object(object) = value else {
            return Err(InputValueError::custom(format!("unexpected value type: {:?}", value)));
        };
        let (2, Some(Value::Number(upper)), Some(Value::Number(lower))) =
            (object.len(), object.get(UPPER), object.get(LOWER))
        else {
            return Err(InputValueError::custom(format!("unexpected object: {:?}", object)));
        };
        let upper = upper
            .as_u64()
            .ok_or_else(|| InputValueError::custom(format!("unexpected number: {:?}", upper)))?;
        let lower = lower
            .as_u64()
            .ok_or_else(|| InputValueError::custom(format!("unexpected number: {:?}", lower)))?;
        Ok(Balance(((upper as u128) << 64) + (lower as u128)))
    }

    fn to_value(&self) -> Value {
        let object = iter::once((Name::new(UPPER), self.upper_half().into()))
            .chain(iter::once((Name::new(LOWER), self.lower_half().into())))
            .collect();
        Value::Object(object)
    }
}

impl InputType for Balance {
    type RawValueType = Self;

    fn type_name() -> Cow<'static, str> {
        Cow::Borrowed(stringify!(Balance))
    }

    fn create_type_info(registry: &mut Registry) -> String {
        registry.create_input_type::<Balance, _>(MetaTypeId::Scalar, |_| MetaType::Scalar {
            name: stringify!(Balance).to_owned(),
            description: Some(BALANCE_DESCRIPTION.to_string()),
            is_valid: None,
            visible: None,
            inaccessible: false,
            tags: Default::default(),
            specified_by_url: None,
        })
    }

    fn parse(value: Option<Value>) -> InputValueResult<Self> {
        <Balance as ScalarType>::parse(value.unwrap_or_default())
    }

    fn to_value(&self) -> Value {
        <Balance as ScalarType>::to_value(self)
    }

    fn as_raw_value(&self) -> Option<&Self::RawValueType> {
        Some(self)
    }
}

#[async_graphql::async_trait::async_trait]
impl OutputType for Balance {
    fn type_name() -> Cow<'static, str> {
        Cow::Borrowed(stringify!(Balance))
    }

    fn create_type_info(registry: &mut Registry) -> String {
        registry.create_output_type::<Balance, _>(MetaTypeId::Scalar, |_| MetaType::Scalar {
            name: stringify!(Balance).to_owned(),
            description: Some(BALANCE_DESCRIPTION.to_string()),
            is_valid: Some(Arc::new(<Balance as ScalarType>::is_valid)),
            visible: None,
            inaccessible: false,
            tags: Default::default(),
            specified_by_url: None,
        })
    }

    async fn resolve(
        &self,
        _: &ContextSelectionSet<'_>,
        _field: &Positioned<Field>,
    ) -> ServerResult<Value> {
        Ok(ScalarType::to_value(self))
    }
}
