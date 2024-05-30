// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
A helper module for combining `Results` with lists of items that must be accumulated as we go.

In many places we would like to talk about some result with a list of items carried along
— `(Result<T, E>, Items)`.  However, using this type in Rust forbids us from using the `?`
operator, which is very inconvenient.

This module contains helper extension traits to help us factor `Result<(T, X), (E, X)>`
into `(Result<T, E>, X)` for pattern-matching and distribute it back into `Result<(T, X),
(E, X)>` for use with `?`.

It also contains traits to help us accumulate lists of items returned in `Result`s by
subfunctions.
*/

use std::iter::IntoIterator;

/// A convenience alias for `Result`s that carry data alongside them.
pub type ResultWith<X, T, E> = Result<(T, X), (E, X)>;

/**
A trait for collecting items returned alongside a return value into a accumulator.

```rust,ignore
fn foo() -> (u8, Vec<Item>) {
    unimplemented!()
}

fn bar() -> (u8, Vec<Item>) {
    unimplemented!()
}

fn baz() -> (u8, Vec<Item>) {
    let mut items = vec![];
    let answer1 = foo().accumulate(&mut items);
    let answer2 = bar().accumulate(&mut items);
    (answer1 + answer2, items)
}
```
*/
pub trait AccumulateExt {
    type Item;
    type Output;

    fn accumulate(self, accumulator: &mut impl Extend<Self::Item>) -> Self::Output;
}

impl<R, Xs: IntoIterator> AccumulateExt for (R, Xs) {
    type Item = Xs::Item;
    type Output = R;

    fn accumulate(self, accumulator: &mut impl Extend<Self::Item>) -> Self::Output {
        accumulator.extend(self.1);
        self.0
    }
}

impl<Xs: IntoIterator, T, E> AccumulateExt for ResultWith<Xs, T, E> {
    type Item = Xs::Item;
    type Output = Result<T, E>;

    fn accumulate(self, accumulator: &mut impl Extend<Self::Item>) -> Self::Output {
        self.factor().accumulate(accumulator)
    }
}

/**
A trait to help early return using `?`.  Specialized to `Result`, like `AccumulateExt` it
collects the items into the provided accumulator, but in the case of an `Err` variant then
removes them from the accumulator (replacing the accumulator with `Default::default()`)
and bundles them into the `Err` variant, to be returned by `?`.

```rust,ignore
fn foo() -> ResultWith<Vec<Item>, u8, Error> {
    unimplemented!()
}

fn bar() -> ResultWith<Vec<Item>, u8, Error> {
    unimplemented!()
}

fn baz() -> ResultWith<Vec<Item>, u8, Error> {
    let mut items = vec![];
    let answer1 = foo().accumulate(&mut items)?;
    let answer2 = bar().accumulate(&mut items)?;
    Ok((answer1 + answer2, items))
}
```
 */
pub trait TryAccumulateExt: AccumulateExt {
    type Ok;
    type Err;

    fn try_accumulate<Accumulator: Default + Extend<Self::Item>>(self, accumulator: &mut Accumulator) -> Result<Self::Ok, (Self::Err, Accumulator)>;
}

impl<Xs: IntoIterator, T, E> TryAccumulateExt for (Result<T, E>, Xs) {
    type Ok = T;
    type Err = E;

    fn try_accumulate<Accumulator: Default + Extend<Self::Item>>(self, accumulator: &mut Accumulator) -> Result<Self::Ok, (Self::Err, Accumulator)> {
        match self.accumulate(accumulator) {
            Ok(x) => Ok(x),
            Err(e) => Err((e, std::mem::replace(accumulator, Default::default()))),
        }
    }
}

impl<Xs: IntoIterator, T, E> TryAccumulateExt for ResultWith<Xs, T, E> {
    type Ok = T;
    type Err = E;

    fn try_accumulate<Accumulator: Default + Extend<Self::Item>>(self, accumulator: &mut Accumulator)
    -> Result<<Self as TryAccumulateExt>::Ok, (<Self as TryAccumulateExt>::Err, Accumulator)> {
        self.factor().try_accumulate(accumulator)
    }
}


pub trait DistributeExt {
    type Ok;
    type Err;
    type Scalar;

    fn distribute<E_: From<Self::Err>>(self) -> Result<(Self::Ok, Self::Scalar), (E_, Self::Scalar)>;
}

impl<T, E, X> DistributeExt for (Result<T, E>, X) {
    type Ok = T;
    type Err = E;
    type Scalar = X;

    fn distribute<E_: From<E>>(self) -> ResultWith<X, T, E_> {
        match self.0 {
            Ok(x) => Ok((x, self.1)),
            Err(e) => Err((e.into(), self.1)),
        }
    }
}


pub trait FactorExt {
    type Ok;
    type Err;
    type Scalar;

    fn factor(self) -> (Result<Self::Ok, Self::Err>, Self::Scalar);
}

impl<T, E, X> FactorExt for ResultWith<X, T, E> {
    type Ok = T;
    type Err = E;
    type Scalar = X;

    fn factor(self) -> (Result<T, E>, X) {
        match self {
            Ok((x, scalar)) => (Ok(x), scalar),
            Err((e, scalar)) => (Err(e), scalar),
        }
    }
}
