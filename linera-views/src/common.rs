// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This provides some common code for the linera-views.

use std::{
    collections::BTreeSet,
    ops::{
        Bound,
        Bound::{Excluded, Included, Unbounded},
    },
};

use serde::de::DeserializeOwned;

use crate::views::ViewError;

#[doc(hidden)]
pub type HasherOutputSize = <sha3::Sha3_256 as sha3::digest::OutputSizeUser>::OutputSize;
#[doc(hidden)]
pub type HasherOutput = generic_array::GenericArray<u8, HasherOutputSize>;

#[derive(Clone, Debug)]
pub(crate) enum Update<T> {
    Removed,
    Set(T),
}

#[derive(Clone, Debug)]
pub(crate) struct DeletionSet {
    pub(crate) delete_storage_first: bool,
    pub(crate) deleted_prefixes: BTreeSet<Vec<u8>>,
}

impl DeletionSet {
    pub(crate) fn new() -> Self {
        Self {
            delete_storage_first: false,
            deleted_prefixes: BTreeSet::new(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.delete_storage_first = true;
        self.deleted_prefixes.clear();
    }

    pub(crate) fn rollback(&mut self) {
        self.delete_storage_first = false;
        self.deleted_prefixes.clear();
    }

    pub(crate) fn contains_prefix_of(&self, index: &[u8]) -> bool {
        self.delete_storage_first || contains_prefix_of(&self.deleted_prefixes, index)
    }

    pub(crate) fn has_pending_changes(&self) -> bool {
        self.delete_storage_first || !self.deleted_prefixes.is_empty()
    }

    pub(crate) fn insert_key_prefix(&mut self, key_prefix: Vec<u8>) {
        if !self.delete_storage_first {
            insert_key_prefix(&mut self.deleted_prefixes, key_prefix);
        }
    }
}

/// When wanting to find the entries in a BTreeMap with a specific prefix,
/// one option is to iterate over all keys. Another is to select an interval
/// that represents exactly the keys having that prefix. Which fortunately
/// is possible with the way the comparison operators for vectors are built.
///
/// The statement is that p is a prefix of v if and only if p <= v < upper_bound(p).
pub(crate) fn get_upper_bound_option(key_prefix: &[u8]) -> Option<Vec<u8>> {
    let len = key_prefix.len();
    for i in (0..len).rev() {
        let val = key_prefix[i];
        if val < u8::MAX {
            let mut upper_bound = key_prefix[0..i + 1].to_vec();
            upper_bound[i] += 1;
            return Some(upper_bound);
        }
    }
    None
}

/// The upper bound that can be used in ranges when accessing
/// a container. That is a vector v is a prefix of p if and only if
/// v belongs to the interval (Included(p), get_upper_bound(p)).
pub(crate) fn get_upper_bound(key_prefix: &[u8]) -> Bound<Vec<u8>> {
    match get_upper_bound_option(key_prefix) {
        None => Unbounded,
        Some(upper_bound) => Excluded(upper_bound),
    }
}

/// Computes an interval so that a vector has `key_prefix` as a prefix
/// if and only if it belongs to the range.
pub(crate) fn get_interval(key_prefix: Vec<u8>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let upper_bound = get_upper_bound(&key_prefix);
    (Included(key_prefix), upper_bound)
}

/// Deserializes an Optional vector of u8
pub(crate) fn from_bytes_option<V: DeserializeOwned, E>(
    key_opt: &Option<Vec<u8>>,
) -> Result<Option<V>, E>
where
    E: From<bcs::Error>,
{
    match key_opt {
        Some(bytes) => {
            let value = bcs::from_bytes(bytes)?;
            Ok(Some(value))
        }
        None => Ok(None),
    }
}

pub(crate) fn from_bytes_option_or_default<V: DeserializeOwned + Default, E>(
    key_opt: &Option<Vec<u8>>,
) -> Result<V, E>
where
    E: From<bcs::Error>,
{
    match key_opt {
        Some(bytes) => Ok(bcs::from_bytes(bytes)?),
        None => Ok(V::default()),
    }
}

/// `SuffixClosedSetIterator` iterates over the entries of a container ordered
/// lexicographically.
///
/// The function call `find_lower_bound(val)` returns a `Some(x)` where `x` is the highest
/// entry such that `x <= val` for the lexicographic order. If none exists then None is
/// returned. The function calls have to be done with increasing `val`.
///
/// The function call `find_key(val)` tests whether there exists a prefix p in the
/// set of vectors such that p is a prefix of val.
pub(crate) struct SuffixClosedSetIterator<'a, I> {
    prefix_len: usize,
    previous: Option<&'a Vec<u8>>,
    current: Option<&'a Vec<u8>>,
    iter: I,
}

impl<'a, I> SuffixClosedSetIterator<'a, I>
where
    I: Iterator<Item = &'a Vec<u8>>,
{
    pub(crate) fn new(prefix_len: usize, mut iter: I) -> Self {
        let previous = None;
        let current = iter.next();
        Self {
            prefix_len,
            previous,
            current,
            iter,
        }
    }

    pub(crate) fn find_lower_bound(&mut self, val: &[u8]) -> Option<&'a Vec<u8>> {
        loop {
            match &self.current {
                None => {
                    return self.previous;
                }
                Some(x) => {
                    if &x[self.prefix_len..] > val {
                        return self.previous;
                    }
                }
            }
            let current = self.iter.next();
            self.previous = std::mem::replace(&mut self.current, current);
        }
    }

    pub(crate) fn find_key(&mut self, index: &[u8]) -> bool {
        let lower_bound = self.find_lower_bound(index);
        match lower_bound {
            None => false,
            Some(key_prefix) => index.starts_with(&key_prefix[self.prefix_len..]),
        }
    }
}

pub(crate) fn contains_prefix_of(prefixes: &BTreeSet<Vec<u8>>, key: &[u8]) -> bool {
    let iter = prefixes.iter();
    let mut suffix_closed_set = SuffixClosedSetIterator::new(0, iter);
    suffix_closed_set.find_key(key)
}

pub(crate) fn insert_key_prefix(prefixes: &mut BTreeSet<Vec<u8>>, prefix: Vec<u8>) {
    if !contains_prefix_of(prefixes, &prefix) {
        let key_prefix_list = prefixes
            .range(get_interval(prefix.clone()))
            .map(|x| x.to_vec())
            .collect::<Vec<_>>();
        for key in key_prefix_list {
            prefixes.remove(&key);
        }
        prefixes.insert(prefix);
    }
}

#[test]
fn suffix_closed_set_test1_the_lower_bound() {
    let mut set = BTreeSet::<Vec<u8>>::new();
    set.insert(vec![4]);
    set.insert(vec![7]);
    set.insert(vec![8]);
    set.insert(vec![10]);
    set.insert(vec![24]);
    set.insert(vec![40]);

    let mut suffix_closed_set = SuffixClosedSetIterator::new(0, set.iter());
    assert_eq!(suffix_closed_set.find_lower_bound(&[3]), None);
    assert_eq!(
        suffix_closed_set.find_lower_bound(&[15]),
        Some(vec![10]).as_ref()
    );
    assert_eq!(
        suffix_closed_set.find_lower_bound(&[17]),
        Some(vec![10]).as_ref()
    );
    assert_eq!(
        suffix_closed_set.find_lower_bound(&[25]),
        Some(vec![24]).as_ref()
    );
    assert_eq!(
        suffix_closed_set.find_lower_bound(&[27]),
        Some(vec![24]).as_ref()
    );
    assert_eq!(
        suffix_closed_set.find_lower_bound(&[42]),
        Some(vec![40]).as_ref()
    );
}

#[test]
fn suffix_closed_set_test2_find_key() {
    let mut set = BTreeSet::<Vec<u8>>::new();
    set.insert(vec![4]);
    set.insert(vec![0, 3]);
    set.insert(vec![5]);

    let mut suffix_closed_set = SuffixClosedSetIterator::new(0, set.iter());
    assert!(!suffix_closed_set.find_key(&[0]));
    assert!(suffix_closed_set.find_key(&[0, 3]));
    assert!(suffix_closed_set.find_key(&[0, 3, 4]));
    assert!(!suffix_closed_set.find_key(&[1]));
    assert!(suffix_closed_set.find_key(&[4]));
}

#[test]
fn suffix_closed_set_test3_find_key_prefix_len() {
    let mut set = BTreeSet::<Vec<u8>>::new();
    set.insert(vec![0, 4]);
    set.insert(vec![0, 3]);
    set.insert(vec![0, 0, 1]);

    let mut suffix_closed_set = SuffixClosedSetIterator::new(1, set.iter());
    assert!(!suffix_closed_set.find_key(&[0]));
    assert!(suffix_closed_set.find_key(&[0, 1]));
    assert!(suffix_closed_set.find_key(&[0, 1, 4]));
    assert!(suffix_closed_set.find_key(&[3]));
    assert!(!suffix_closed_set.find_key(&[5]));
}

#[test]
fn insert_key_prefix_test1() {
    let mut set = BTreeSet::<Vec<u8>>::new();
    set.insert(vec![0, 4]);

    insert_key_prefix(&mut set, vec![0, 4, 5]);
    let keys = set.iter().cloned().collect::<Vec<_>>();
    assert_eq!(keys, vec![vec![0, 4]]);
}

/// Sometimes we need a serialization that is different from the usual one and
/// for example preserves order.
/// The {to/from}_custom_bytes has to be coherent with the Borrow trait.
pub trait CustomSerialize: Sized {
    /// Serializes the value
    fn to_custom_bytes(&self) -> Result<Vec<u8>, ViewError>;

    /// Deserialize the vector
    fn from_custom_bytes(short_key: &[u8]) -> Result<Self, ViewError>;
}

impl CustomSerialize for u128 {
    fn to_custom_bytes(&self) -> Result<Vec<u8>, ViewError> {
        let mut bytes = bcs::to_bytes(&self)?;
        bytes.reverse();
        Ok(bytes)
    }

    fn from_custom_bytes(bytes: &[u8]) -> Result<Self, ViewError> {
        let mut bytes = bytes.to_vec();
        bytes.reverse();
        let value = bcs::from_bytes(&bytes)?;
        Ok(value)
    }
}

/// This computes the offset of the BCS serialization of a vector.
/// The formula that should be satisfied is
/// serialized_size(vec![v_1, ...., v_n]) = get_uleb128_size(n)
///  + serialized_size(v_1)? + .... serialized_size(v_n)?
pub(crate) const fn get_uleb128_size(len: usize) -> usize {
    let mut power = 128;
    let mut expo = 1;
    while len >= power {
        power *= 128;
        expo += 1;
    }
    expo
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use linera_views::common::CustomSerialize;
    use rand::Rng;

    #[test]
    fn test_ordering_serialization() {
        let mut rng = crate::random::make_deterministic_rng();
        let n = 1000;
        let mut set = BTreeSet::new();
        for _ in 0..n {
            let val = rng.gen::<u128>();
            set.insert(val);
        }
        let mut vec = Vec::new();
        for val in set {
            vec.push(val);
        }
        for i in 1..vec.len() {
            let val1 = vec[i - 1];
            let val2 = vec[i];
            assert!(val1 < val2);
            let vec1 = val1.to_custom_bytes().unwrap();
            let vec2 = val2.to_custom_bytes().unwrap();
            assert!(vec1 < vec2);
            let val_ret1 = u128::from_custom_bytes(&vec1).unwrap();
            let val_ret2 = u128::from_custom_bytes(&vec2).unwrap();
            assert_eq!(val1, val_ret1);
            assert_eq!(val2, val_ret2);
        }
    }
}

#[test]
fn test_upper_bound() {
    assert_eq!(get_upper_bound(&[255]), Unbounded);
    assert_eq!(get_upper_bound(&[255, 255, 255, 255]), Unbounded);
    assert_eq!(get_upper_bound(&[0, 2]), Excluded(vec![0, 3]));
    assert_eq!(get_upper_bound(&[0, 255]), Excluded(vec![1]));
    assert_eq!(get_upper_bound(&[255, 0]), Excluded(vec![255, 1]));
}
