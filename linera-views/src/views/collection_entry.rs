// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The key layout of a collection entry, shared by the collection views.
//!
//! An entry is stored as an index marker recording that it exists, plus — under a separate tag —
//! the key space of its subview. The marker is what distinguishes an absent entry from one that
//! is present but has written no bytes of its own, so it cannot be inferred from the subview's
//! keys. It is read in the same batch instead, which is what makes loading an entry cost a single
//! round trip whether or not the entry exists.
//!
//! The tags are what keep those two key spaces apart. Sub-views in a collection share a common
//! key prefix, like in other view types, so appending a sub-view's own keys straight to that
//! prefix would leave a child sub-view's key indistinguishable from a grandchild's — consider a
//! collection stored inside a collection.

use crate::{
    context::Context,
    views::{View, ViewError, MIN_VIEW_TAG},
};

/// The prefixes of the keys of a collection.
#[repr(u8)]
pub(crate) enum KeyTag {
    /// Prefix for specifying an index and serves to indicate the existence of an entry in the
    /// collection.
    Index = MIN_VIEW_TAG,
    /// Prefix for specifying as the prefix for the sub-view.
    Subview,
}

/// The key marking the existence of the entry at `short_key`.
pub(crate) fn index_key<C: Context>(context: &C, short_key: &[u8]) -> Vec<u8> {
    context
        .base_key()
        .base_tag_index(KeyTag::Index as u8, short_key)
}

/// The base key of the subview stored at `short_key`.
pub(crate) fn subview_key<C: Context>(context: &C, short_key: &[u8]) -> Vec<u8> {
    context
        .base_key()
        .base_tag_index(KeyTag::Subview as u8, short_key)
}

/// The context of the subview stored at `short_key`.
pub(crate) fn subview_context<C: Context>(context: &C, short_key: &[u8]) -> C {
    context.clone_with_base_key(subview_key(context, short_key))
}

/// The number of keys [`entry_keys`] produces for one entry.
///
/// Never zero, because of the marker: that is what makes `chunks_exact` safe when a batch of
/// entries is split back up, even for a subview type with no initialization keys.
pub(crate) fn entry_len<W: View>() -> usize {
    1 + W::NUM_INIT_KEYS
}

/// The subview's context, and the keys that decide whether the entry at `short_key` exists and,
/// if it does, load it: the index marker followed by the subview's initialization keys.
pub(crate) fn entry_keys<W: View>(
    context: &W::Context,
    short_key: &[u8],
) -> Result<(W::Context, Vec<Vec<u8>>), ViewError> {
    let subview_context = subview_context(context, short_key);
    let mut keys = Vec::with_capacity(entry_len::<W>());
    keys.push(index_key(context, short_key));
    keys.extend(W::pre_load(&subview_context)?);
    Ok((subview_context, keys))
}

/// Builds the subview out of the values read for [`entry_keys`], or `None` when the index marker
/// is absent.
///
/// The marker is stored with an empty value, so its presence and not its content is what decides.
pub(crate) fn post_load_entry<W: View>(
    subview_context: W::Context,
    values: &[Option<Vec<u8>>],
) -> Result<Option<W>, ViewError> {
    // Fewer values than keys is a broken store contract rather than an absent entry.
    let (marker, init_values) = values.split_first().ok_or(ViewError::PostLoadValuesError)?;
    if marker.is_none() {
        return Ok(None);
    }
    Ok(Some(W::post_load(subview_context, init_values)?))
}
