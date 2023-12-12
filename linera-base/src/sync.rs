// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Provide `Lazy` struct on top of `std`. Based on [`once_cell::sync::Lazy`](https://github.com/matklad/once_cell).
//!
//! This is temporary until
//! [std::sync::LazyLock](https://doc.rust-lang.org/std/sync/struct.LazyLock.html) is
//! stabilized [#109736](https://github.com/rust-lang/rust/issues/109736).

use std::{cell::Cell, fmt, ops::Deref, panic::RefUnwindSafe, sync::OnceLock};

/// A value which is initialized on the first access.
///
/// This type is thread-safe and can be used in statics.
///
/// # Example
///
/// ```
/// use std::collections::HashMap;
///
/// use linera_base::sync::Lazy;
///
/// static HASHMAP: Lazy<HashMap<i32, String>> = Lazy::new(|| {
///     println!("initializing");
///     let mut m = HashMap::new();
///     m.insert(13, "Spica".to_string());
///     m.insert(74, "Hoyten".to_string());
///     m
/// });
///
/// fn main() {
///     println!("ready");
///     std::thread::spawn(|| {
///         println!("{:?}", HASHMAP.get(&13));
///     }).join().unwrap();
///     println!("{:?}", HASHMAP.get(&74));
///
///     // Prints:
///     //   ready
///     //   initializing
///     //   Some("Spica")
///     //   Some("Hoyten")
/// }
/// ```
pub struct Lazy<T, F = fn() -> T> {
    cell: OnceLock<T>,
    init: Cell<Option<F>>,
}

impl<T: fmt::Debug, F> fmt::Debug for Lazy<T, F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Lazy")
            .field("cell", &self.cell)
            .field("init", &"..")
            .finish()
    }
}

// We never create a `&F` from a `&Lazy<T, F>` so it is fine to not impl
// `Sync` for `F`. We do create a `&mut Option<F>` in `force`, but this is
// properly synchronized, so it only happens once so it also does not
// contribute to this impl.
unsafe impl<T, F: Send> Sync for Lazy<T, F> where OnceLock<T>: Sync {}
// auto-derived `Send` impl is OK.

impl<T, F: RefUnwindSafe> RefUnwindSafe for Lazy<T, F> where OnceLock<T>: RefUnwindSafe {}

impl<T, F> Lazy<T, F> {
    /// Creates a new lazy value with the given initializing
    /// function.
    pub const fn new(f: F) -> Lazy<T, F> {
        Lazy {
            cell: OnceLock::new(),
            init: Cell::new(Some(f)),
        }
    }

    /// Consumes this `Lazy` returning the stored value.
    ///
    /// Returns `Ok(value)` if `Lazy` is initialized and `Err(f)` otherwise.
    pub fn into_value(this: Lazy<T, F>) -> Result<T, F> {
        let cell = this.cell;
        let init = this.init;
        cell.into_inner().ok_or_else(|| {
            init.take()
                .unwrap_or_else(|| panic!("Lazy instance has previously been poisoned"))
        })
    }
}

impl<T, F: FnOnce() -> T> Lazy<T, F> {
    /// Forces the evaluation of this lazy value and
    /// returns a reference to the result. This is equivalent
    /// to the `Deref` impl, but is explicit.
    ///
    /// # Example
    /// ```
    /// use linera_base::sync::Lazy;
    ///
    /// let lazy = Lazy::new(|| 92);
    ///
    /// assert_eq!(Lazy::force(&lazy), &92);
    /// assert_eq!(&*lazy, &92);
    /// ```
    pub fn force(this: &Lazy<T, F>) -> &T {
        this.cell.get_or_init(|| match this.init.take() {
            Some(f) => f(),
            None => panic!("Lazy instance has previously been poisoned"),
        })
    }

    /// Gets the reference to the result of this lazy value if
    /// it was initialized, otherwise returns `None`.
    ///
    /// # Example
    /// ```
    /// use linera_base::sync::Lazy;
    ///
    /// let lazy = Lazy::new(|| 92);
    ///
    /// assert_eq!(Lazy::get(&lazy), None);
    /// assert_eq!(&*lazy, &92);
    /// assert_eq!(Lazy::get(&lazy), Some(&92));
    /// ```
    pub fn get(this: &Lazy<T, F>) -> Option<&T> {
        this.cell.get()
    }

    /// Gets the reference to the result of this lazy value if
    /// it was initialized, otherwise returns `None`.
    ///
    /// # Example
    /// ```
    /// use linera_base::sync::Lazy;
    ///
    /// let mut lazy = Lazy::new(|| 92);
    ///
    /// assert_eq!(Lazy::get_mut(&mut lazy), None);
    /// assert_eq!(&*lazy, &92);
    /// assert_eq!(Lazy::get_mut(&mut lazy), Some(&mut 92));
    /// ```
    pub fn get_mut(this: &mut Lazy<T, F>) -> Option<&mut T> {
        this.cell.get_mut()
    }
}

impl<T, F: FnOnce() -> T> Deref for Lazy<T, F> {
    type Target = T;
    fn deref(&self) -> &T {
        Lazy::force(self)
    }
}

impl<T: Default> Default for Lazy<T> {
    /// Creates a new lazy value using `Default` as the initializing function.
    fn default() -> Lazy<T> {
        Lazy::new(T::default)
    }
}
