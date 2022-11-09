// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmer](https://wasmer.io/) runtime.

// Export the system interface used by a user application.
wit_bindgen_host_wasmer_rust::export!("../linera-sdk/system.wit");

// Import the interface implemented by a user application.
wit_bindgen_host_wasmer_rust::import!("../linera-sdk/application.wit");

use self::system::PollLoad;
use super::async_boundary::{ContextForwarder, HostFuture};
use crate::{ExecutionError, WritableStorage};
use std::{marker::PhantomData, mem, sync::Arc, task::Poll};
use tokio::sync::Mutex;

/// Implementation to forward system calls from the guest WASM module to the host implementation.
pub struct SystemApi {
    context: ContextForwarder,
    storage: Arc<Mutex<Option<&'static dyn WritableStorage>>>,
}

impl SystemApi {
    /// Create a new [`SystemApi`] instance, ensuring that the lifetime of the [`WritableStorage`]
    /// trait object is respected.
    ///
    /// # Safety
    ///
    /// This method uses a [`mem::transmute`] call to erase the lifetime of the `storage` trait
    /// object reference. However, this is safe because the lifetime is transfered to the returned
    /// [`StorageGuard`], which removes the unsafe reference from memory when it is dropped,
    /// ensuring the lifetime is respected.
    ///
    /// The [`StorageGuard`] instance must be kept alive while the trait object is still expected to
    /// be alive and usable by the WASM application.
    pub fn new(context: ContextForwarder, storage: &dyn WritableStorage) -> (Self, StorageGuard) {
        let storage_without_lifetime = unsafe { mem::transmute(storage) };
        let storage = Arc::new(Mutex::new(Some(storage_without_lifetime)));

        let guard = StorageGuard {
            storage: storage.clone(),
            _lifetime: PhantomData,
        };

        (SystemApi { context, storage }, guard)
    }

    /// Safely obtain the [`WritableStorage`] trait object instance to handle a system call.
    ///
    /// # Panics
    ///
    /// If there is a concurrent call from the WASM application (which is impossible as long as it
    /// is executed in a single thread) or if the trait object is no longer alive (or more
    /// accurately, if the [`StorageGuard`] returned by [`Self::new`] was dropped to indicate it's
    /// no longer alive).
    fn storage(&self) -> &'static dyn WritableStorage {
        *self
            .storage
            .try_lock()
            .expect("Unexpected concurrent storage access by application")
            .as_ref()
            .expect("Application called storage after it should have stopped")
    }
}

impl system::System for SystemApi {
    type Load = HostFuture<'static, Result<Vec<u8>, ExecutionError>>;
    type LoadAndLock = HostFuture<'static, Result<Vec<u8>, ExecutionError>>;

    fn load_new(&mut self) -> Self::Load {
        HostFuture::new(self.storage().try_read_my_state())
    }

    fn load_poll(&mut self, future: &Self::Load) -> PollLoad {
        match future.poll(&mut self.context) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
        }
    }

    fn load_and_lock_new(&mut self) -> Self::LoadAndLock {
        HostFuture::new(self.storage().try_read_and_lock_my_state())
    }

    fn load_and_lock_poll(&mut self, future: &Self::LoadAndLock) -> PollLoad {
        match future.poll(&mut self.context) {
            Poll::Pending => PollLoad::Pending,
            Poll::Ready(Ok(bytes)) => PollLoad::Ready(Ok(bytes)),
            Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
        }
    }

    fn store_and_unlock(&mut self, state: &[u8]) -> bool {
        self.storage()
            .save_and_unlock_my_state(state.to_owned())
            .is_ok()
    }
}

/// A guard to unsure that the [`WritableStorage`] trait object isn't called after it's no longer
/// borrowed.
pub struct StorageGuard<'storage> {
    storage: Arc<Mutex<Option<&'static dyn WritableStorage>>>,
    _lifetime: PhantomData<&'storage ()>,
}

impl Drop for StorageGuard<'_> {
    fn drop(&mut self) {
        self.storage
            .try_lock()
            .expect("Guard dropped while storage is still in use")
            .take();
    }
}
