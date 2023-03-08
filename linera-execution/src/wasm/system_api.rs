/// Generates an implementation of `WritableSystem` for the provided `contract_system_api` type.
///
/// Generates the common code for contract system API types for all WASM runtimes.
macro_rules! impl_writable_system {
    ($contract_system_api:ident<$storage:lifetime>) => {
        impl_writable_system!(@generate $contract_system_api<$storage>, $storage, <$storage>);
    };

    ($contract_system_api:ident) => {
        impl_writable_system!(@generate $contract_system_api, 'static);
    };

    (@generate $contract_system_api:ty, $storage:lifetime $(, <$param:lifetime> )?) => {
        impl$(<$param>)? WritableSystem for $contract_system_api {
            type Load = HostFuture<$storage, Result<Vec<u8>, ExecutionError>>;
            type LoadAndLock = HostFuture<$storage, Result<Vec<u8>, ExecutionError>>;
            type Lock = HostFuture<$storage, Result<(), ExecutionError>>;
            type ReadKeyBytes = HostFuture<$storage, Result<Option<Vec<u8>>, ExecutionError>>;
            type FindKeys = HostFuture<$storage, Result<Vec<Vec<u8>>, ExecutionError>>;
            type FindKeyValues =
                HostFuture<$storage, Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>>;
            type WriteBatch = HostFuture<$storage, Result<(), ExecutionError>>;
            type TryCallApplication = HostFuture<$storage, Result<CallResult, ExecutionError>>;
            type TryCallSession = HostFuture<$storage, Result<CallResult, ExecutionError>>;

            fn chain_id(&mut self) -> writable_system::ChainId {
                self.storage().chain_id().into()
            }

            fn application_id(&mut self) -> writable_system::ApplicationId {
                self.storage().application_id().into()
            }

            fn application_parameters(&mut self) -> Vec<u8> {
                self.storage().application_parameters()
            }

            fn read_system_balance(&mut self) -> writable_system::SystemBalance {
                self.storage().read_system_balance().into()
            }

            fn read_system_timestamp(&mut self) -> writable_system::Timestamp {
                self.storage().read_system_timestamp().micros()
            }

            fn load_new(&mut self) -> Self::Load {
                self.queued_future_factory
                    .enqueue(self.storage().try_read_my_state())
            }

            fn load_poll(&mut self, future: &Self::Load) -> writable_system::PollLoad {
                use writable_system::PollLoad;
                match future.poll(self.context()) {
                    Poll::Pending => PollLoad::Pending,
                    Poll::Ready(Ok(bytes)) => PollLoad::Ready(bytes),
                    Poll::Ready(Err(error)) => {
                        self.report_internal_error(error);
                        PollLoad::Pending
                    }
                }
            }

            fn load_and_lock_new(&mut self) -> Self::LoadAndLock {
                self.queued_future_factory
                    .enqueue(self.storage().try_read_and_lock_my_state())
            }

            fn load_and_lock_poll(
                &mut self,
                future: &Self::LoadAndLock,
            ) -> writable_system::PollLoadAndLock {
                use writable_system::PollLoadAndLock;
                match future.poll(self.context()) {
                    Poll::Pending => PollLoadAndLock::Pending,
                    Poll::Ready(Ok(bytes)) => PollLoadAndLock::Ready(Some(bytes)),
                    Poll::Ready(Err(ExecutionError::ViewError(ViewError::NotFound(_)))) => {
                        PollLoadAndLock::Ready(None)
                    }
                    Poll::Ready(Err(error)) => {
                        self.report_internal_error(error);
                        PollLoadAndLock::Pending
                    }
                }
            }

            fn store_and_unlock(&mut self, state: &[u8]) -> bool {
                self.storage()
                    .save_and_unlock_my_state(state.to_owned())
                    .is_ok()
            }

            fn lock_new(&mut self) -> Self::Lock {
                self.queued_future_factory
                    .enqueue(self.storage().lock_view_user_state())
            }

            fn lock_poll(&mut self, future: &Self::Lock) -> writable_system::PollLock {
                use writable_system::PollLock;
                match future.poll(self.context()) {
                    Poll::Pending => PollLock::Pending,
                    Poll::Ready(Ok(())) => PollLock::ReadyLocked,
                    Poll::Ready(Err(ExecutionError::ViewError(ViewError::TryLockError(_)))) => {
                        PollLock::ReadyNotLocked
                    }
                    Poll::Ready(Err(error)) => {
                        self.report_internal_error(error);
                        PollLock::Pending
                    }
                }
            }

            fn read_key_bytes_new(&mut self, key: &[u8]) -> Self::ReadKeyBytes {
                self.queued_future_factory
                    .enqueue(self.storage().read_key_bytes(key.to_owned()))
            }

            fn read_key_bytes_poll(
                &mut self,
                future: &Self::ReadKeyBytes,
            ) -> writable_system::PollReadKeyBytes {
                use writable_system::PollReadKeyBytes;
                match future.poll(self.context()) {
                    Poll::Pending => PollReadKeyBytes::Pending,
                    Poll::Ready(Ok(opt_list)) => PollReadKeyBytes::Ready(opt_list),
                    Poll::Ready(Err(error)) => {
                        self.report_internal_error(error);
                        PollReadKeyBytes::Pending
                    }
                }
            }

            fn find_keys_new(&mut self, key_prefix: &[u8]) -> Self::FindKeys {
                self.queued_future_factory
                    .enqueue(self.storage().find_keys_by_prefix(key_prefix.to_owned()))
            }

            fn find_keys_poll(&mut self, future: &Self::FindKeys) -> writable_system::PollFindKeys {
                use writable_system::PollFindKeys;
                match future.poll(self.context()) {
                    Poll::Pending => PollFindKeys::Pending,
                    Poll::Ready(Ok(keys)) => PollFindKeys::Ready(keys),
                    Poll::Ready(Err(error)) => {
                        self.report_internal_error(error);
                        PollFindKeys::Pending
                    }
                }
            }

            fn find_key_values_new(&mut self, key_prefix: &[u8]) -> Self::FindKeyValues {
                self.queued_future_factory.enqueue(
                    self.storage()
                        .find_key_values_by_prefix(key_prefix.to_owned()),
                )
            }

            fn find_key_values_poll(
                &mut self,
                future: &Self::FindKeyValues,
            ) -> writable_system::PollFindKeyValues {
                use writable_system::PollFindKeyValues;
                match future.poll(self.context()) {
                    Poll::Pending => PollFindKeyValues::Pending,
                    Poll::Ready(Ok(key_values)) => PollFindKeyValues::Ready(key_values),
                    Poll::Ready(Err(error)) => {
                        self.report_internal_error(error);
                        PollFindKeyValues::Pending
                    }
                }
            }

            fn write_batch_new(
                &mut self,
                list_oper: Vec<writable_system::WriteOperation>,
            ) -> Self::WriteBatch {
                let mut batch = Batch::new();
                for x in list_oper {
                    match x {
                        writable_system::WriteOperation::Delete(key) => {
                            batch.delete_key(key.to_vec())
                        }
                        writable_system::WriteOperation::Deleteprefix(key_prefix) => {
                            batch.delete_key_prefix(key_prefix.to_vec())
                        }
                        writable_system::WriteOperation::Put(key_value) => {
                            batch.put_key_value_bytes(key_value.0.to_vec(), key_value.1.to_vec())
                        }
                    }
                }
                self.queued_future_factory
                    .enqueue(self.storage().write_batch_and_unlock(batch))
            }

            fn write_batch_poll(&mut self, future: &Self::WriteBatch) -> writable_system::PollUnit {
                use writable_system::PollUnit;
                match future.poll(self.context()) {
                    Poll::Pending => PollUnit::Pending,
                    Poll::Ready(Ok(())) => PollUnit::Ready,
                    Poll::Ready(Err(error)) => {
                        self.report_internal_error(error);
                        PollUnit::Pending
                    }
                }
            }

            fn try_call_application_new(
                &mut self,
                authenticated: bool,
                application: writable_system::ApplicationId,
                argument: &[u8],
                forwarded_sessions: &[Le<writable_system::SessionId>],
            ) -> Self::TryCallApplication {
                let storage = self.storage();
                let forwarded_sessions = forwarded_sessions
                    .iter()
                    .map(Le::get)
                    .map(SessionId::from)
                    .collect();
                let argument = Vec::from(argument);

                self.queued_future_factory.enqueue(async move {
                    storage
                        .try_call_application(
                            authenticated,
                            application.into(),
                            &argument,
                            forwarded_sessions,
                        )
                        .await
                })
            }

            fn try_call_application_poll(
                &mut self,
                future: &Self::TryCallApplication,
            ) -> writable_system::PollCallResult {
                use writable_system::PollCallResult;
                match future.poll(self.context()) {
                    Poll::Pending => PollCallResult::Pending,
                    Poll::Ready(Ok(result)) => PollCallResult::Ready(result.into()),
                    Poll::Ready(Err(error)) => {
                        self.report_internal_error(error);
                        PollCallResult::Pending
                    }
                }
            }

            fn try_call_session_new(
                &mut self,
                authenticated: bool,
                session: writable_system::SessionId,
                argument: &[u8],
                forwarded_sessions: &[Le<writable_system::SessionId>],
            ) -> Self::TryCallApplication {
                let storage = self.storage();
                let forwarded_sessions = forwarded_sessions
                    .iter()
                    .map(Le::get)
                    .map(SessionId::from)
                    .collect();
                let argument = Vec::from(argument);

                self.queued_future_factory.enqueue(async move {
                    storage
                        .try_call_session(
                            authenticated,
                            session.into(),
                            &argument,
                            forwarded_sessions,
                        )
                        .await
                })
            }

            fn try_call_session_poll(
                &mut self,
                future: &Self::TryCallApplication,
            ) -> writable_system::PollCallResult {
                use writable_system::PollCallResult;
                match future.poll(self.context()) {
                    Poll::Pending => PollCallResult::Pending,
                    Poll::Ready(Ok(result)) => PollCallResult::Ready(result.into()),
                    Poll::Ready(Err(error)) => {
                        self.report_internal_error(error);
                        PollCallResult::Pending
                    }
                }
            }

            fn log(&mut self, message: &str, level: writable_system::LogLevel) {
                log::log!(level.into(), "{message}");
            }
        }
    };
}
