use super::common::{self, WritableRuntimeContext};
use crate::ExecutionError;
use futures::future::BoxFuture;
use std::{
    any::type_name,
    fmt::{self, Debug, Formatter},
    future::Future,
    marker::PhantomData,
    mem,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::Mutex;

pub struct HostFuture<'future, Output> {
    future: Mutex<BoxFuture<'future, Output>>,
}

impl<Output> Debug for HostFuture<'_, Output> {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        formatter
            .debug_struct(&format!("HostFuture<'_, {}>", type_name::<Output>()))
            .finish_non_exhaustive()
    }
}

impl<'future, Output> HostFuture<'future, Output> {
    pub fn new(future: impl Future<Output = Output> + Send + 'future) -> Self {
        HostFuture {
            future: Mutex::new(Box::pin(future)),
        }
    }

    pub fn poll(&self, context: &mut ContextForwarder) -> Poll<Output> {
        let mut context_reference = context
            .0
            .try_lock()
            .expect("Unexpected concurrent application call");

        let context = context_reference
            .as_mut()
            .expect("Application called without an async task context");

        let mut future = self
            .future
            .try_lock()
            .expect("Application can't call the future concurrently because it's single threaded");

        future.as_mut().poll(context)
    }
}

pub enum GuestFuture<Future, Runtime>
where
    Runtime: common::Runtime,
{
    FailedToCreate(Option<Runtime::Error>),
    Active {
        future: Future,
        context: WritableRuntimeContext<Runtime>,
    },
}

impl<Future, Runtime> GuestFuture<Future, Runtime>
where
    Runtime: common::Runtime,
{
    pub fn new(
        creation_result: Result<Future, Runtime::Error>,
        context: WritableRuntimeContext<Runtime>,
    ) -> Self {
        match creation_result {
            Ok(future) => GuestFuture::Active { future, context },
            Err(error) => GuestFuture::FailedToCreate(Some(error)),
        }
    }
}

impl<InnerFuture, Runtime> Future for GuestFuture<InnerFuture, Runtime>
where
    InnerFuture: GuestFutureInterface<Runtime> + Unpin,
    Runtime: common::Runtime,
    Runtime::Application: Unpin,
    Runtime::Store: Unpin,
    Runtime::StorageGuard: Unpin,
    Runtime::Error: Unpin,
{
    type Output = Result<InnerFuture::Output, ExecutionError>;

    fn poll(self: Pin<&mut Self>, task_context: &mut Context) -> Poll<Self::Output> {
        match self.get_mut() {
            GuestFuture::FailedToCreate(runtime_error) => {
                let error = runtime_error.take().expect("Unexpected poll after error");
                Poll::Ready(Err(error.into()))
            }
            GuestFuture::Active { future, context } => {
                let _context_guard = context.context_forwarder.forward(task_context);
                future.poll(&context.application, &mut context.store)
            }
        }
    }
}

pub trait GuestFutureInterface<Runtime>
where
    Runtime: common::Runtime,
{
    type Output;

    fn poll(
        &self,
        application: &Runtime::Application,
        store: &mut Runtime::Store,
    ) -> Poll<Result<Self::Output, ExecutionError>>;
}

#[derive(Clone, Default)]
pub struct ContextForwarder(Arc<Mutex<Option<&'static mut Context<'static>>>>);

impl ContextForwarder {
    pub fn forward<'context>(
        &mut self,
        context: &'context mut Context,
    ) -> ActiveContextGuard<'context> {
        let mut context_reference = self
            .0
            .try_lock()
            .expect("Unexpected concurrent task context access");

        assert!(
            context_reference.is_none(),
            "`ContextForwarder` accessed by concurrent tasks"
        );

        *context_reference = Some(unsafe { mem::transmute(context) });

        ActiveContextGuard {
            context: self.0.clone(),
            lifetime: PhantomData,
        }
    }
}

pub struct ActiveContextGuard<'context> {
    context: Arc<Mutex<Option<&'static mut Context<'static>>>>,
    lifetime: PhantomData<&'context mut ()>,
}

impl Drop for ActiveContextGuard<'_> {
    fn drop(&mut self) {
        let mut context_reference = self
            .context
            .try_lock()
            .expect("Unexpected concurrent task context access");

        *context_reference = None;
    }
}
