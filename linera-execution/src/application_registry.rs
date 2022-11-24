use crate::ExecutionError;
use linera_base::messages::{ApplicationDescription, ApplicationId, BytecodeId, BytecodeLocation};
use linera_views::{impl_view, map_view::MapView, scoped_view::ScopedView, views::ViewError};

#[derive(Debug)]
pub struct ApplicationRegistryView<C> {
    /// The application bytecodes that have been published.
    pub published_bytecodes: ScopedView<0, MapView<C, BytecodeId, BytecodeLocation>>,
    /// The applications that are known by the chain.
    pub known_applications: ScopedView<1, MapView<C, ApplicationId, ApplicationDescription>>,
}

impl_view!(
    ApplicationRegistryView {
        published_bytecodes,
        known_applications,
    };
);

impl<C> ApplicationRegistryView<C>
where
    C: ApplicationRegistryViewContext,
    ViewError: From<C::Error>,
{
    /// Register a published bytecode so that it can be used by applications.
    ///
    /// Keeps track of the bytecode's location so that it can be loaded when needed.
    pub fn register_published_bytecode(&mut self, id: BytecodeId, location: BytecodeLocation) {
        self.published_bytecodes.insert(id, location);
    }

    /// Register an existing application.
    ///
    /// Keeps track of an existing application that the current chain is seeing for the first time.
    pub fn register_existing_application(
        &mut self,
        application: ApplicationDescription,
    ) -> ApplicationId {
        let id = ApplicationId::from(&application);
        self.known_applications.insert(id, application);
        id
    }

    /// Retrieve an application's description.
    pub async fn describe_application(
        &mut self,
        id: ApplicationId,
    ) -> Result<ApplicationDescription, ExecutionError> {
        match id {
            ApplicationId::System => Ok(ApplicationDescription::System),
            ApplicationId::User { .. } => self
                .known_applications
                .get(&id)
                .await?
                .ok_or_else(|| ExecutionError::UnknownApplication(Box::new(id))),
        }
    }
}
