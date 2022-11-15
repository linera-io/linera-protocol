use crate::{
    grpc_network::grpc_network::{bcs_service_server::BcsService, BcsMessage},
    transport::MessageHandler,
    Message,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

pub mod grpc_network {
    tonic::include_proto!("rpc.v1");
}

#[derive(Debug, Default)]
pub struct GenericBcsService<S> {
    state: Arc<Mutex<S>>,
}

impl<S> From<S> for GenericBcsService<S> {
    fn from(s: S) -> Self {
        GenericBcsService {
            state: Arc::new(Mutex::new(s)),
        }
    }
}

#[tonic::async_trait]
impl<S> BcsService for GenericBcsService<S>
where
    S: MessageHandler + Send + Sync + 'static,
{
    async fn handle(&self, request: Request<BcsMessage>) -> Result<Response<BcsMessage>, Status> {
        let message: Message = bcs::from_bytes(&request.get_ref().inner).unwrap();

        let mut state = self
            .state
            .try_lock()
            .map_err(|_| Status::internal("service lock poisoned"))?;

        let response: Option<Message> = state.handle_message(message).await;

        let response_bytes = match response {
            Some(response) => bcs::to_bytes(&response),
            None => bcs::to_bytes::<Vec<()>>(&vec![]), // todo(security): do we want the error msg showing the serialization internals?
        }
        .map_err(|e| {
            Status::data_loss(format!(
                "there was an error while serializing the response: {:?}",
                e
            ))
        })?;

        Ok(Response::new(BcsMessage {
            inner: response_bytes,
        }))
    }
}
