use std::collections::hash_map::Entry;
use std::collections::HashMap;
use tonic::transport::Channel;
use crate::grpc_network::grpc_network::validator_worker_client::ValidatorWorkerClient;
use crate::grpc_network::GrpcError;

// todo make generic over a trait Connect?
pub struct ClientPool(HashMap<String, ValidatorWorkerClient<Channel>>);

impl ClientPool {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub async fn client_for_address(&mut self, remote_address: String) -> Result<&mut ValidatorWorkerClient<Channel>, GrpcError> {
        let client = if self.0.contains_key(&remote_address) {
            self.0.get_mut(&remote_address).unwrap()
        } else {
            let client = ValidatorWorkerClient::connect(remote_address.clone()).await?;
            self.0.insert(remote_address.clone(), client);
            self.0.get_mut(&remote_address).unwrap()
        };
        Ok(client)
    }
}