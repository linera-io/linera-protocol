use std::time::Duration;
use ethers::{
    providers::{Http, Provider},
    signers::LocalWallet,
    types::Address,
};
use ethers_core::utils::{Anvil, AnvilInstance};
use ethers_signers::Signer;

pub async fn get_provider(url: &str) -> Provider<Http> {
    Provider::try_from(url).unwrap()
}

pub struct AnvilTest {
    pub anvil_instance: AnvilInstance,
    pub provider: Provider<Http>,
    pub endpoint: String,
}

pub fn get_anvil() -> AnvilTest {
    let port = 8595u16;
    let anvil_instance = Anvil::new()
        .port(port)
        .mnemonic("abstract vacuum mammal awkward pudding scene penalty purchase dinner depart evoke puzzle")
        .spawn();
    let endpoint = anvil_instance.endpoint();
    let provider = Provider::<Http>::try_from(endpoint.clone())
        .unwrap()
        .interval(Duration::from_millis(10u64));
    AnvilTest {
        anvil_instance,
        provider,
        endpoint,
    }
}

impl AnvilTest {
    pub fn get_wallet(&self, index: usize) -> (LocalWallet, Address) {
        let address = self.anvil_instance.addresses()[index].clone();
        let wallet: LocalWallet = self.anvil_instance.keys()[index].clone().into();
        let wallet = wallet.with_chain_id(self.anvil_instance.chain_id());
        (wallet, address)
    }
}

