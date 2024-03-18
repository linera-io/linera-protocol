use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::{Mutex, OnceLock};

static RNG: OnceLock<Mutex<StdRng>> = OnceLock::new();

fn custom_getrandom(buf: &mut [u8]) -> Result<(), getrandom::Error> {
    let mut seed = linera_sdk::service::system_api::current_system_time()
        .micros()
        .to_be_bytes()
        .to_vec();
    // Timestamp is 8 bytes and RNG seed requires 32 bytes.
    seed.extend(std::iter::repeat(0).take(32 - 8));
    RNG.get_or_init(|| Mutex::new(StdRng::from_seed(seed.try_into().expect("wrong length"))))
        .lock()
        .expect("failed to get RNG lock")
        .fill(buf);
    Ok(())
}

getrandom::register_custom_getrandom!(custom_getrandom);
