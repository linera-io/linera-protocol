use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::{Mutex, OnceLock};

static RNG: OnceLock<Mutex<StdRng>> = OnceLock::new();

fn custom_getrandom(buf: &mut [u8]) -> Result<(), getrandom::Error> {
    let mut seed = [0u8; 32];
    RNG.get_or_init(|| Mutex::new(StdRng::from_seed(seed.try_into().expect("wrong length"))))
        .lock()
        .expect("failed to get RNG lock")
        .fill(buf);
    Ok(())
}

getrandom::register_custom_getrandom!(custom_getrandom);
