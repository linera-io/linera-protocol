use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{
    cell::OnceCell,
    sync::{Arc, Mutex, OnceLock},
};

static RNG: OnceLock<Mutex<StdRng>> = OnceLock::new();

fn custom_getrandom(buf: &mut [u8]) -> Result<(), getrandom::Error> {
    RNG.get_or_init(|| Mutex::new(StdRng::from_seed([0; 32])))
        .lock()
        .expect("failed to get RNG lock")
        .fill(buf);
    Ok(())
}

getrandom::register_custom_getrandom!(custom_getrandom);
