use crate::{contract, service};
use log::{LevelFilter, Log, Metadata, Record};
use std::panic::PanicInfo;

static CONTRACT_LOGGER: ContractLogger = ContractLogger;
static SERVICE_LOGGER: ServiceLogger = ServiceLogger;

/// A logger that uses the system API for contracts.
#[derive(Clone, Copy, Debug)]
pub struct ContractLogger;

impl ContractLogger {
    /// Configures [`log`] to use the log system API for contracts.
    pub fn install() {
        log::set_logger(&CONTRACT_LOGGER).expect("Failed to initialize contract logger");
        log::set_max_level(LevelFilter::Trace);
    }
}

impl Log for ContractLogger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        contract::system_api::log(record.args(), record.level());
    }

    fn flush(&self) {}
}

/// A logger that uses the system API for services.
#[derive(Clone, Copy, Debug)]
pub struct ServiceLogger;

impl ServiceLogger {
    /// Configures [`log`] to use the log system API for services.
    pub fn install() {
        log::set_logger(&SERVICE_LOGGER).expect("Failed to initialize service logger");
        log::set_max_level(LevelFilter::Trace);
    }
}

impl Log for ServiceLogger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        service::system_api::log(record.args(), record.level());
    }

    fn flush(&self) {}
}

/// Logs a panic using the [`log`] API.
fn log_panic(info: &PanicInfo<'_>) {
    log::error!("{info}");
}
