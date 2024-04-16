// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    panic::{self, PanicInfo},
    sync::Once,
};

use log::{LevelFilter, Log, Metadata, Record};

use crate::{contract::wit::contract_system_api, service::wit::service_system_api};

static CONTRACT_LOGGER: ContractLogger = ContractLogger;
static SERVICE_LOGGER: ServiceLogger = ServiceLogger;

static INSTALL_LOGGER: Once = Once::new();

/// A logger that uses the system API for contracts.
#[derive(Clone, Copy, Debug)]
pub struct ContractLogger;

impl ContractLogger {
    /// Configures [`log`] to use the log system API for contracts.
    pub fn install() {
        INSTALL_LOGGER.call_once(|| {
            log::set_logger(&CONTRACT_LOGGER).expect("Failed to initialize contract logger");
            log::set_max_level(LevelFilter::Trace);
            panic::set_hook(Box::new(log_panic));
        });
    }
}

impl Log for ContractLogger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        contract_system_api::log(&record.args().to_string(), record.level().into());
    }

    fn flush(&self) {}
}

/// A logger that uses the system API for services.
#[derive(Clone, Copy, Debug)]
pub struct ServiceLogger;

impl ServiceLogger {
    /// Configures [`log`] to use the log system API for services.
    pub fn install() {
        INSTALL_LOGGER.call_once(|| {
            log::set_logger(&SERVICE_LOGGER).expect("Failed to initialize service logger");
            log::set_max_level(LevelFilter::Trace);
            panic::set_hook(Box::new(log_panic));
        });
    }
}

impl Log for ServiceLogger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        service_system_api::log(&record.args().to_string(), record.level().into());
    }

    fn flush(&self) {}
}

/// Logs a panic using the [`log`] API.
fn log_panic(info: &PanicInfo<'_>) {
    log::error!("{info}");
}
