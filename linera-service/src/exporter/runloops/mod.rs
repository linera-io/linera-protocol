// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    future::{Future, IntoFuture},
    sync::Arc,
    thread,
};

use block_processor::BlockProcessor;
use linera_client::config::{Destination, DestinationConfig, DestinationId, LimitsConfig};
use linera_rpc::{grpc::GrpcNodeProvider, NodeOptions};
use linera_storage::Storage;
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinSet,
};
use validator_exporter::Exporter;

use crate::{
    common::{BlockId, ExporterError},
    storage::{BlockProcessorStorage, ExporterStorage},
};

mod block_processor;
mod validator_exporter;

pub(crate) fn start_runloops<S, F>(
    storage: S,
    shutdown_signal: F,
    limits: LimitsConfig,
    options: NodeOptions,
    block_exporter_id: u32,
    clients_per_thread: usize,
    destination_config: DestinationConfig,
) -> Result<UnboundedSender<BlockId>, ExporterError>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    let (task_sender, queue_front) = unbounded_channel();
    let moved_task_sender = task_sender.clone();
    let _handle = thread::spawn(move || {
        start_block_processor(
            storage,
            shutdown_signal,
            limits,
            options,
            block_exporter_id,
            clients_per_thread,
            moved_task_sender,
            destination_config,
            queue_front,
        )
    });

    Ok(task_sender)
}

fn start_exporters<S, F>(
    shutdown_signal: F,
    options: NodeOptions,
    work_queue_size: usize,
    clients_per_thread: usize,
    mut storage: ExporterStorage<S>,
    destination_config: DestinationConfig,
) -> Result<(), ExporterError>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    let number_of_threads = destination_config
        .destinations
        .len()
        .div_ceil(clients_per_thread);
    let _threadpool = {
        let mut pool = Vec::new();
        for n in 0..number_of_threads {
            let moved_signal = shutdown_signal.clone();
            let moved_storage = storage.clone()?;
            let destinations = destination_config
                .destinations
                .iter()
                .enumerate()
                .filter_map(|(i, dest)| {
                    if i % number_of_threads == n {
                        Some((i as u16, dest.clone()))
                    } else {
                        None
                    }
                })
                .collect();

            let handle = thread::spawn(move || {
                start_exporter_tasks(
                    moved_signal,
                    options,
                    work_queue_size,
                    moved_storage,
                    destinations,
                )
            });

            pool.push(handle);
        }

        pool
    };

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn start_exporter_tasks<S, F>(
    shutdown_signal: F,
    options: NodeOptions,
    work_queue_size: usize,
    mut storage: ExporterStorage<S>,
    destinations: Vec<(DestinationId, Destination)>,
) where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    let node_provider = GrpcNodeProvider::new(options);
    let arced_node_provider = Arc::new(node_provider);
    let mut set = JoinSet::new();
    for (id, destination) in destinations {
        let exporter_task = Exporter::new(
            arced_node_provider.clone(),
            id,
            storage.clone().unwrap(),
            destination,
            work_queue_size,
        );
        let _handle = set.spawn(exporter_task.run_with_shutdown(shutdown_signal.clone()));
    }

    while let Some(res) = set.join_next().await {
        res.unwrap().unwrap();
    }
}

#[allow(clippy::too_many_arguments)]
#[tokio::main(flavor = "current_thread")]
async fn start_block_processor<S, F>(
    storage: S,
    shutdown_signal: F,
    limits: LimitsConfig,
    options: NodeOptions,
    block_exporter_id: u32,
    clients_per_thread: usize,
    queue_rear: UnboundedSender<BlockId>,
    destination_config: DestinationConfig,
    queue_front: UnboundedReceiver<BlockId>,
) -> Result<(), ExporterError>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    let (block_processor_storage, exporter_storage) = BlockProcessorStorage::load(
        storage.clone(),
        block_exporter_id,
        destination_config.destinations.len() as u16,
        limits,
    )
    .await?;

    let mut block_processor = BlockProcessor::new(block_processor_storage, queue_rear, queue_front);

    start_exporters(
        shutdown_signal.clone(),
        options,
        limits.work_queue_size.into(),
        clients_per_thread,
        exporter_storage,
        destination_config,
    )
    .unwrap();

    block_processor
        .run_with_shutdown(shutdown_signal, limits.persistence_period)
        .await
        .unwrap();

    Ok(())
}
