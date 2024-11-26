// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use anyhow::{ensure, Result};
use octocrab::models::workflows::{Conclusion, Job, Status};
use serde::Serialize;
use tracing::warn;

type WorkflowName = String;

#[derive(Serialize)]
pub struct WorkflowJobRuntimeComparison {
    name: String,
    workflow_name: String,
    base_runtime: u64,
    pr_runtime: u64,
    runtime_difference_pct: f64,
}

impl WorkflowJobRuntimeComparison {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn workflow_name(&self) -> &str {
        &self.workflow_name
    }

    pub fn base_runtime(&self) -> u64 {
        self.base_runtime
    }

    pub fn pr_runtime(&self) -> u64 {
        self.pr_runtime
    }

    pub fn runtime_difference_pct(&self) -> f64 {
        self.runtime_difference_pct
    }
}

// The key is the name of the workflow, and the value is a list of per job comparisons.
#[derive(Serialize)]
pub struct CiRuntimeComparison(pub BTreeMap<WorkflowName, Vec<WorkflowJobRuntimeComparison>>);

impl CiRuntimeComparison {
    fn get_runtimes(jobs: Vec<Job>) -> Result<BTreeMap<String, BTreeMap<String, u64>>> {
        let mut runtimes: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
        for job in jobs {
            ensure!(job.status == Status::Completed);
            ensure!(job.conclusion.is_some());
            ensure!(job.conclusion.unwrap() == Conclusion::Success);
            ensure!(job.completed_at.is_some());

            runtimes.entry(job.workflow_name).or_default().insert(
                job.name.clone(),
                job.completed_at
                    .unwrap()
                    .signed_duration_since(job.started_at)
                    .num_seconds()
                    .try_into()?,
            );
        }
        Ok(runtimes)
    }

    pub fn from_jobs(base_jobs: Vec<Job>, pr_jobs: Vec<Job>) -> Result<Self> {
        let base_runtimes = Self::get_runtimes(base_jobs)?;
        let pr_runtimes = Self::get_runtimes(pr_jobs)?;
        Ok(Self(
            pr_runtimes
                .into_iter()
                .map(|(workflow_name, jobs)| {
                    (
                        workflow_name.clone(),
                        jobs.into_iter()
                            .filter_map(|(job_name, pr_runtime)| {
                                let base_jobs_runtimes = base_runtimes.get(&workflow_name);
                                if let Some(base_jobs_runtimes) = base_jobs_runtimes {
                                    let base_runtime = base_jobs_runtimes.get(&job_name);
                                    if let Some(base_runtime) = base_runtime {
                                        return Some(WorkflowJobRuntimeComparison {
                                            name: job_name.clone(),
                                            workflow_name: workflow_name.clone(),
                                            pr_runtime,
                                            base_runtime: *base_runtime,
                                            runtime_difference_pct:
                                                ((pr_runtime as f64) / (*base_runtime as f64)
                                                    - 1.0)
                                                    * 100.0,
                                        });
                                    } else {
                                        warn!(
                                            "No base runtime information found for job {} of workflow {}",
                                            job_name, workflow_name
                                        );
                                    }
                                } else {
                                    warn!(
                                        "No base runtime information found for workflow {}",
                                        workflow_name
                                    );
                                }

                                None
                            })
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        ))
    }
}
