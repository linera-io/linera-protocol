// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Comparison of per-job CI runtimes between a PR and its base branch.

use std::collections::BTreeMap;

use anyhow::{ensure, Result};
use octocrab::models::workflows::{Conclusion, Job, Status};
use serde::Serialize;
use tracing::warn;

type WorkflowName = String;

/// The runtime comparison for a single CI job between the base branch and the PR.
#[derive(Serialize)]
pub struct WorkflowJobRuntimeComparison {
    name: String,
    workflow_name: String,
    base_runtime: u64,
    pr_runtime: u64,
    runtime_difference_pct: f64,
}

impl WorkflowJobRuntimeComparison {
    /// Returns the job name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the name of the workflow the job belongs to.
    pub fn workflow_name(&self) -> &str {
        &self.workflow_name
    }

    /// Returns the job's runtime on the base branch, in seconds.
    pub fn base_runtime(&self) -> u64 {
        self.base_runtime
    }

    /// Returns the job's runtime on the PR, in seconds.
    pub fn pr_runtime(&self) -> u64 {
        self.pr_runtime
    }

    /// Returns the PR runtime relative to the base, as a percentage difference.
    pub fn runtime_difference_pct(&self) -> f64 {
        self.runtime_difference_pct
    }
}

/// Per-job runtime comparisons, keyed by workflow name.
#[derive(Serialize)]
pub struct CiRuntimeComparison(
    /// The comparisons for each workflow's jobs, keyed by workflow name.
    pub BTreeMap<WorkflowName, Vec<WorkflowJobRuntimeComparison>>,
);

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

    /// Builds the comparison from the base-branch jobs and the PR jobs, matching them by
    /// workflow and job name.
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
