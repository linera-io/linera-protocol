// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use anyhow::{anyhow, ensure, Result};
use octocrab::models::workflows::{Conclusion, Job, Status};
use serde::Serialize;

type WorkflowName = String;

#[derive(Serialize)]
struct WorkflowJobRuntimeComparison {
    name: String,
    workflow_name: String,
    base_runtime: String,
    pr_runtime: String,
    runtime_difference_pct: String,
}

// The key is the name of the workflow, and the value is a list of per job comparisons.
#[derive(Serialize)]
pub struct CiRuntimeComparison(HashMap<WorkflowName, Vec<WorkflowJobRuntimeComparison>>);

impl CiRuntimeComparison {
    fn get_runtimes(jobs: Vec<Job>) -> Result<HashMap<String, HashMap<String, u64>>> {
        let mut runtimes: HashMap<String, HashMap<String, u64>> = HashMap::new();
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
                    Ok((
                        workflow_name.clone(),
                        jobs.into_iter()
                            .map(|(job_name, pr_runtime)| {
                                let base_runtime = *base_runtimes
                                    .get(&workflow_name)
                                    .ok_or_else(|| {
                                        anyhow!("No base runtime for workflow {}", workflow_name)
                                    })?
                                    .get(&job_name)
                                    .ok_or_else(|| {
                                        anyhow!("No base runtime for job {}", job_name)
                                    })?;
                                Ok(WorkflowJobRuntimeComparison {
                                    name: job_name.clone(),
                                    workflow_name: workflow_name.clone(),
                                    pr_runtime: format!("{}s", pr_runtime),
                                    base_runtime: format!("{}s", base_runtime),
                                    runtime_difference_pct: format!(
                                        "{:+.2}%",
                                        ((pr_runtime as f64) / (base_runtime as f64) - 1.0) * 100.0
                                    ),
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                    ))
                })
                .collect::<Result<HashMap<_, _>>>()?,
        ))
    }
}
