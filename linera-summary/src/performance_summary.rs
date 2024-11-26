// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use anyhow::Result;
use serde::Serialize;

use crate::{ci_runtime_comparison::CiRuntimeComparison, github::Github};

#[derive(Serialize)]
pub struct PerformanceSummary {
    #[serde(skip_serializing)]
    github: Github,
    ci_runtime_comparison: CiRuntimeComparison,
}

impl PerformanceSummary {
    pub async fn init(github: Github, tracked_workflows: HashSet<String>) -> Result<Self> {
        let workflows_handler = github.workflows_handler();
        let workflows = github
            .workflows(&workflows_handler)
            .await?
            .into_iter()
            .filter(|workflow| tracked_workflows.contains(&workflow.name))
            .collect::<Vec<_>>();
        let base_jobs = Box::pin(github.latest_jobs(
            github.context().base_branch(),
            "push",
            &workflows_handler,
            &workflows,
        ))
        .await?;
        let pr_jobs = Box::pin(github.latest_jobs(
            github.context().pr_branch(),
            "pull_request",
            &workflows_handler,
            &workflows,
        ))
        .await?;

        Ok(Self {
            github,
            ci_runtime_comparison: CiRuntimeComparison::from_jobs(base_jobs, pr_jobs)?,
        })
    }

    pub async fn comment_on_pr(&self) -> Result<()> {
        let body = format!(
            "## CI Performance Summary for commit {}\n\n```json\n{}\n```",
            &self.github.context().pr_commit_hash()[..7],
            serde_json::to_string_pretty(self)?
        );

        self.github.comment_on_pr(&body).await?;
        Ok(())
    }
}
