// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides the internal tool executable for performance summary generation.

#![deny(clippy::large_futures)]
#![allow(missing_docs)]

use std::{
    collections::{HashMap, HashSet},
    env, process,
};

use anyhow::{anyhow, ensure, Context, Result};
use linera_version::VersionInfo;
use octocrab::{
    models::workflows::{Conclusion, Job, Run, Status, WorkFlow},
    params::workflows::Filter,
    workflows::WorkflowsHandler,
    Octocrab,
};
use serde::Serialize;
use tracing::{error, info, Instrument};

const API_REQUEST_DELAY_MS: u64 = 100;
const IGNORED_JOB_PREFIXES: &[&str] = &["lint-", "check-outdated-cli-md"];

type WorkflowName = String;

#[derive(clap::Parser)]
#[command(
    name = "linera-summary",
    version = VersionInfo::default_clap_str(),
    about = "Executable for performance summary generation.",
)]
pub struct SummaryOptions {
    #[command(subcommand)]
    command: Command,

    /// The list of comma separated workflow names to track.
    #[arg(long, required = true)]
    workflows: String,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Run in CI mode.
    Ci,
    /// Run in local mode. Instead of commenting on the PR, the summary is printed to stdout.
    Local {
        /// PR number to analyze.
        #[arg(long, required = true)]
        pr: u64,
    },
}

impl SummaryOptions {
    pub fn init() -> Self {
        <SummaryOptions as clap::Parser>::parse()
    }

    pub fn is_local(&self) -> bool {
        matches!(self.command, Command::Local { .. })
    }

    pub fn pr_number(&self) -> Result<u64> {
        match self.command {
            Command::Local { pr } => Ok(pr),
            Command::Ci { .. } => Err(anyhow!("--pr should not be provided in CI mode")),
        }
    }

    pub fn workflows(&self) -> HashSet<String> {
        self.workflows.split(',').map(|s| s.to_string()).collect()
    }
}

struct GithubRepository {
    owner: String,
    name: String,
}

impl GithubRepository {
    fn from_env(is_local: bool) -> Result<Self> {
        let env_repository = env::var("GITHUB_REPOSITORY");
        let repository = if is_local {
            env_repository.unwrap_or_else(|_| "linera-io/linera-protocol".to_string())
        } else {
            env_repository.map_err(|_| {
                anyhow!("GITHUB_REPOSITORY is not set! This must be run from within CI")
            })?
        };
        let parts = repository.split('/').collect::<Vec<_>>();
        assert_eq!(parts.len(), 2);
        let owner = parts[0].to_string();
        let name = parts[1].to_string();
        Ok(Self { owner, name })
    }
}

struct GithubContext {
    repository: GithubRepository,
    pr_commit_hash: String,
    pr_branch: String,
    base_branch: String,
    pr_number: u64,
}

impl GithubContext {
    fn get_local_git_info() -> Result<(String, String, String)> {
        let repo = git2::Repository::open_from_env().context("Failed to open git repository")?;

        let head = repo.head()?;
        let commit_hash = head.peel_to_commit()?.id().to_string();

        let branch_name = if head.is_branch() {
            head.shorthand()
                .ok_or_else(|| anyhow!("Failed to get current branch name"))?
                .to_string()
        } else {
            anyhow::bail!("HEAD is not on a branch - it may be detached");
        };

        // This local mode is only used for testing, so we're just hardcoding `main` as the base branch for now.
        Ok((commit_hash, branch_name, "main".to_string()))
    }

    fn from_env(is_local: bool, pr_number: u64) -> Result<Self> {
        let env_pr_commit_hash = env::var("GITHUB_PR_COMMIT_HASH");
        let env_pr_branch = env::var("GITHUB_PR_BRANCH");
        let env_base_branch = env::var("GITHUB_BASE_BRANCH");
        let env_pr_number = env::var("GITHUB_PR_NUMBER");

        let (pr_commit_hash, pr_branch, base_branch, pr_number) = if is_local {
            let (commit_hash, branch_name, base) = Self::get_local_git_info()?;
            (commit_hash, branch_name, base, pr_number)
        } else {
            let pr_string = env_pr_number.map_err(|_| {
                anyhow!("GITHUB_PR_NUMBER is not set! This must be run from within CI")
            })?;
            (
                env_pr_commit_hash.map_err(|_| {
                    anyhow!("GITHUB_PR_COMMIT_HASH is not set! This must be run from within CI")
                })?,
                env_pr_branch.map_err(|_| {
                    anyhow!("GITHUB_PR_BRANCH is not set! This must be run from within CI")
                })?,
                env_base_branch.map_err(|_| {
                    anyhow!("GITHUB_BASE_BRANCH is not set! This must be run from within CI")
                })?,
                pr_string.parse().map_err(|_| {
                    anyhow!("GITHUB_PR_NUMBER is not a valid number: {}", pr_string)
                })?,
            )
        };

        Ok(Self {
            repository: GithubRepository::from_env(is_local)?,
            pr_commit_hash,
            pr_branch,
            base_branch,
            pr_number,
        })
    }
}

struct Github {
    octocrab: Octocrab,
    context: GithubContext,
    is_local: bool,
}

impl Github {
    fn new(options: SummaryOptions) -> Result<Self> {
        let octocrab_builder = Octocrab::builder();
        let is_local = options.is_local();
        let octocrab =
            if is_local {
                octocrab_builder
            } else {
                octocrab_builder.personal_token(env::var("GITHUB_TOKEN").map_err(|_| {
                    anyhow!("GITHUB_TOKEN is not set! This must be run from within CI")
                })?)
            }
            .build()
            .map_err(|_| anyhow!("Creating Octocrab instance should not fail!"))?;

        Ok(Self {
            octocrab,
            context: GithubContext::from_env(is_local, options.pr_number()?)?,
            is_local,
        })
    }

    async fn latest_runs<'octo>(
        &self,
        branch: &str,
        event: &str,
        workflows_handler: &WorkflowsHandler<'octo>,
        workflows: &[WorkFlow],
    ) -> Result<Vec<Run>> {
        let mut latest_runs = Vec::new();
        for workflow in workflows {
            // Add a delay between requests to avoid rate limiting
            tokio::time::sleep(tokio::time::Duration::from_millis(API_REQUEST_DELAY_MS)).await;

            let runs = workflows_handler
                .list_runs(workflow.id.to_string())
                .branch(branch)
                .event(event)
                .status("success")
                .per_page(1)
                .send()
                .await?
                .items;
            if runs.is_empty() {
                // Not all workflows will necessarily have runs for the given branch and event.
                info!(
                    "No runs found for workflow \"{}\", on path \"{}\", for branch \"{}\" and event \"{}\", with \"success\" status",
                    workflow.name,
                    workflow.path,
                    branch,
                    event
                );
                continue;
            }
            info!(
                "Got latest run for workflow \"{}\", on path \"{}\", for branch \"{}\" and event \"{}\", with \"success\" status",
                workflow.name,
                workflow.path,
                branch,
                event
            );
            latest_runs.push(runs.first().unwrap().clone());
        }

        Ok(latest_runs)
    }

    async fn latest_jobs<'octo>(
        &self,
        branch: &str,
        event: &str,
        workflows_handler: &WorkflowsHandler<'octo>,
        workflows: &[WorkFlow],
    ) -> Result<Vec<Job>> {
        let latest_runs = self
            .latest_runs(branch, event, workflows_handler, workflows)
            .await?;
        let mut jobs = Vec::new();
        for run in latest_runs {
            // Add a delay between requests to avoid rate limiting
            tokio::time::sleep(tokio::time::Duration::from_millis(API_REQUEST_DELAY_MS)).await;

            let run_jobs = workflows_handler
                .list_jobs(run.id)
                .filter(Filter::Latest)
                .send()
                .await?
                .items;
            info!("Got {} jobs for run {}", run_jobs.len(), run.name);
            jobs.push(run_jobs);
        }

        let jobs = jobs.into_iter().flatten().collect::<Vec<_>>();
        let jobs_len = jobs.len();
        let jobs_filtered = jobs
            .into_iter()
            .filter(|job| {
                !IGNORED_JOB_PREFIXES
                    .iter()
                    .any(|prefix| job.name.starts_with(prefix))
            }) // Filter out jobs with ignored prefixes
            .collect::<Vec<_>>();

        info!("Filtered out {} jobs", jobs_len - jobs_filtered.len());
        info!("Returning {} jobs", jobs_filtered.len());

        Ok(jobs_filtered)
    }

    fn workflows_handler(&self) -> WorkflowsHandler {
        self.octocrab.workflows(
            self.context.repository.owner.clone(),
            self.context.repository.name.clone(),
        )
    }

    async fn workflows<'octo>(
        &self,
        workflows_handler: &WorkflowsHandler<'octo>,
    ) -> Result<Vec<WorkFlow>> {
        Ok(workflows_handler.list().send().await?.items)
    }
}

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
struct CiRuntimeComparison(HashMap<WorkflowName, Vec<WorkflowJobRuntimeComparison>>);

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

    fn from_jobs(base_jobs: Vec<Job>, pr_jobs: Vec<Job>) -> Result<Self> {
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

#[derive(Serialize)]
struct PerformanceSummary {
    #[serde(skip_serializing)]
    github: Github,
    ci_runtime_comparison: CiRuntimeComparison,
}

impl PerformanceSummary {
    async fn init(github: Github, tracked_workflows: HashSet<WorkflowName>) -> Result<Self> {
        let workflows_handler = github.workflows_handler();
        let workflows = github
            .workflows(&workflows_handler)
            .await?
            .into_iter()
            .filter(|workflow| tracked_workflows.contains(&workflow.name))
            .collect::<Vec<_>>();
        let base_jobs = Box::pin(github.latest_jobs(
            &github.context.base_branch,
            "push",
            &workflows_handler,
            &workflows,
        ))
        .await?;
        let pr_jobs = Box::pin(github.latest_jobs(
            &github.context.pr_branch,
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

    async fn comment_on_pr(&self) -> Result<()> {
        let body = format!(
            "## CI Performance Summary for commit {}\n\n```json\n{}\n```",
            &self.github.context.pr_commit_hash[..7],
            serde_json::to_string_pretty(self)?
        );

        if self.github.is_local {
            info!("Printing summary to stdout:");
            println!("{}", body);
        } else {
            info!("Commenting on PR {}", self.github.context.pr_number);
            self.github
                .octocrab
                .issues(
                    self.github.context.repository.owner.clone(),
                    self.github.context.repository.name.clone(),
                )
                .create_comment(self.github.context.pr_number, body)
                .await?;
        }

        Ok(())
    }
}

async fn run(options: SummaryOptions) -> Result<i32> {
    let tracked_workflows = options.workflows();
    let github = Github::new(options)?;
    let summary = PerformanceSummary::init(github, tracked_workflows).await?;
    summary.comment_on_pr().await?;
    Ok(0)
}

fn main() -> anyhow::Result<()> {
    let options = SummaryOptions::init();

    linera_base::tracing::init("summary");

    let mut runtime = tokio::runtime::Builder::new_multi_thread();

    let span = tracing::info_span!("linera-summary::main");

    let result = runtime
        .enable_all()
        .build()
        .context("Failed to create Tokio runtime")?
        .block_on(run(options).instrument(span));

    let error_code = match result {
        Ok(code) => code,
        Err(msg) => {
            error!("Error is {:?}", msg);
            2
        }
    };
    process::exit(error_code);
}
