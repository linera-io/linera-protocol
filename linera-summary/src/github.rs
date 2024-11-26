// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::env;

use anyhow::{anyhow, Context, Result};
use octocrab::{
    models::workflows::{Job, Run, WorkFlow},
    params::workflows::Filter,
    workflows::WorkflowsHandler,
    Octocrab,
};
use tracing::info;

use crate::performance_summary::PR_COMMENT_HEADER;

const API_REQUEST_DELAY_MS: u64 = 100;
const IGNORED_JOB_PREFIXES: &[&str] = &["lint-", "check-outdated-cli-md"];

pub struct GithubRepository {
    owner: String,
    name: String,
}

impl GithubRepository {
    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn name(&self) -> &str {
        &self.name
    }

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

pub struct GithubContext {
    repository: GithubRepository,
    pr_commit_hash: String,
    pr_branch: String,
    base_branch: String,
    pr_number: u64,
}

impl GithubContext {
    pub fn base_branch(&self) -> &str {
        &self.base_branch
    }

    pub fn pr_branch(&self) -> &str {
        &self.pr_branch
    }

    pub fn pr_commit_hash(&self) -> &str {
        &self.pr_commit_hash
    }

    pub fn repository(&self) -> &GithubRepository {
        &self.repository
    }

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

    fn from_env(is_local: bool, pr_number: Option<u64>) -> Result<Self> {
        let env_pr_commit_hash = env::var("GITHUB_PR_COMMIT_HASH");
        let env_pr_branch = env::var("GITHUB_PR_BRANCH");
        let env_base_branch = env::var("GITHUB_BASE_BRANCH");
        let env_pr_number = env::var("GITHUB_PR_NUMBER");

        let (pr_commit_hash, pr_branch, base_branch, pr_number) = if is_local {
            let (commit_hash, branch_name, base) = Self::get_local_git_info()?;
            (
                commit_hash,
                branch_name,
                base,
                pr_number.ok_or_else(|| anyhow!("pr_number is None"))?,
            )
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

pub struct Github {
    octocrab: Octocrab,
    context: GithubContext,
    is_local: bool,
}

impl Github {
    pub fn new(is_local: bool, pr_number: Option<u64>) -> Result<Self> {
        let octocrab_builder = Octocrab::builder();
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
            context: GithubContext::from_env(is_local, pr_number)?,
            is_local,
        })
    }

    pub fn context(&self) -> &GithubContext {
        &self.context
    }

    // Updates an existing comment or creates a new one in the PR.
    pub async fn upsert_pr_comment(&self, body: String) -> Result<()> {
        let issue_handler = self.octocrab.issues(
            self.context.repository.owner.clone(),
            self.context.repository.name.clone(),
        );
        let existing_comment_id = issue_handler
            .list_comments(self.context.pr_number)
            .send()
            .await?
            .items
            .into_iter()
            .find_map(|comment| {
                if comment.user.login == "github-actions[bot]"
                    && comment
                        .body
                        .is_some_and(|body| body.starts_with(PR_COMMENT_HEADER))
                {
                    Some(comment.id)
                } else {
                    None
                }
            });

        // Always print the summary to stdout, as we'll use it to set the job summary in CI.
        info!("Printing summary to stdout...");
        println!("{}", body);

        if let Some(existing_comment_id) = existing_comment_id {
            if self.is_local {
                info!(
                    "Would have updated comment {} on PR {}, but is local",
                    existing_comment_id, self.context.pr_number
                );
            } else {
                info!(
                    "Updating existing comment {} on PR {}",
                    existing_comment_id, self.context.pr_number
                );
                issue_handler
                    .update_comment(existing_comment_id, body)
                    .await?;
            }
        } else if self.is_local {
            info!(
                "Would have commented on PR {}, but is local",
                self.context.pr_number
            );
        } else {
            info!("Commenting on PR {}", self.context.pr_number);
            issue_handler
                .create_comment(self.context.pr_number, body)
                .await?;
        }
        Ok(())
    }

    async fn latest_runs(
        &self,
        branch: &str,
        event: &str,
        workflows_handler: &WorkflowsHandler<'_>,
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

    pub async fn latest_jobs(
        &self,
        branch: &str,
        event: &str,
        workflows_handler: &WorkflowsHandler<'_>,
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

    pub fn workflows_handler(&self) -> WorkflowsHandler {
        self.octocrab.workflows(
            self.context.repository.owner.clone(),
            self.context.repository.name.clone(),
        )
    }

    pub async fn workflows(
        &self,
        workflows_handler: &WorkflowsHandler<'_>,
    ) -> Result<Vec<WorkFlow>> {
        Ok(workflows_handler.list().send().await?.items)
    }
}
