#![doc = include_str!("../README.md")]

use std::collections::{BTreeMap, BTreeSet};

use aliri_braid::braid;
use compact_str::{format_compact, CompactString};
use modyne::{keys, Entity, Table};
use svix_ksuid::Ksuid;
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug)]
pub struct App {
    table_name: std::sync::Arc<str>,
    client: aws_sdk_dynamodb::Client,
}

impl App {
    pub fn new(client: aws_sdk_dynamodb::Client) -> Self {
        Self::new_with_table(client, "GitHubTable")
    }

    pub fn new_with_table(client: aws_sdk_dynamodb::Client, table_name: &str) -> Self {
        Self {
            table_name: std::sync::Arc::from(table_name),
            client,
        }
    }
}

impl Table for App {
    type PrimaryKey = keys::Primary;
    type IndexKeys = (keys::Gsi1, keys::Gsi2, keys::Gsi3);

    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn client(&self) -> &aws_sdk_dynamodb::Client {
        &self.client
    }
}

#[braid(serde)]
pub struct OwnerName(CompactString);

#[braid(serde)]
pub struct RepoName(CompactString);

#[derive(Clone, Copy, Debug)]
pub struct RepositoryId<'a> {
    pub repo_owner: &'a OwnerNameRef,
    pub repo_name: &'a RepoNameRef,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RepositoryIdentity {
    pub repo_owner: OwnerName,
    pub repo_name: RepoName,
}

impl RepositoryIdentity {
    fn borrowed(&self) -> RepositoryId {
        RepositoryId {
            repo_owner: &self.repo_owner,
            repo_name: &self.repo_name,
        }
    }
}

#[derive(Clone, Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct Repository {
    #[serde(flatten)]
    pub id: RepositoryIdentity,
    pub created_at: time::OffsetDateTime,
    pub updated_at: time::OffsetDateTime,
    pub issues_and_pull_request_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork_source: Option<RepositoryIdentity>,
    pub fork_count: u32,
    pub star_count: u32,
}

impl Entity for Repository {
    type KeyInput<'a> = RepositoryId<'a>;
    type Table = App;
    type IndexKeys = (keys::Gsi1, keys::Gsi2, keys::Gsi3);

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let common = format!("REPO#{}#{}", input.repo_owner, input.repo_name);
        keys::Primary {
            hash: common.clone(),
            range: common,
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        let primary = Self::primary_key(self.id.borrowed());
        let updated_at = self.updated_at.format(&Rfc3339).unwrap();

        let fork_index = if let Some(source) = &self.fork_source {
            keys::Gsi2 {
                hash: format!("REPO#{}#{}", source.repo_owner, source.repo_name),
                range: format!("FORK#{}", self.id.repo_owner),
            }
        } else {
            keys::Gsi2 {
                hash: primary.hash.clone(),
                range: format!("#REPO#{}", self.id.repo_name),
            }
        };

        keys::FullKey {
            indexes: (
                keys::Gsi1 {
                    hash: primary.hash.clone(),
                    range: primary.range.clone(),
                },
                fork_index,
                keys::Gsi3 {
                    hash: format!("ACCOUNT#{}", self.id.repo_owner),
                    range: format!("#{}", updated_at),
                },
            ),
            primary,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct IssueId<'a> {
    repo: RepositoryId<'a>,
    issue_number: u32,
}

#[derive(Clone, Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct Issue {
    #[serde(flatten)]
    pub repo: RepositoryIdentity,
    pub issue_number: u32,
    pub created_at: time::OffsetDateTime,
    pub status: IssueStatus,
    pub star_count: u32,
}

impl Entity for Issue {
    type KeyInput<'a> = IssueId<'a>;
    type Table = App;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!("REPO#{}#{}", input.repo.repo_owner, input.repo.repo_name),
            range: format!("ISSUE#{:010}", input.issue_number),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(IssueId {
                repo: self.repo.borrowed(),
                issue_number: self.issue_number,
            }),
            indexes: (),
        }
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum IssueStatus {
    Open,
    Closed,
}

#[derive(Clone, Copy, Debug)]
pub struct IssueCommentId<'a> {
    repo: RepositoryId<'a>,
    issue_number: u32,
    comment_id: Ksuid,
}

#[derive(Clone, Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct IssueComment {
    #[serde(flatten)]
    pub repo: RepositoryIdentity,
    pub issue_number: u32,
    pub comment_id: Ksuid,
    pub created_at: time::OffsetDateTime,
    pub comment: String,
    pub star_count: u32,
}

impl Entity for IssueComment {
    type KeyInput<'a> = IssueCommentId<'a>;
    type Table = App;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!(
                "ISSUECOMMENT#{}#{}#{}",
                input.repo.repo_owner, input.repo.repo_name, input.issue_number
            ),
            range: format!("ISSUECOMMENT#{}", input.comment_id),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(IssueCommentId {
                repo: self.repo.borrowed(),
                issue_number: self.issue_number,
                comment_id: self.comment_id,
            }),
            indexes: (),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PullRequestId<'a> {
    repo: RepositoryId<'a>,
    pull_request_number: u32,
}

#[derive(Clone, Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct PullRequest {
    #[serde(flatten)]
    pub repo: RepositoryIdentity,
    pub pull_request_number: u32,
    pub created_at: time::OffsetDateTime,
    pub star_count: u32,
}

impl Entity for PullRequest {
    type KeyInput<'a> = PullRequestId<'a>;
    type Table = App;
    type IndexKeys = keys::Gsi1;

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let common = format!(
            "PR#{}#{}#{:010}",
            input.repo.repo_owner, input.repo.repo_name, input.pull_request_number
        );
        keys::Primary {
            hash: common.clone(),
            range: common,
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(PullRequestId {
                repo: self.repo.borrowed(),
                pull_request_number: self.pull_request_number,
            }),
            indexes: keys::Gsi1 {
                hash: format!("REPO#{}#{}", self.repo.repo_owner, self.repo.repo_name),
                range: format!("PR#{:010}", self.pull_request_number),
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PullRequestCommentId<'a> {
    repo: RepositoryId<'a>,
    pull_request_number: u32,
    comment_id: Ksuid,
}

#[derive(Clone, Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct PullRequestComment {
    #[serde(flatten)]
    pub repo: RepositoryIdentity,
    pub pull_request_number: u32,
    pub comment_id: Ksuid,
    pub created_at: time::OffsetDateTime,
    pub comment: String,
    pub star_count: u32,
}

impl Entity for PullRequestComment {
    type KeyInput<'a> = PullRequestCommentId<'a>;
    type Table = App;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!(
                "PRCOMMENT#{}#{}#{}",
                input.repo.repo_owner, input.repo.repo_name, input.pull_request_number
            ),
            range: format!("PRCOMMENT#{}", input.comment_id),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(PullRequestCommentId {
                repo: self.repo.borrowed(),
                pull_request_number: self.pull_request_number,
                comment_id: self.comment_id,
            }),
            indexes: (),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct StarId<'a> {
    pub repo: RepositoryId<'a>,
    pub staring_user: &'a OwnerNameRef,
}

#[derive(Clone, Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct Star {
    pub repo: RepositoryIdentity,
    pub staring_user: OwnerName,
}

impl Entity for Star {
    type KeyInput<'a> = StarId<'a>;
    type Table = App;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!("REPO#{}#{}", input.repo.repo_owner, input.repo.repo_name),
            range: format!("STAR#{}", input.staring_user),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(StarId {
                repo: self.repo.borrowed(),
                staring_user: &self.staring_user,
            }),
            indexes: (),
        }
    }
}

pub struct ReactionId<'a> {
    pub repo: RepositoryId<'a>,
    pub target_type: ReactionTarget,
    pub reacting_user: &'a OwnerNameRef,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "target_type", content = "target_id")]
pub enum ReactionTarget {
    Issue(u32),
    IssueComment(Ksuid),
    PullRequest(u32),
    PullRequestComment(Ksuid),
}

impl ReactionTarget {
    fn fmt_components(&self) -> (&'static str, CompactString) {
        match self {
            ReactionTarget::Issue(num) => ("ISSUE", format_compact!("{:010}", num)),
            ReactionTarget::IssueComment(id) => ("ISSUECOMMENT", format_compact!("{}", id)),
            ReactionTarget::PullRequest(num) => ("PR", format_compact!("{:010}", num)),
            ReactionTarget::PullRequestComment(id) => ("PRCOMMENT", format_compact!("{}", id)),
        }
    }
}

#[derive(Clone, Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct Reaction {
    #[serde(flatten)]
    pub repo: RepositoryIdentity,
    #[serde(flatten)]
    pub target_type: ReactionTarget,
    pub reacting_user: OwnerName,
    #[serde(
        default,
        skip_serializing_if = "BTreeSet::is_empty",
        with = "serde_dynamo::string_set"
    )]
    pub reactions: BTreeSet<CompactString>,
}

impl Entity for Reaction {
    type KeyInput<'a> = ReactionId<'a>;
    type Table = App;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let (target_type, target_id) = input.target_type.fmt_components();
        let common = format!(
            "{}REACTION#{}#{}#{}#{}",
            target_type,
            input.repo.repo_owner,
            input.repo.repo_name,
            target_id,
            input.reacting_user
        );
        keys::Primary {
            hash: common.clone(),
            range: common,
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(ReactionId {
                repo: self.repo.borrowed(),
                target_type: self.target_type,
                reacting_user: &self.reacting_user,
            }),
            indexes: (),
        }
    }
}

#[derive(Clone, Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct User {
    pub username: OwnerName,
    pub created_at: time::OffsetDateTime,
    pub organizations: BTreeMap<OwnerName, Role>,
    pub payment_plan: PaymentPlan,
}

impl Entity for User {
    type KeyInput<'a> = &'a OwnerNameRef;
    type Table = App;
    type IndexKeys = keys::Gsi3;

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let common = format!("ACCOUNT#{}", input);
        keys::Primary {
            hash: common.clone(),
            range: common,
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        let primary = Self::primary_key(&self.username);
        keys::FullKey {
            indexes: keys::Gsi3 {
                hash: primary.hash.clone(),
                range: primary.range.clone(),
            },
            primary,
        }
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum Role {
    Owner,
    Member,
}

#[derive(Clone, Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct Organization {
    pub organization_name: OwnerName,
    pub created_at: time::OffsetDateTime,
    pub payment_plan: PaymentPlan,
}

impl Entity for Organization {
    type KeyInput<'a> = &'a OwnerNameRef;
    type Table = App;
    type IndexKeys = keys::Gsi3;

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let common = format!("ACCOUNT#{}", input);
        keys::Primary {
            hash: common.clone(),
            range: common,
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        let primary = Self::primary_key(&self.organization_name);
        keys::FullKey {
            indexes: keys::Gsi3 {
                hash: primary.hash.clone(),
                range: primary.range.clone(),
            },
            primary,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MembershipId<'a> {
    pub organization: &'a OwnerNameRef,
    pub username: &'a OwnerNameRef,
}

#[derive(Clone, Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct Membership {
    pub organization: OwnerName,
    pub username: OwnerName,
    pub created_at: time::OffsetDateTime,
    pub role: Role,
}

impl Entity for Membership {
    type KeyInput<'a> = MembershipId<'a>;
    type Table = App;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!("ACCOUNT#{}", input.organization),
            range: format!("MEMBERSHIP#{}", input.username),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(MembershipId {
                organization: &self.organization,
                username: &self.username,
            }),
            indexes: (),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PaymentPlan {
    pub plan_type: PlanType,
    pub plan_start_date: time::OffsetDateTime,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum PlanType {
    Free,
    Pro,
    Enterprise,
}
