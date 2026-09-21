use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use bcrypt::{DEFAULT_COST, hash};
use chrono::{DateTime, TimeZone, Utc};
use diesel::{ExpressionMethods, QueryDsl, delete, insert_into};
use diesel_async::{AsyncConnection, RunQueryDsl};
use gumdrop::Options;
use indicatif::ProgressBar;
use lemmy_db_schema::{
    newtypes::{CommentId, DbUrl, PostId},
    schema::{comment_like, community, post_like},
    source::{
        comment::{Comment, CommentInsertForm, CommentLikeForm},
        community::Community,
        local_user::{LocalUser, LocalUserInsertForm},
        person::{Person, PersonInsertForm},
        post::{Post, PostInsertForm, PostLikeForm},
    },
    traits::{ApubActor, Crud},
    utils::{ActualDbPool, DbPool, build_db_pool, get_conn},
};
use lemmy_db_views::structs::SiteView;
use log::{LevelFilter, error, info};
use rand::{Rng, distr::Alphanumeric};
use serde::Deserialize;
use tokio::fs;
use url::Url;
use walkdir::WalkDir;

#[derive(Debug, Options)]
struct CommandOptions {
    #[options(help = "print help message")]
    help: bool,
    #[options(help = "be verbose")]
    verbose: bool,
    #[options(command)]
    command: Option<Command>,
}

#[derive(Debug, Options)]
enum Command {
    #[options(help = "insert a BDFR JSON archive into a Lemmy database")]
    Import(ImportOptions),
}

#[derive(Debug, Options)]
struct ImportOptions {
    #[options(help = "print import help message")]
    help: bool,
    #[options(help = "archive folder path", required)]
    archive_path: PathBuf,
    #[options(help = "only create mirror users; do not insert posts")]
    gen_users_only: bool,
    #[options(help = "force all posts to be not marked NSFW")]
    dont_mark_nsfw: bool,
    #[options(help = "load users from the database for each post (compatibility option)")]
    load_users_every_post: bool,
    #[options(help = "show only progress and errors")]
    only_progress: bool,
    #[options(help = "compatibility option; imports are always sequential")]
    no_tasks: bool,
    #[options(help = "map imported authors to this username")]
    username_override: Option<String>,
    #[options(help = "skip posts and comment subtrees with a score below 1")]
    skip_low_score: bool,
}

#[derive(Debug, Deserialize)]
struct RedditPost {
    title: String,
    url: String,
    selftext: String,
    score: i32,
    permalink: String,
    id: String,
    author: String,
    over_18: bool,
    locked: bool,
    created_utc: f64,
    comments: Vec<RedditComment>,
}

#[derive(Debug, Deserialize)]
struct RedditComment {
    author: String,
    id: String,
    score: i32,
    body: String,
    distinguished: Option<String>,
    created_utc: f64,
    replies: Vec<RedditComment>,
}

#[derive(Default)]
struct ImportStats {
    succeeded: usize,
    skipped: usize,
    failed: usize,
}

struct Importer<'a> {
    pool: &'a ActualDbPool,
    site_view: SiteView,
    communities: HashMap<String, Community>,
    options: &'a ImportOptions,
    users: HashMap<String, Person>,
    voters: Vec<Person>,
}

impl<'a> Importer<'a> {
    async fn process_file(&mut self, path: &Path) -> Result<ProcessResult> {
        let contents = fs::read(path)
            .await
            .with_context(|| format!("failed to read {}", path.display()))?;
        let post: RedditPost = serde_json::from_slice(&contents)
            .with_context(|| format!("failed to parse JSON in {}", path.display()))?;

        let community = if self.options.gen_users_only {
            None
        } else {
            let community_name = post
                .permalink
                .strip_prefix("/r/")
                .and_then(|value| value.split('/').next())
                .filter(|value| !value.is_empty())
                .map(|value| format!("{value}Mirror"))
                .ok_or_else(|| anyhow!("{} has no usable subreddit permalink", path.display()))?;
            Some(
                self.communities
                    .get(&community_name)
                    .cloned()
                    .ok_or_else(|| anyhow!("Lemmy community does not exist: {community_name}"))?,
            )
        };

        let mut pool = DbPool::Pool(self.pool);
        let mut connection = get_conn(&mut pool)
            .await
            .context("failed to obtain a database connection")?;

        let transaction_result = connection
            .transaction(|connection| {
                Box::pin(async {
                    let mut transaction_pool = DbPool::Conn(connection);
                    self.import_post(&mut transaction_pool, &post, community.as_ref())
                        .await
                })
            })
            .await;

        match transaction_result {
            Ok(result) => Ok(result),
            Err(import_error) => {
                // The transaction may have rolled back users created while processing this file.
                // Discard both caches so the next file re-reads database state.
                self.users.clear();
                self.voters.clear();
                Err(import_error).with_context(|| format!("failed to import {}", path.display()))
            }
        }
    }

    async fn import_post(
        &mut self,
        pool: &mut DbPool<'_>,
        post: &RedditPost,
        community: Option<&Community>,
    ) -> Result<ProcessResult> {
        if self.options.gen_users_only {
            self.create_users_for_post(pool, post).await?;
            return Ok(ProcessResult::Imported);
        }

        if self.options.skip_low_score && post.score < 1 {
            return Ok(ProcessResult::Skipped);
        }

        let community = community.ok_or_else(|| anyhow!("no community was resolved for post"))?;
        let post_author = self.get_or_create_user(pool, &post.author, true).await?;
        let published = reddit_timestamp(post.created_utc)?;
        let title = truncate_chars(&post.title, 200);
        let post_form = PostInsertForm::builder()
            .name(title)
            .creator_id(post_author.id)
            .community_id(community.id)
            .nsfw(Some(!self.options.dont_mark_nsfw && post.over_18))
            .locked(Some(post.locked))
            .deleted(Some(false))
            .removed(Some(false))
            .local(Some(true))
            .url(parse_db_url(&post.url))
            .body(Some(post.selftext.clone()))
            .published(Some(published))
            .ap_id(Some(source_post_url(&post.id)?.into()))
            .build();

        let imported_post = Post::insert_apub(pool, Utc::now(), &post_form)
            .await
            .context("failed to upsert post")?;

        self.apply_post_score(pool, imported_post.id, post.score)
            .await?;

        let mut imported_comments = HashMap::<String, Comment>::new();
        let mut flattened_comments = Vec::new();
        flatten_comments(
            &post.comments,
            None,
            self.options.skip_low_score,
            &mut flattened_comments,
        );

        for (parent_id, comment) in flattened_comments {
            let parent_path = parent_id
                .and_then(|id| imported_comments.get(id))
                .map(|parent| &parent.path);
            let author_is_op = comment.author == post.author;
            let author = self
                .get_or_create_user(pool, &comment.author, author_is_op)
                .await?;
            let comment_form = CommentInsertForm::builder()
                .creator_id(author.id)
                .post_id(imported_post.id)
                .content(comment.body.clone())
                .published(Some(reddit_timestamp(comment.created_utc)?))
                .deleted(Some(comment.body == "[deleted]"))
                .removed(Some(false))
                .local(Some(true))
                .distinguished(Some(comment.distinguished.is_some()))
                .ap_id(Some(source_comment_url(&post.id, &comment.id)?.into()))
                .build();

            let imported_comment =
                Comment::insert_apub(pool, Some(Utc::now()), &comment_form, parent_path)
                    .await
                    .with_context(|| format!("failed to upsert comment {}", comment.id))?;

            imported_comments.insert(comment.id.clone(), imported_comment.clone());
            self.apply_comment_score(pool, imported_comment.id, imported_post.id, comment.score)
                .await?;
        }

        Ok(ProcessResult::Imported)
    }

    async fn create_users_for_post(
        &mut self,
        pool: &mut DbPool<'_>,
        post: &RedditPost,
    ) -> Result<()> {
        self.get_or_create_user(pool, &post.author, true).await?;
        let mut comments = Vec::new();
        flatten_comments(&post.comments, None, false, &mut comments);
        for (_, comment) in comments {
            self.get_or_create_user(pool, &comment.author, comment.author == post.author)
                .await?;
        }
        Ok(())
    }

    async fn get_or_create_user(
        &mut self,
        pool: &mut DbPool<'_>,
        source_username: &str,
        is_op: bool,
    ) -> Result<Person> {
        let username = imported_username(
            source_username,
            self.options.username_override.as_deref(),
            is_op,
        );

        if let Some(user) = self.users.get(&username) {
            return Ok(user.clone());
        }

        if let Some(user) = Person::read_from_name(pool, &username, false)
            .await
            .context("failed to look up imported user")?
        {
            self.users.insert(username, user.clone());
            return Ok(user);
        }

        let person_form = PersonInsertForm::new(
            username.clone(),
            self.site_view.site.public_key.clone(),
            self.site_view.site.instance_id,
        );
        let user = Person::create(pool, &person_form)
            .await
            .with_context(|| format!("failed to create imported user {username}"))?;

        let random_password = random_string(32);
        let password_hash = hash(random_password, DEFAULT_COST)
            .context("failed to hash generated imported-user password")?;
        let local_user_form = LocalUserInsertForm::new(user.id, password_hash);
        LocalUser::create(pool, &local_user_form, vec![])
            .await
            .with_context(|| format!("failed to create local user record for {username}"))?;

        self.users.insert(username, user.clone());
        Ok(user)
    }

    async fn ensure_voters(&mut self, pool: &mut DbPool<'_>, count: usize) -> Result<()> {
        while self.voters.len() < count {
            let username = format!("reddit-vote-{}", self.voters.len() + 1);
            let user = self.get_or_create_named_user(pool, &username).await?;
            self.voters.push(user);
        }
        Ok(())
    }

    async fn get_or_create_named_user(
        &mut self,
        pool: &mut DbPool<'_>,
        username: &str,
    ) -> Result<Person> {
        if let Some(user) = self.users.get(username) {
            return Ok(user.clone());
        }

        if let Some(user) = Person::read_from_name(pool, username, false)
            .await
            .context("failed to look up synthetic voter")?
        {
            self.users.insert(username.to_owned(), user.clone());
            return Ok(user);
        }

        let person_form = PersonInsertForm::new(
            username.to_owned(),
            self.site_view.site.public_key.clone(),
            self.site_view.site.instance_id,
        );
        let user = Person::create(pool, &person_form)
            .await
            .with_context(|| format!("failed to create synthetic voter {username}"))?;
        let random_password = random_string(32);
        let password_hash = hash(random_password, DEFAULT_COST)
            .context("failed to hash generated voter password")?;
        let local_user_form = LocalUserInsertForm::new(user.id, password_hash);
        LocalUser::create(pool, &local_user_form, vec![])
            .await
            .with_context(|| format!("failed to create local user record for {username}"))?;
        self.users.insert(username.to_owned(), user.clone());
        Ok(user)
    }

    async fn apply_post_score(
        &mut self,
        pool: &mut DbPool<'_>,
        post_id: PostId,
        score: i32,
    ) -> Result<()> {
        let count = usize::try_from(score.unsigned_abs())
            .context("post score is too large to represent")?;
        self.ensure_voters(pool, count).await?;
        let connection = &mut get_conn(pool).await?;
        let stale_voter_ids: Vec<_> = self.voters[count..].iter().map(|user| user.id).collect();
        if !stale_voter_ids.is_empty() {
            delete(
                post_like::table
                    .filter(post_like::post_id.eq(post_id))
                    .filter(post_like::person_id.eq_any(&stale_voter_ids)),
            )
            .execute(connection)
            .await
            .context("failed to remove stale post votes")?;
        }
        if count == 0 {
            return Ok(());
        }

        let score_value = if score >= 0 { 1 } else { -1 };
        let forms: Vec<_> = self.voters[..count]
            .iter()
            .map(|user| PostLikeForm {
                post_id,
                person_id: user.id,
                score: score_value,
            })
            .collect();
        insert_into(post_like::table)
            .values(&forms)
            .on_conflict((post_like::post_id, post_like::person_id))
            .do_update()
            .set(post_like::score.eq(score_value))
            .execute(connection)
            .await
            .context("failed to apply post score")?;
        Ok(())
    }

    async fn apply_comment_score(
        &mut self,
        pool: &mut DbPool<'_>,
        comment_id: CommentId,
        post_id: PostId,
        score: i32,
    ) -> Result<()> {
        let count = usize::try_from(score.unsigned_abs())
            .context("comment score is too large to represent")?;
        self.ensure_voters(pool, count).await?;
        let connection = &mut get_conn(pool).await?;
        let stale_voter_ids: Vec<_> = self.voters[count..].iter().map(|user| user.id).collect();
        if !stale_voter_ids.is_empty() {
            delete(
                comment_like::table
                    .filter(comment_like::comment_id.eq(comment_id))
                    .filter(comment_like::person_id.eq_any(&stale_voter_ids)),
            )
            .execute(connection)
            .await
            .context("failed to remove stale comment votes")?;
        }
        if count == 0 {
            return Ok(());
        }

        let score_value = if score >= 0 { 1 } else { -1 };
        let forms: Vec<_> = self.voters[..count]
            .iter()
            .map(|user| CommentLikeForm {
                comment_id,
                post_id,
                person_id: user.id,
                score: score_value,
            })
            .collect();
        insert_into(comment_like::table)
            .values(&forms)
            .on_conflict((comment_like::comment_id, comment_like::person_id))
            .do_update()
            .set(comment_like::score.eq(score_value))
            .execute(connection)
            .await
            .context("failed to apply comment score")?;
        Ok(())
    }
}

#[derive(Debug)]
enum ProcessResult {
    Imported,
    Skipped,
}

#[tokio::main]
async fn main() -> Result<()> {
    let options = CommandOptions::parse_args_default_or_exit();
    let only_progress = matches!(
        &options.command,
        Some(Command::Import(import_options)) if import_options.only_progress
    );
    init_logging(options.verbose, only_progress);

    match options.command {
        Some(Command::Import(import_options)) => run_import(import_options).await,
        None => bail!("no command supplied; use import <archive-path>"),
    }
}

async fn run_import(options: ImportOptions) -> Result<()> {
    if !options.archive_path.is_dir() {
        bail!(
            "archive path is not a directory: {}",
            options.archive_path.display()
        );
    }
    if options.no_tasks {
        info!("imports are processed sequentially; --no-tasks is retained for compatibility");
    }
    if options.load_users_every_post {
        info!("--load-users-every-post is deprecated and ignored; users are cached safely");
    }

    require_env("LEMMY_INITIALIZE_WITH_DEFAULT_SETTINGS")?;
    require_env("LEMMY_DATABASE_URL")?;

    let files = collect_archive_files(&options.archive_path)?;
    if files.is_empty() {
        bail!(
            "no JSON files found under {}",
            options.archive_path.display()
        );
    }

    info!("connecting to Lemmy database");
    let actual_pool = build_db_pool()
        .await
        .map_err(|error| anyhow!("failed to create Lemmy database pool: {error:?}"))?;
    let mut db_pool = DbPool::Pool(&actual_pool);
    let site_view = SiteView::read_local(&mut db_pool)
        .await
        .context("failed to read local Lemmy site")?
        .ok_or_else(|| anyhow!("no local Lemmy site is configured"))?;
    let mut connection = get_conn(&mut db_pool)
        .await
        .context("failed to obtain a database connection")?;
    let communities = community::table
        .load::<Community>(&mut connection)
        .await
        .context("failed to load Lemmy communities")?
        .into_iter()
        .map(|community| (community.name.clone(), community))
        .collect();

    let progress = ProgressBar::new(files.len() as u64);
    progress.enable_steady_tick(Duration::from_millis(100));
    let mut importer = Importer {
        pool: &actual_pool,
        site_view,
        communities,
        options: &options,
        users: HashMap::new(),
        voters: Vec::new(),
    };
    let mut stats = ImportStats::default();

    for path in files {
        match importer.process_file(&path).await {
            Ok(ProcessResult::Imported) => stats.succeeded += 1,
            Ok(ProcessResult::Skipped) => stats.skipped += 1,
            Err(import_error) => {
                stats.failed += 1;
                error!("{import_error:#} (file: {})", path.display());
            }
        }
        progress.inc(1);
    }
    progress.finish();

    info!(
        "import complete: {} succeeded, {} skipped, {} failed",
        stats.succeeded, stats.skipped, stats.failed
    );
    if stats.failed > 0 {
        bail!("{} archive files failed to import", stats.failed);
    }
    Ok(())
}

fn init_logging(verbose: bool, only_progress: bool) {
    let mut builder = env_logger::Builder::new();
    builder.filter_level(if only_progress {
        LevelFilter::Error
    } else if verbose {
        LevelFilter::Debug
    } else {
        LevelFilter::Info
    });
    builder.init();
}

fn require_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("set {name} before running the importer"))
}

fn collect_archive_files(path: &Path) -> Result<Vec<PathBuf>> {
    WalkDir::new(path)
        .into_iter()
        .map(|entry| entry.with_context(|| format!("failed to walk {}", path.display())))
        .filter_map(|entry| match entry {
            Ok(entry) if entry.file_type().is_file() => Some(Ok(entry.into_path())),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .filter(|entry| {
            entry.as_ref().map_or(true, |path| {
                path.extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
            })
        })
        .collect()
}

fn flatten_comments<'a>(
    comments: &'a [RedditComment],
    parent_id: Option<&'a str>,
    skip_low_score: bool,
    output: &mut Vec<(Option<&'a str>, &'a RedditComment)>,
) {
    for comment in comments {
        if skip_low_score && comment.score < 1 {
            continue;
        }
        output.push((parent_id, comment));
        flatten_comments(&comment.replies, Some(&comment.id), skip_low_score, output);
    }
}

fn reddit_timestamp(seconds: f64) -> Result<DateTime<Utc>> {
    if !seconds.is_finite() {
        bail!("Reddit timestamp is not finite: {seconds}");
    }
    let seconds = seconds.trunc();
    if seconds < i64::MIN as f64 || seconds > i64::MAX as f64 {
        bail!("Reddit timestamp is out of range: {seconds}");
    }
    Utc.timestamp_opt(seconds as i64, 0)
        .single()
        .ok_or_else(|| anyhow!("invalid Reddit timestamp: {seconds}"))
}

fn imported_username(source: &str, override_username: Option<&str>, is_op: bool) -> String {
    if let Some(override_username) = override_username {
        let suffix = if is_op { " OP" } else { "" };
        return fit_username(&format!("{override_username}{suffix}"));
    }

    let cleaned: String = source
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' || character == '-' {
                character
            } else {
                '_'
            }
        })
        .collect();
    let base = if cleaned.is_empty() { "user" } else { &cleaned };
    fit_username(&format!("{base}-mirror"))
}

fn fit_username(value: &str) -> String {
    if value.chars().count() <= 20 {
        return value.to_owned();
    }
    let prefix = truncate_chars(value, 13);
    format!("{prefix}-{:06x}", stable_hash(value))
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn stable_hash(value: &str) -> u32 {
    value.bytes().fold(0x811c9dc5_u32, |hash, byte| {
        hash.wrapping_mul(0x01000193).wrapping_add(u32::from(byte))
    })
}

fn random_string(length: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

fn parse_db_url(value: &str) -> Option<DbUrl> {
    Url::parse(value).ok().map(Into::into)
}

fn source_post_url(post_id: &str) -> Result<Url> {
    Url::parse(&format!(
        "https://reddit2lemmy.invalid/import/post/{post_id}"
    ))
    .context("invalid Reddit post ID for source URL")
}

fn source_comment_url(post_id: &str, comment_id: &str) -> Result<Url> {
    Url::parse(&format!(
        "https://reddit2lemmy.invalid/import/post/{post_id}/comment/{comment_id}"
    ))
    .context("invalid Reddit comment ID for source URL")
}
