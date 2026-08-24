use crate::{
    change::{Change, ChangeSubject},
    recent_changes::{
        ChangedItem, RcCursor, RecentChanges, RecentChangesResults, RecentDeletions,
        RecentRedirects,
    },
    revision_compare::{RevisionCompare, RevisionId},
    wikidata_api::{WikidataApi, MAX_REVIDS_PER_REQUEST},
};
use anyhow::{anyhow, Result};
use futures::{join, StreamExt};
use ini::Ini;
use mysql_async::{from_row, prelude::Queryable, Pool};
use serde_json::{json, Value};
use std::{
    collections::{BTreeSet, HashMap},
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use toolforge::db::{get_db_connection_info, toolsdb, Cluster, DBConnectionInfo};
use wikimisc::timestamp::TimeStamp;

pub type TextId = u64;
pub type ItemId = u64;

/// Returned from `run_once` to indicate whether the bot should sleep or immediately process more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunResult {
    /// There is more work waiting. Don't sleep.
    MoreWork,
    /// We've caught up. Sleep before next iteration.
    CaughtUp,
}

/// The Wikidata replica database, without the `_p` suffix.
const WIKIDATA_DB: &str = "wikidatawiki";
/// The tool's own database on toolsdb.
const WDRC_DB: &str = "s55078__wdrc_p";
/// The tool's credentials file. The `toolforge` crate looks for this in `$HOME`,
/// which is not the tool's home directory in every execution context, so use the
/// absolute path when it exists.
const REPLICA_MY_CNF: &str = "/data/project/wdrc/replica.my.cnf";
/// Connection limit for a Toolforge tool account, and the `toolforge` crate default.
const DEFAULT_POOL_MAX: usize = 10;
/// Hosts serving the tool's own database, for regular and local (tunnelled) use.
const TOOLSDB_HOST: &str = "tools.db.svc.wikimedia.cloud";
const TOOLSDB_HOST_LOCAL: &str = "tools.db.svc.local.wmftest.net";

/// `recentchanges` rows per batch. Each batch costs one cursor round trip, so
/// larger batches catch up faster; the API requests within a batch are what
/// bounds the work, and they are grouped and run concurrently.
const MAX_RECENT_CHANGES: u64 = 5000;
/// Concurrent Wikidata API requests, each covering `ITEMS_PER_API_REQUEST` items.
const MAX_API_CONCURRENT: u64 = 8;
/// Items per API request: every item needs two revisions, and the API caps a
/// request at `MAX_REVIDS_PER_REQUEST` revision IDs.
const ITEMS_PER_API_REQUEST: usize = MAX_REVIDS_PER_REQUEST / 2;
/// Wait before retrying a failed API request. One retry is worthwhile because a
/// failure would otherwise drop a whole group of items.
const API_RETRY_DELAY: Duration = Duration::from_secs(2);
/// Rows per INSERT statement, to stay well below the server's `max_allowed_packet`.
const ROWS_PER_INSERT: usize = 2000;
/// Upper bound of the time window a single query looks at, which keeps range
/// scans on `recentchanges` bounded.
const QUERY_WINDOW: Duration = Duration::from_secs(60 * 60);
/// A timestamp far enough in the future to act as "no upper bound".
const NO_WINDOW_END: &str = "99991231235959";
/// Row limit for the deletion and redirect queries.
const AUX_QUERY_LIMIT: usize = 5000;
/// `meta` keys holding the processing cursors.
const META_RC_TIMESTAMP: &str = "timestamp";
const META_RC_ID: &str = "rc_id";
const META_REDIRECT: &str = "timestamp_redirect";
const META_DELETION: &str = "timestamp_deletion";
/// Default timeout for individual DB queries (seconds)
const DEFAULT_DB_TIMEOUT_SEC: u64 = 300;
/// Default timeout for entire run_once cycle (seconds)
const DEFAULT_RUN_TIMEOUT_SEC: u64 = 600;
/// Default sleep between bot loop iterations (seconds)
const DEFAULT_BOT_SLEEP_SEC: u64 = 10;
/// Default interval between deletion/redirect refreshes (seconds). Those change
/// slowly compared to recent changes, and their queries are comparatively
/// expensive, so they don't run on every pass of a catch-up loop.
const DEFAULT_AUX_INTERVAL_SEC: u64 = 300;
/// A cursor further behind than this means there is more work to do, even if the
/// last batch was not full. Comfortably above normal replica lag.
const CATCHUP_TOLERANCE: Duration = Duration::from_secs(600);

/// The span of `recentchanges` rows the replica currently holds. The table keeps
/// only a rolling window, and the replica may lag behind the live database, so
/// both ends matter when moving a cursor over it.
#[derive(Debug, Clone)]
struct ReplicaRange {
    oldest: String,
    newest: String,
}

/// One batch of rows for `deletions` or `redirects`.
#[derive(Debug)]
struct AuxBatch {
    updates: Vec<String>,
    /// Where the next batch resumes.
    cursor: String,
    /// More rows are waiting, so don't wait for the next interval.
    pending: bool,
}

#[derive(Debug)]
pub struct WdRc {
    text_cache: HashMap<String, TextId>,
    api: WikidataApi,
    wikidata_pool: Pool,
    wdrc_pool: Pool,
    wdrc_pool_max: usize,
    logging: bool,
    max_recent_changes: u64,
    max_api_concurrent: usize,
    db_timeout: Duration,
    run_timeout: Duration,
    bot_sleep: Duration,
    aux_interval: Duration,
    last_aux_run: Option<Instant>,
}

impl WdRc {
    pub fn new(config_file: &str) -> Result<WdRc> {
        let config = Self::read_config(config_file)?;
        let config_u64 = |key: &str, default: u64| {
            config
                .get(key)
                .and_then(|j| j.as_u64())
                .unwrap_or(default)
        };
        let max_api_concurrent = config_u64("max_api_concurrent", MAX_API_CONCURRENT) as usize;
        let (wikidata_pool, wdrc_pool, wdrc_pool_max) = Self::prepare_pools(&config)?;
        Ok(WdRc {
            text_cache: HashMap::new(),
            api: WikidataApi::new(max_api_concurrent)?,
            wikidata_pool,
            wdrc_pool,
            wdrc_pool_max,
            logging: config
                .get("logging")
                .unwrap_or(&json!(false))
                .as_bool()
                .unwrap_or(false),
            max_recent_changes: config_u64("max_recent_changes", MAX_RECENT_CHANGES),
            max_api_concurrent,
            db_timeout: Duration::from_secs(config_u64("db_timeout_sec", DEFAULT_DB_TIMEOUT_SEC)),
            run_timeout: Duration::from_secs(config_u64("run_timeout_sec", DEFAULT_RUN_TIMEOUT_SEC)),
            bot_sleep: Duration::from_secs(config_u64("bot_sleep_sec", DEFAULT_BOT_SLEEP_SEC)),
            aux_interval: Duration::from_secs(config_u64(
                "aux_interval_sec",
                DEFAULT_AUX_INTERVAL_SEC,
            )),
            last_aux_run: None,
        })
    }

    fn log(&self, msg: String) {
        if self.logging {
            println!("{}", msg);
        }
    }

    /// Returns the configured sleep duration between bot loop iterations.
    pub fn bot_sleep(&self) -> Duration {
        self.bot_sleep
    }

    /// Wraps an async operation with a timeout, returning an error if it exceeds `duration`.
    async fn with_timeout<T>(
        duration: Duration,
        label: &str,
        fut: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        match tokio::time::timeout(duration, fut).await {
            Ok(result) => result,
            Err(_) => Err(anyhow!("Timeout after {:?} in {}", duration, label)),
        }
    }

    /// End of the time window starting at `oldest`, as a MediaWiki timestamp.
    fn window_end(oldest: &str) -> String {
        TimeStamp::str2utc(oldest)
            .map(|dt| dt + QUERY_WINDOW)
            .map(|dt| TimeStamp::datetime(&dt))
            .unwrap_or_else(|| NO_WINDOW_END.to_string())
    }

    /// True if `timestamp` lies further in the past than `CATCHUP_TOLERANCE`.
    /// Timestamps are fixed-width and numeric, so they compare lexicographically.
    fn is_behind(timestamp: &str) -> bool {
        match TimeStamp::str2utc(&TimeStamp::now()).map(|now| now - CATCHUP_TOLERANCE) {
            Some(cutoff) => timestamp < TimeStamp::datetime(&cutoff).as_str(),
            None => false,
        }
    }

    /// Reads the cursor. A missing `rc_id` starts at 0, which makes the first
    /// batch re-read its timestamp's second, exactly as before this was tracked.
    async fn get_rc_cursor(&self) -> Result<RcCursor> {
        Ok(RcCursor {
            timestamp: self
                .get_key_value(META_RC_TIMESTAMP)
                .await?
                .unwrap_or_default(),
            rc_id: self
                .get_key_value(META_RC_ID)
                .await?
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
        })
    }

    /// The `rc_id` is stored first: should the second write fail, the next run
    /// re-reads rows it has already stored, which is safe, rather than skipping
    /// rows it has not.
    async fn set_rc_cursor(&self, cursor: &RcCursor) -> Result<()> {
        let timestamp = Self::sanitize_timestamp(&cursor.timestamp)?;
        self.set_key_value(META_RC_ID, &cursor.rc_id.to_string())
            .await?;
        self.set_key_value(META_RC_TIMESTAMP, timestamp).await
    }

    async fn get_recent_changes(&self, from: &RcCursor) -> Result<RecentChangesResults> {
        let results = self.get_next_recent_changes_batch(from).await?;
        let rc = RecentChangesResults::new(&results);
        self.log(format!(
            "{} rows from {}/{}; new: {}, changed: {}",
            rc.row_count(),
            from.timestamp,
            from.rc_id,
            rc.new_items().len(),
            rc.changed_items().len()
        ));
        Ok(rc)
    }

    async fn get_next_recent_changes_batch(&self, from: &RcCursor) -> Result<Vec<RecentChanges>> {
        let window_end = Self::window_end(&from.timestamp);
        // Keyset pagination: the range condition on `rc_timestamp` uses the
        // index, and the disjunction then excludes the rows of the boundary
        // second that were already read.
        let sql = "SELECT `rc_id`,`rc_source`,`rc_timestamp`,`rc_title`,`rc_this_oldid`,`rc_last_oldid` FROM `recentchanges`
			WHERE `rc_namespace`=0 AND `rc_timestamp`>=? AND `rc_timestamp`<=? AND (`rc_timestamp`>? OR `rc_id`>?)
			ORDER BY `rc_timestamp`,`rc_id` LIMIT ?";
        let timeout = self.db_timeout;
        let pool = &self.wikidata_pool;
        let params = (
            &from.timestamp,
            &window_end,
            &from.timestamp,
            from.rc_id,
            self.max_recent_changes,
        );
        Self::with_timeout(timeout, "get_next_recent_changes_batch", async {
            let mut conn = pool.get_conn().await?;
            let results: Vec<RecentChanges> = conn
                .exec_iter(sql, params)
                .await?
                .map_and_drop(RecentChanges::from_row)
                .await?
                .into_iter()
                .flatten()
                .collect();
            Ok(results)
        })
        .await
    }

    pub(crate) fn sanitize_timestamp(ts: &str) -> Result<&str> {
        if ts.chars().all(|c| c.is_ascii_digit()) {
            Ok(ts)
        } else {
            Err(anyhow!("Invalid timestamp: {ts:?}"))
        }
    }

    pub fn make_id_numeric(id: &str) -> Result<ItemId> {
        if id.len() < 2 {
            return Err(anyhow!("Bad ID: {id:?}"));
        }
        let q = id[1..].parse::<ItemId>()?;
        if q == 0 {
            return Err(anyhow!("Bad ID: {id:?}"));
        }
        Ok(q)
    }

    /// Splits `rows` into statements of at most `ROWS_PER_INSERT` rows, so that a
    /// large batch cannot exceed the server's `max_allowed_packet`.
    fn chunked_statements(prefix: &str, rows: &[String]) -> Vec<String> {
        rows.chunks(ROWS_PER_INSERT)
            .map(|chunk| format!("{prefix} VALUES {}", chunk.join(",")))
            .collect()
    }

    /// Runs independent statements against the tool database, as many at a time
    /// as the connection pool allows. Returns the first error, if any.
    async fn exec_all(&self, label: &str, statements: &[String]) -> Result<()> {
        let timeout = self.db_timeout;
        let pool = &self.wdrc_pool;
        let queries = statements.iter().map(|sql| {
            Self::with_timeout(timeout, label, async {
                pool.get_conn().await?.exec_drop(sql.as_str(), ()).await?;
                Ok(())
            })
        });
        futures::stream::iter(queries)
            .buffer_unordered(self.wdrc_pool_max)
            .collect::<Vec<Result<()>>>()
            .await
            .into_iter()
            .collect()
    }

    async fn log_new_items(&self, rc: &RecentChangesResults) -> Result<()> {
        let mut creations = vec![];
        let mut item_ids = vec![];
        for new_item in rc.new_items() {
            let (Ok(q), Ok(ts)) = (
                Self::make_id_numeric(new_item.q()),
                Self::sanitize_timestamp(new_item.timestamp()),
            ) else {
                continue;
            };
            item_ids.push(q.to_string());
            creations.push(format!("({q},'{ts}')"));
        }
        if creations.is_empty() {
            return Ok(());
        }
        let mut statements = Self::chunked_statements(
            "REPLACE INTO `creations` (`q`,`timestamp`)",
            &creations,
        );
        // A recreated item is no longer deleted.
        statements.extend(item_ids.chunks(ROWS_PER_INSERT).map(|chunk| {
            format!(
                "DELETE FROM `deletions` WHERE `q` IN ({})",
                chunk.join(",")
            )
        }));
        self.exec_all("log_new_items", &statements).await
    }

    /// Fetches the revisions of a group of items in a single API request and
    /// compares each pair. Grouping is what keeps the request count low; the
    /// alternative, one request per item, also risked losing items whose
    /// revision span exceeded the API's default revision limit.
    async fn changes_for_items(api: &WikidataApi, items: &[&ChangedItem]) -> Result<Vec<Change>> {
        let revision_ids: Vec<RevisionId> = items
            .iter()
            .flat_map(|ci| [ci.rev_old(), ci.rev_new()])
            .collect::<BTreeSet<RevisionId>>()
            .into_iter()
            .collect();
        let revisions = match api.get_revisions(&revision_ids).await {
            Ok(revisions) => revisions,
            Err(first) => {
                tokio::time::sleep(API_RETRY_DELAY).await;
                api.get_revisions(&revision_ids)
                    .await
                    .map_err(|e| anyhow!("{first}; on retry: {e}"))?
            }
        };
        let mut changes = vec![];
        for ci in items {
            let compare = match RevisionCompare::new(ci) {
                Ok(compare) => compare,
                Err(e) => {
                    eprintln!("Skipping {}: {e}", ci.q());
                    continue;
                }
            };
            match (revisions.get(&ci.rev_old()), revisions.get(&ci.rev_new())) {
                (Some(old), Some(new)) => changes.append(&mut compare.compare_revisions(old, new)),
                _ => eprintln!(
                    "Skipping {}: revisions {} and {} not both available",
                    ci.q(),
                    ci.rev_old(),
                    ci.rev_new()
                ),
            }
        }
        Ok(changes)
    }

    async fn log_recent_changes(&mut self, rc: &RecentChangesResults) -> Result<()> {
        // Without a parent revision (imported or undeleted edits) there is
        // nothing to compare against.
        let items: Vec<&ChangedItem> = rc
            .changed_items()
            .iter()
            .filter(|ci| ci.rev_old() != 0 && ci.rev_new() != 0)
            .collect();
        if items.is_empty() {
            return Ok(());
        }
        let api = self.api.clone();
        let requests = items
            .chunks(ITEMS_PER_API_REQUEST)
            .map(|group| Self::changes_for_items(&api, group));
        let changes: Vec<Change> = futures::stream::iter(requests)
            .buffer_unordered(self.max_api_concurrent)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|result| {
                result
                    .map_err(|e| eprintln!("Revision group failed: {e}"))
                    .ok()
            })
            .flatten()
            .collect();
        self.log(format!(
            "CHANGES: {} from {} items",
            changes.len(),
            items.len()
        ));
        self.log_changes(&changes).await
    }

    /// Writes an auxiliary batch and advances the cursor it was read with.
    async fn store_aux_batch(
        &self,
        cursor_key: &str,
        table_and_columns: &str,
        batch: &AuxBatch,
    ) -> Result<()> {
        if !batch.updates.is_empty() {
            self.log(format!("{cursor_key}: {} changes", batch.updates.len()));
            let statements = Self::chunked_statements(
                &format!("REPLACE INTO {table_and_columns}"),
                &batch.updates,
            );
            self.exec_all(cursor_key, &statements).await?;
        }
        self.set_key_value(cursor_key, Self::sanitize_timestamp(&batch.cursor)?)
            .await
    }

    /// Returns `true` while more redirects are waiting to be read.
    async fn update_recent_redirects(&self, available: &ReplicaRange) -> Result<bool> {
        let batch = Self::with_timeout(
            self.db_timeout,
            "recent_redirects_batch",
            self.recent_redirects_batch(available),
        )
        .await?;
        self.store_aux_batch(
            META_REDIRECT,
            "`redirects` (`source`,`target`,`timestamp`)",
            &batch,
        )
        .await?;
        Ok(batch.pending)
    }

    async fn recent_redirects_batch(&self, available: &ReplicaRange) -> Result<AuxBatch> {
        // `recentchanges` only keeps a rolling window, so a cursor older than
        // its first row can never be satisfied and would otherwise crawl
        // forward one window at a time for years.
        let oldest = self
            .get_key_value(META_REDIRECT)
            .await?
            .unwrap_or_default()
            .max(available.oldest.clone());
        let window_end = Self::window_end(&oldest);
        let results = self.get_recent_redirects(&oldest, &window_end).await?;
        let truncated = results.len() >= AUX_QUERY_LIMIT;
        // A window that was read in full may be skipped past even when it held
        // nothing, but never past what the replica has: while replication lags,
        // rows for an apparently empty window can still arrive.
        let mut cursor = match truncated {
            true => oldest,
            false => window_end.clone().min(available.newest.clone()),
        };
        let mut updates = vec![];
        for result in &results {
            let (Ok(source), Ok(target), Ok(ts)) = (
                Self::make_id_numeric(result.source()),
                Self::make_id_numeric(result.target()),
                Self::sanitize_timestamp(result.timestamp()),
            ) else {
                continue;
            };
            if cursor.as_str() < ts {
                cursor = ts.to_string();
            }
            updates.push(format!("({source},{target},'{ts}')"));
        }
        Ok(AuxBatch {
            updates,
            pending: truncated || window_end < available.newest,
            cursor,
        })
    }

    async fn get_recent_redirects(
        &self,
        oldest: &str,
        window_end: &str,
    ) -> Result<Vec<RecentRedirects>> {
        let sql = "SELECT `rc_title` AS `source`,`rd_title` AS `target`,max(`rc_timestamp`) AS `timestamp` FROM `recentchanges`,`redirect`
			WHERE `rc_namespace`=0 AND `rd_from`=`rc_cur_id` AND `rd_namespace`=0 AND `rc_timestamp`>=? AND `rc_timestamp`<=? GROUP BY `source`,`target` ORDER BY `timestamp` LIMIT ?";
        let results: Vec<RecentRedirects> = self
            .wikidata_pool
            .get_conn()
            .await?
            .exec_iter(sql, (oldest, window_end, AUX_QUERY_LIMIT))
            .await?
            .map_and_drop(RecentRedirects::from_row)
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(results)
    }

    /// Returns `true` while more deletions are waiting to be read.
    async fn update_recent_deletions(&self) -> Result<bool> {
        let batch = Self::with_timeout(
            self.db_timeout,
            "recent_deletions_batch",
            self.recent_deletions_batch(),
        )
        .await?;
        self.store_aux_batch(META_DELETION, "`deletions` (`q`,`timestamp`)", &batch)
            .await?;
        Ok(batch.pending)
    }

    async fn recent_deletions_batch(&self) -> Result<AuxBatch> {
        // `logging` keeps its full history, so the cursor may start at the
        // beginning of time and work forward a batch at a time.
        let oldest = self
            .get_key_value(META_DELETION)
            .await?
            .unwrap_or_else(|| "20000101000000".to_string());
        let results = self.get_recent_deletions(&oldest).await?;
        let mut cursor = oldest;
        let mut updates = vec![];
        for result in &results {
            let (Ok(q), Ok(ts)) = (
                Self::make_id_numeric(result.q()),
                Self::sanitize_timestamp(result.timestamp()),
            ) else {
                continue;
            };
            if cursor.as_str() < ts {
                cursor = ts.to_string();
            }
            updates.push(format!("({q},'{ts}')"));
        }
        Ok(AuxBatch {
            pending: results.len() >= AUX_QUERY_LIMIT,
            updates,
            cursor,
        })
    }

    async fn get_recent_deletions(&self, oldest: &str) -> Result<Vec<RecentDeletions>> {
        let sql = "SELECT `log_title` AS `q`,`log_timestamp` AS `timestamp` FROM `logging` WHERE `log_type`='delete' AND `log_action`='delete' AND `log_timestamp`>=? AND `log_namespace`=0 ORDER BY `log_timestamp` LIMIT ?";
        let results: Vec<RecentDeletions> = self
            .wikidata_pool
            .get_conn()
            .await?
            .exec_iter(sql, (oldest, AUX_QUERY_LIMIT))
            .await?
            .map_and_drop(RecentDeletions::from_row)
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(results)
    }

    /// The span of `recentchanges` rows the replica currently holds, or `None`
    /// if it holds none at all.
    async fn recentchanges_range(&self) -> Result<Option<ReplicaRange>> {
        let sql = "SELECT MIN(`rc_timestamp`),MAX(`rc_timestamp`) FROM `recentchanges`";
        let timeout = self.db_timeout;
        let pool = &self.wikidata_pool;
        Self::with_timeout(timeout, "recentchanges_range", async {
            let mut conn = pool.get_conn().await?;
            let rows: Vec<(Option<String>, Option<String>)> = conn
                .exec_iter(sql, ())
                .await?
                .map_and_drop(from_row::<(Option<String>, Option<String>)>)
                .await?;
            let range = match rows.into_iter().next() {
                Some((Some(oldest), Some(newest))) => Some(ReplicaRange { oldest, newest }),
                _ => None,
            };
            Ok(range)
        })
        .await
    }

    fn build_statement_inserts(changes: &[Change]) -> Vec<String> {
        let rows: Vec<String> = changes
            .iter()
            .filter(|c| c.subject == ChangeSubject::Claims)
            .filter_map(|c| c.get_statement_log().ok())
            .collect();
        Self::chunked_statements(
            "INSERT IGNORE INTO `statements` (`item`,`revision`,`property`,`timestamp`,`change_type`)",
            &rows,
        )
    }

    /// Labels, descriptions, aliases and sitelinks all live in the `labels`
    /// table, keyed by the `texts` entry the change refers to.
    fn build_label_inserts(
        changes: &[Change],
        text_cache: &HashMap<String, TextId>,
    ) -> Vec<String> {
        let rows: Vec<String> = changes
            .iter()
            .filter_map(|c| {
                let text_id = text_cache.get(c.text_key()?)?;
                c.get_label_log(*text_id).ok()
            })
            .collect();
        Self::chunked_statements(
            "INSERT IGNORE INTO `labels` (`item`,`revision`,`type`,`timestamp`,`change_type`,`language`)",
            &rows,
        )
    }

    async fn log_changes(&mut self, changes: &[Change]) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        self.resolve_text_ids(changes).await?;
        let mut statements = Self::build_statement_inserts(changes);
        statements.append(&mut Self::build_label_inserts(changes, &self.text_cache));
        self.exec_all("log_changes", &statements).await
    }

    /// Makes sure every language and site name used by `changes` has a `texts`
    /// row, so that the rows written afterwards can all be resolved from cache.
    async fn resolve_text_ids(&mut self, changes: &[Change]) -> Result<()> {
        self.cache_texts_in_memory().await?;
        let missing: BTreeSet<String> = changes
            .iter()
            .filter_map(|c| c.text_key())
            .filter(|key| !key.is_empty() && !self.text_cache.contains_key(*key))
            .map(|key| key.to_string())
            .collect();
        for key in missing {
            // Changes for an unwritable text are dropped, not fatal for the batch.
            if let Err(e) = self.get_or_create_text_id(&key).await {
                eprintln!("Could not create text entry {key:?}: {e}");
            }
        }
        Ok(())
    }

    async fn get_or_create_text_id(&mut self, text: &str) -> Result<TextId> {
        self.cache_texts_in_memory().await?;
        if let Some(id) = self.text_cache.get(text) {
            return Ok(*id);
        }
        let id = self.insert_text(text).await?;
        self.text_cache.insert(text.to_string(), id);
        Ok(id)
    }

    /// `texts`.`value` is unique, so another process may have inserted the same
    /// value in the meantime; in that case read the existing row.
    async fn insert_text(&self, text: &str) -> Result<TextId> {
        let timeout = self.db_timeout;
        let pool = &self.wdrc_pool;
        Self::with_timeout(timeout, "insert_text", async {
            let mut conn = pool.get_conn().await?;
            conn.exec_drop("INSERT IGNORE INTO `texts` (`value`) VALUES (?)", (text,))
                .await
                .map_err(|e| anyhow!("Error inserting text: {e}"))?;
            match conn.last_insert_id() {
                Some(id) if id > 0 => Ok(id),
                _ => {
                    let existing: Vec<TextId> = conn
                        .exec_iter("SELECT `id` FROM `texts` WHERE `value`=?", (text,))
                        .await?
                        .map_and_drop(from_row::<TextId>)
                        .await?;
                    existing
                        .first()
                        .copied()
                        .ok_or_else(|| anyhow!("No text row for {text:?}"))
                }
            }
        })
        .await
    }

    async fn cache_texts_in_memory(&mut self) -> Result<()> {
        if !self.text_cache.is_empty() {
            return Ok(());
        }
        let sql = "SELECT `value`,`id` FROM `texts`";
        let timeout = self.db_timeout;
        let pool = &self.wdrc_pool;
        let result: Vec<(String, TextId)> =
            Self::with_timeout(timeout, "cache_texts_in_memory", async {
                let mut conn = pool.get_conn().await?;
                let result: Vec<(String, TextId)> = conn
                    .exec_iter(sql, ())
                    .await?
                    .map_and_drop(from_row::<(String, TextId)>)
                    .await?;
                Ok(result)
            })
            .await?;
        self.text_cache = result.into_iter().collect();
        Ok(())
    }

    async fn get_key_value(&self, key: &str) -> Result<Option<String>> {
        let sql = "SELECT value FROM `meta` WHERE `key`=?";
        let timeout = self.db_timeout;
        let pool = &self.wdrc_pool;
        Self::with_timeout(timeout, &format!("get_key_value({key})"), async {
            let mut conn = pool.get_conn().await?;
            let result: Vec<String> = conn
                .exec_iter(sql, (key,))
                .await?
                .map_and_drop(from_row::<String>)
                .await?;
            Ok(result.first().map(|s| s.to_string()))
        })
        .await
    }

    /// Upsert: a plain `UPDATE` silently matches no rows for a key that has no
    /// row yet, which would leave that cursor stuck at its fallback value.
    async fn set_key_value(&self, key: &str, value: &str) -> Result<()> {
        let sql = "INSERT INTO `meta` (`key`,`value`) VALUES (?,?) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)";
        let timeout = self.db_timeout;
        let pool = &self.wdrc_pool;
        Self::with_timeout(timeout, &format!("set_key_value({key})"), async {
            let mut conn = pool.get_conn().await?;
            conn.exec_drop(sql, (key, value)).await?;
            Ok(())
        })
        .await
    }

    fn read_config(config_file: &str) -> Result<Value> {
        let file = File::open(config_file)
            .map_err(|e| anyhow!("Reading {config_file} failed: {e}"))?;
        serde_json::from_reader(BufReader::new(file))
            .map_err(|e| anyhow!("Parsing {config_file} failed: {e}"))
    }

    /// Creates the connection pools for the Wikidata replica and the tool's own
    /// database, and reports the size of the latter.
    fn prepare_pools(config: &Value) -> Result<(Pool, Pool, usize)> {
        let config_wikidata = config
            .get("wikidata")
            .ok_or_else(|| anyhow!("Missing 'wikidata' config"))?;
        let config_wdrc = config
            .get("wdrc")
            .ok_or_else(|| anyhow!("Missing 'wdrc' config"))?;
        let my_cnf = Self::my_cnf_path(config);
        let wikidata_pool = Self::prepare_pool("wikidata", config_wikidata, || {
            Self::wikidata_url(config_wikidata, my_cnf.clone())
        })?;
        let wdrc_pool = Self::prepare_pool("wdrc", config_wdrc, || {
            Self::wdrc_url(config_wdrc, my_cnf.as_deref())
        })?;
        Ok((wikidata_pool, wdrc_pool, Self::pool_max(config_wdrc)))
    }

    /// The credentials file to read the database user and password from.
    /// `None` leaves the `toolforge` crate to look for `$HOME/replica.my.cnf`.
    fn my_cnf_path(config: &Value) -> Option<PathBuf> {
        if let Some(path) = config.get("replica_my_cnf").and_then(|v| v.as_str()) {
            return Some(PathBuf::from(path));
        }
        let default = PathBuf::from(REPLICA_MY_CNF);
        default.exists().then_some(default)
    }

    fn db_name<'a>(config: &'a Value, default: &'a str) -> &'a str {
        config
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or(default)
    }

    fn pool_max(config: &Value) -> usize {
        config
            .get("max_connections")
            .and_then(|v| v.as_u64())
            .map(|max_connections| max_connections as usize)
            .unwrap_or(DEFAULT_POOL_MAX)
    }

    /// `toolforge` only names the file it could not find, so say where we looked.
    fn my_cnf_error(error: impl std::fmt::Display, my_cnf: Option<&Path>) -> anyhow::Error {
        match my_cnf {
            Some(my_cnf) => anyhow!("{error} (tried {})", my_cnf.display()),
            None => anyhow!(
                "{error} (tried $HOME, as neither the 'replica_my_cnf' config key nor {REPLICA_MY_CNF} was usable)"
            ),
        }
    }

    fn wikidata_url(config: &Value, my_cnf: Option<PathBuf>) -> Result<String> {
        let db_name = Self::db_name(config, WIKIDATA_DB);
        let info = get_db_connection_info(db_name, Cluster::WEB, my_cnf.clone())
            .map_err(|e| Self::my_cnf_error(e, my_cnf.as_deref()))?;
        Ok(info.pool_max(Self::pool_max(config)).to_string())
    }

    fn wdrc_url(config: &Value, my_cnf: Option<&Path>) -> Result<String> {
        let database = Self::db_name(config, WDRC_DB).to_string();
        match my_cnf {
            // `toolsdb()` always reads `$HOME/replica.my.cnf` and offers no way to
            // point it elsewhere, so assemble the URL ourselves in that case.
            Some(my_cnf) => Self::toolsdb_url(my_cnf, &database, Self::pool_max(config)),
            None => {
                let info: DBConnectionInfo =
                    toolsdb(database).map_err(|e| Self::my_cnf_error(e, None))?;
                Ok(info.pool_max(Self::pool_max(config)).to_string())
            }
        }
    }

    /// Equivalent of `toolforge::db::toolsdb()` for an explicitly located
    /// `replica.my.cnf`. The query parameters follow the Toolforge connection
    /// handling policy, as the `toolforge` crate does.
    fn toolsdb_url(my_cnf: &Path, database: &str, pool_max: usize) -> Result<String> {
        let ini = Ini::load_from_file(my_cnf)
            .map_err(|e| anyhow!("Reading {} failed: {e}", my_cnf.display()))?;
        let client = ini
            .section(Some("client"))
            .ok_or_else(|| anyhow!("No [client] section in {}", my_cnf.display()))?;
        let value = |key: &str| {
            client
                .get(key)
                .ok_or_else(|| anyhow!("No '{key}' in {}", my_cnf.display()))
        };
        let user = value("user")?;
        let password = value("password")?;
        let host = match client.get("local") {
            Some(_) => TOOLSDB_HOST_LOCAL,
            None => TOOLSDB_HOST,
        };
        Ok(format!("mysql://{user}:{password}@{host}:3306/{database}?pool_min=0&pool_max={pool_max}&inactive_connection_ttl=1&ttl_check_interval=30"))
    }

    /// Creates a single connection pool.
    /// An explicit `url` in the config takes precedence, which is useful for local
    /// development through SSH tunnels. Otherwise the connection info is derived from
    /// `replica.my.cnf`, so no credentials are needed in the config file.
    fn prepare_pool(
        name: &str,
        config: &Value,
        url: impl FnOnce() -> Result<String>,
    ) -> Result<Pool> {
        let url = match config.get("url").and_then(|v| v.as_str()) {
            Some(url) => url.to_string(),
            None => url().map_err(|e| anyhow!("No {name} connection info: {e}"))?,
        };
        Pool::from_url(&url).map_err(|e| anyhow!("Creating {name} pool failed: {e}"))
    }

    pub async fn run_once(&mut self) -> Result<RunResult> {
        let run_timeout = self.run_timeout;
        Self::with_timeout(run_timeout, "run_once", self.run_once_inner()).await
    }

    async fn run_once_inner(&mut self) -> Result<RunResult> {
        let Some(available) = self.recentchanges_range().await? else {
            return Ok(RunResult::CaughtUp);
        };
        // Deletions and redirects may have a backlog of their own to work off.
        let mut more_work = self.update_aux_tables(&available).await;

        let from = self.get_rc_cursor().await?;
        let rc = self.get_recent_changes(&from).await?;
        if let Some(cursor) = rc.cursor() {
            self.log_recent_changes(&rc).await?;
            self.log_new_items(&rc).await?;
            // The cursor only moves once this batch is stored.
            self.set_rc_cursor(cursor).await?;
            // Keep going without sleeping while batches fill up, or while the
            // cursor is still far behind the present.
            more_work |= rc.row_count() >= self.max_recent_changes as usize
                || Self::is_behind(&cursor.timestamp);
        }
        Ok(match more_work {
            true => RunResult::MoreWork,
            false => RunResult::CaughtUp,
        })
    }

    /// Refreshes the deletion and redirect tables, at most once per
    /// `aux_interval` unless rows are still waiting, and returns whether they
    /// are. Errors are reported but do not fail the run: recent changes are the
    /// more time-critical part.
    async fn update_aux_tables(&mut self, available: &ReplicaRange) -> bool {
        if self
            .last_aux_run
            .is_some_and(|last| last.elapsed() < self.aux_interval)
        {
            return false;
        }
        let (deletions, redirects) = join!(
            self.update_recent_deletions(),
            self.update_recent_redirects(available)
        );
        for result in [&deletions, &redirects] {
            if let Err(e) = result {
                eprintln!("Auxiliary table update failed: {e}");
            }
        }
        // Work off a backlog batch by batch; an error waits for the interval,
        // so that a persistent failure is not retried in a tight loop.
        let pending = |result: &Result<bool>| matches!(result, Ok(true));
        let more_work = pending(&deletions) || pending(&redirects);
        if !more_work {
            self.last_aux_run = Some(Instant::now());
        }
        more_work
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes a `replica.my.cnf` to a unique temporary directory.
    fn write_my_cnf(name: &str, contents: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("wdrc_test_{name}"));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("replica.my.cnf");
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn test_toolsdb_url() {
        let path = write_my_cnf(
            "toolsdb_url",
            "[client]\nuser='u12345'\npassword='correcthorsebatterystaple'\n",
        );
        assert_eq!(
            WdRc::toolsdb_url(&path, "s55078__wdrc_p", 8).unwrap(),
            "mysql://u12345:correcthorsebatterystaple@tools.db.svc.wikimedia.cloud:3306/s55078__wdrc_p?pool_min=0&pool_max=8&inactive_connection_ttl=1&ttl_check_interval=30"
        );
    }

    #[test]
    fn test_toolsdb_url_local() {
        // `local` in the cnf means connections go through a local tunnel.
        let path = write_my_cnf(
            "toolsdb_url_local",
            "[client]\nuser='u12345'\npassword='pw'\nlocal='true'\n",
        );
        assert!(WdRc::toolsdb_url(&path, "db_p", 10)
            .unwrap()
            .contains("@tools.db.svc.local.wmftest.net:3306/db_p?"));
    }

    #[test]
    fn test_toolsdb_url_errors() {
        // A missing file, and a file without the fields we need, must not panic.
        assert!(WdRc::toolsdb_url(Path::new("/nonexistent/replica.my.cnf"), "db_p", 10).is_err());
        let path = write_my_cnf("toolsdb_url_errors", "[client]\nuser='u12345'\n");
        assert!(WdRc::toolsdb_url(&path, "db_p", 10).is_err());
    }

    #[test]
    fn test_my_cnf_path() {
        // An explicit path in the config wins, even if it does not exist.
        assert_eq!(
            WdRc::my_cnf_path(&json!({"replica_my_cnf": "/tmp/somewhere/replica.my.cnf"})),
            Some(PathBuf::from("/tmp/somewhere/replica.my.cnf"))
        );
        // Without config, fall back to the tool's path only if it exists,
        // otherwise let the `toolforge` crate look in `$HOME`.
        assert_eq!(
            WdRc::my_cnf_path(&json!({})),
            Path::new(REPLICA_MY_CNF)
                .exists()
                .then(|| PathBuf::from(REPLICA_MY_CNF))
        );
    }

    #[test]
    fn test_pool_max() {
        assert_eq!(WdRc::pool_max(&json!({"max_connections": 8})), 8);
        assert_eq!(WdRc::pool_max(&json!({})), DEFAULT_POOL_MAX);
    }

    #[tokio::test]
    #[ignore = "requires network access to www.wikidata.org"]
    async fn test_changes_for_items() {
        // Both items are covered by a single API request: an alias was added to
        // Q42, and a statement was added to Q64, between the given revisions.
        let items = [
            ChangedItem::new_for_test("Q42", 2208025531, 2208025540, "20240101000000"),
            ChangedItem::new_for_test("Q64", 2520718920, 2522551666, "20240102000000"),
        ];
        let refs: Vec<&ChangedItem> = items.iter().collect();
        let api = WikidataApi::new(1).unwrap();
        let changes = WdRc::changes_for_items(&api, &refs).await.unwrap();

        let alias = changes
            .iter()
            .find(|c| c.subject == ChangeSubject::Aliases)
            .expect("alias change");
        assert_eq!((alias.item_id, alias.language.as_str()), (42, "ak"));
        assert_eq!(alias.revision_id, 2208025540);
        assert_eq!(alias.timestamp, "20240101000000");

        let claim = changes
            .iter()
            .find(|c| c.subject == ChangeSubject::Claims)
            .expect("claim change");
        assert_eq!((claim.item_id, claim.property.as_str()), (64, "P14470"));
        assert_eq!(claim.revision_id, 2522551666);
    }

    #[tokio::test]
    #[ignore = "requires a local config.json and live access to the wdrc database"]
    async fn test_get_or_create_text_id() {
        let mut wdrc = WdRc::new("config.json").unwrap();
        let text = "aawikibooks";
        let id = wdrc.get_or_create_text_id(text).await.unwrap();
        assert_eq!(id, 1252);
    }

    #[test]
    fn test_sanitize_timestamp() {
        assert!(WdRc::sanitize_timestamp("20231231235959").is_ok());
        assert!(WdRc::sanitize_timestamp("").is_ok()); // empty is technically valid
        assert!(WdRc::sanitize_timestamp("2023-12-31").is_err());
        assert!(WdRc::sanitize_timestamp("'; DROP TABLE--").is_err());
    }

    #[test]
    fn test_chunked_statements() {
        let rows: Vec<String> = (0..ROWS_PER_INSERT + 1).map(|i| format!("({i})")).collect();
        let statements = WdRc::chunked_statements("INSERT INTO `t` (`a`)", &rows);
        // One statement per ROWS_PER_INSERT rows, so no single statement can
        // grow past the server's packet limit.
        assert_eq!(statements.len(), 2);
        assert!(statements[0].starts_with("INSERT INTO `t` (`a`) VALUES (0),(1),"));
        assert_eq!(
            statements[1],
            format!("INSERT INTO `t` (`a`) VALUES ({})", ROWS_PER_INSERT)
        );
        assert!(WdRc::chunked_statements("INSERT INTO `t` (`a`)", &[]).is_empty());
    }

    #[test]
    fn test_window_end() {
        assert_eq!(WdRc::window_end("20240101120000"), "20240101130000");
        // An unparsable cursor must not bound the query to the past.
        assert_eq!(WdRc::window_end(""), NO_WINDOW_END);
        assert_eq!(WdRc::window_end("not a timestamp"), NO_WINDOW_END);
    }

    #[test]
    fn test_is_behind() {
        assert!(WdRc::is_behind("20200101000000"));
        assert!(!WdRc::is_behind(&TimeStamp::now()));
        // A future or unparsable timestamp is not "behind".
        assert!(!WdRc::is_behind(NO_WINDOW_END));
    }

    /// Changes of every subject, for the SQL builders below.
    fn all_subject_changes() -> Vec<Change> {
        vec![
            Change {
                subject: ChangeSubject::Claims,
                property: "P31".to_string(),
                item_id: 1,
                revision_id: 10,
                timestamp: "20240101000000".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Labels,
                language: "en".to_string(),
                item_id: 2,
                revision_id: 20,
                timestamp: "20240101000000".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Sitelinks,
                site: "enwiki".to_string(),
                item_id: 3,
                revision_id: 30,
                timestamp: "20240101000000".to_string(),
                ..Default::default()
            },
        ]
    }

    #[test]
    fn test_build_statement_inserts() {
        let statements = WdRc::build_statement_inserts(&all_subject_changes());
        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("INTO `statements`"));
        // Only the claim change, and its property is stored numerically.
        assert!(statements[0].ends_with("VALUES (1,10,31,'20240101000000','changed')"));
    }

    #[test]
    fn test_build_label_inserts() {
        let changes = all_subject_changes();
        let text_cache: HashMap<String, TextId> =
            [("en".to_string(), 7), ("enwiki".to_string(), 8)].into();
        let statements = WdRc::build_label_inserts(&changes, &text_cache);
        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("INTO `labels`"));
        // Labels and sitelinks share the table; the claim change is not in it.
        assert!(statements[0].contains("(2,20,'labels','20240101000000','changed',7)"));
        assert!(statements[0].contains("(3,30,'sitelinks','20240101000000','changed',8)"));
        assert!(!statements[0].contains("(1,10"));
    }

    #[test]
    fn test_build_label_inserts_skips_unknown_texts() {
        // Without a `texts` id there is nothing to reference, so the row is dropped.
        let statements = WdRc::build_label_inserts(&all_subject_changes(), &HashMap::new());
        assert!(statements.is_empty());
    }

    #[test]
    fn test_make_id_numeric() {
        // Empty string should return Err, not panic
        assert!(WdRc::make_id_numeric("").is_err());

        // Single char "Q" -> no numeric part, should return Err
        assert!(WdRc::make_id_numeric("Q").is_err());

        // "Q0" -> zero is explicitly rejected
        assert!(WdRc::make_id_numeric("Q0").is_err());

        // Valid Q-prefixed ID
        assert_eq!(WdRc::make_id_numeric("Q42").unwrap(), 42);

        // Valid P-prefixed ID
        assert_eq!(WdRc::make_id_numeric("P123").unwrap(), 123);
    }
}

