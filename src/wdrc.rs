use crate::{
    change::{Change, ChangeSubject},
    recent_changes::{RecentChanges, RecentChangesResults, RecentDeletions, RecentRedirects},
    revision_compare::RevisionCompare,
};
use anyhow::{anyhow, Result};
use futures::{join, StreamExt};
use ini::Ini;
use mysql_async::{from_row, prelude::Queryable, Pool};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use toolforge::db::{get_db_connection_info, toolsdb, Cluster, DBConnectionInfo};
use wikimisc::{timestamp::TimeStamp, wikidata::Wikidata};

pub type TextId = u64;
pub type ItemId = u64;

/// Returned from `run_once` to indicate whether the bot should sleep or immediately process more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunResult {
    /// The batch was full — there is likely more work waiting. Don't sleep.
    MoreWork,
    /// The batch was not full — we've caught up. Sleep before next iteration.
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

const MAX_RECENT_CHANGES: u64 = 500;
const MAX_API_CONCURRENT: u64 = 50;
/// Default timeout for individual DB queries (seconds)
const DEFAULT_DB_TIMEOUT_SEC: u64 = 300;
/// Default timeout for entire run_once cycle (seconds)
const DEFAULT_RUN_TIMEOUT_SEC: u64 = 600;
/// Default sleep between bot loop iterations (seconds)
const DEFAULT_BOT_SLEEP_SEC: u64 = 10;

#[derive(Debug)]
pub struct WdRc {
    text_cache: HashMap<String, TextId>,
    wd: Arc<Wikidata>,
    wikidata_pool: Pool,
    wdrc_pool: Pool,
    logging: bool,
    max_recent_changes: u64,
    max_api_concurrent: usize,
    db_timeout: Duration,
    run_timeout: Duration,
    bot_sleep: Duration,
}

impl WdRc {
    pub fn new(config_file: &str) -> WdRc {
        let config = Self::read_config(config_file);
        let db_timeout_sec = config
            .get("db_timeout_sec")
            .and_then(|j| j.as_u64())
            .unwrap_or(DEFAULT_DB_TIMEOUT_SEC);
        let run_timeout_sec = config
            .get("run_timeout_sec")
            .and_then(|j| j.as_u64())
            .unwrap_or(DEFAULT_RUN_TIMEOUT_SEC);
        let bot_sleep_sec = config
            .get("bot_sleep_sec")
            .and_then(|j| j.as_u64())
            .unwrap_or(DEFAULT_BOT_SLEEP_SEC);
        let (wikidata_pool, wdrc_pool) = Self::prepare_pools(&config);
        WdRc {
            text_cache: HashMap::new(),
            wd: Self::prepare_wd(),
            wikidata_pool,
            wdrc_pool,
            logging: config
                .get("logging")
                .unwrap_or(&json!(false))
                .as_bool()
                .unwrap_or(false),
            max_recent_changes: config
                .get("max_recent_changes")
                .and_then(|j| j.as_u64())
                .unwrap_or(MAX_RECENT_CHANGES),
            max_api_concurrent: config
                .get("max_api_concurrent")
                .and_then(|j| j.as_u64())
                .unwrap_or(MAX_API_CONCURRENT) as usize,
            db_timeout: Duration::from_secs(db_timeout_sec),
            run_timeout: Duration::from_secs(run_timeout_sec),
            bot_sleep: Duration::from_secs(bot_sleep_sec),
        }
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

    pub async fn get_recent_changes(&self) -> Result<RecentChangesResults> {
        let oldest = self.get_key_value("timestamp").await?.unwrap_or_default();
        let results = self.get_next_recent_changes_batch(&oldest).await?;
        let rc = RecentChangesResults::new(&results);
        self.log(format!(
            "New: {}, changed:{}",
            rc.new_items().len(),
            rc.changed_items().len()
        ));

        // Determine and set new oldest timestamp
        Ok(rc)
    }

    async fn get_next_recent_changes_batch(&self, oldest: &str) -> Result<Vec<RecentChanges>> {
        let upper_limit = TimeStamp::str2utc(oldest)
            .map(|dt| dt + Duration::from_secs(60 * 60))
            .map(|dt| TimeStamp::datetime(&dt))
            .unwrap_or("99991231235900".to_string());
        let sql = "SELECT `rc_source`,`rc_timestamp`,`rc_title`,`rc_this_oldid`,`rc_last_oldid` FROM `recentchanges` WHERE `rc_namespace`=0 AND `rc_timestamp`>=? AND rc_timestamp<=? ORDER BY `rc_timestamp`,`rc_title`,`rc_id` LIMIT ?";
        let timeout = self.db_timeout;
        let pool = &self.wikidata_pool;
        let max_rc = &self.max_recent_changes;
        Self::with_timeout(timeout, "get_next_recent_changes_batch", async {
            let mut conn = pool.get_conn().await?;
            let results: Vec<RecentChanges> = conn
                .exec_iter(sql, (oldest, &upper_limit, max_rc))
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

    pub async fn log_new_items(&self, rc: &RecentChangesResults) -> Result<()> {
        if rc.new_items().is_empty() {
            return Ok(());
        }
        let mut updates = vec![];
        let mut delete_from_deleted = vec![];
        for new_item in rc.new_items() {
            let q = Self::make_id_numeric(new_item.q())?;
            let ts = new_item.timestamp();
            if Self::sanitize_timestamp(ts).is_err() {
                continue;
            }
            delete_from_deleted.push(format!("{q}"));
            updates.push(format!("({q},'{ts}')"));
        }
        let updates = updates.join(",");
        let delete_from_deleted = delete_from_deleted.join(",");

        // Write changes to DB in parallel (different tables, no ordering dependency)
        let timeout = self.db_timeout;
        let pool = &self.wdrc_pool;
        let create_sql = format!("REPLACE INTO `creations` (`q`,`timestamp`) VALUES {updates}");
        let delete_sql = format!("DELETE FROM `deletions` WHERE `q` IN ({delete_from_deleted})");

        let create_fut = Self::with_timeout(timeout, "log_new_items/creations", async {
            pool.get_conn().await?.exec_drop(&create_sql, ()).await?;
            Ok(())
        });
        let delete_fut = Self::with_timeout(timeout, "log_new_items/deletions", async {
            pool.get_conn().await?.exec_drop(&delete_sql, ()).await?;
            Ok(())
        });
        let (r1, r2) = join!(create_fut, delete_fut);
        r1?;
        r2?;
        Ok(())
    }

    pub async fn log_recent_changes(&mut self, rc: &RecentChangesResults) -> Result<()> {
        if rc.changed_items().is_empty() {
            return Ok(());
        }
        let wd = self.wd.clone();
        let futures = rc.changed_items().iter().map(|ci| {
            let mut revision_compare = RevisionCompare::new(wd.clone());
            async move { revision_compare.run(ci).await }
        });
        let stream = futures::stream::iter(futures).buffer_unordered(self.max_api_concurrent);
        let changes: Vec<Change> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|r| r.ok())
            .flatten()
            .collect();
        self.log(format!("CHANGES: {}", changes.len()));

        self.log_changes(&changes).await?;
        let new_oldest = rc.get_last_rc_timetamp("20000101000000");
        let _ = self.set_key_value("timestamp", &new_oldest).await;
        Ok(())
    }

    pub async fn update_recent_redirects(&self) -> Result<()> {
        let (updates, new_ts) = Self::with_timeout(
            self.db_timeout,
            "update_recent_redirects_get_updates",
            self.update_recent_redirects_get_updates(),
        )
        .await?;
        if updates.is_empty() {
            return Ok(());
        }
        self.log(format!("REDIRECTS: {} changes", updates.len()));

        let updates = updates.join(",");
        let sql =
            format!("REPLACE INTO `redirects` (`source`,`target`,`timestamp`) VALUES {updates}");
        let timeout = self.db_timeout;
        let pool = &self.wdrc_pool;
        Self::with_timeout(timeout, "update_recent_redirects write", async {
            pool.get_conn().await?.exec_drop(&sql, ()).await?;
            Ok(())
        })
        .await?;
        self.set_key_value("timestamp_redirect", &new_ts).await?;
        Ok(())
    }

    async fn update_recent_redirects_get_updates(&self) -> Result<(Vec<String>, String)> {
        let oldest = self
            .get_key_value("timestamp_redirect")
            .await?
            .unwrap_or_else(|| "20000101000000".to_string());
        let results = self.get_recent_redirects(&oldest).await?;
        let mut updates = vec![];
        let mut new_ts = oldest;
        for result in &results {
            let source = match Self::make_id_numeric(result.source()) {
                Ok(q) => q,
                Err(_) => continue,
            };
            let target = match Self::make_id_numeric(result.target()) {
                Ok(q) => q,
                Err(_) => continue,
            };
            let ts = result.timestamp().to_string();
            if new_ts < ts {
                new_ts = ts;
            }
            if Self::sanitize_timestamp(result.timestamp()).is_err() {
                continue;
            }
            updates.push(format!("({source},{target},'{}')", result.timestamp()));
        }
        Ok((updates, new_ts))
    }

    async fn get_recent_redirects(&self, oldest: &str) -> Result<Vec<RecentRedirects>> {
        let sql = "SELECT `rc_title` AS `source`,`rd_title` AS `target`,max(`rc_timestamp`) AS `timestamp` FROM `recentchanges`,`redirect`
			WHERE `rc_namespace`=0 AND `rd_from`=`rc_cur_id` AND `rd_namespace`=0 AND `rc_timestamp`>=? GROUP BY `source`,`target` ORDER BY `timestamp` LIMIT 5000";
        let results: Vec<RecentRedirects> = self
            .wikidata_pool
            .get_conn()
            .await?
            .exec_iter(sql, (oldest,))
            .await?
            .map_and_drop(RecentRedirects::from_row)
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(results)
    }

    pub async fn update_recent_deletions(&self) -> Result<()> {
        let (updates, new_ts) = Self::with_timeout(
            self.db_timeout,
            "update_recent_deletions_get_updates",
            self.update_recent_deletions_get_updates(),
        )
        .await?;
        if updates.is_empty() {
            return Ok(());
        }
        self.log(format!("DELETIONS: {} changes", updates.len()));

        let updates = updates.join(",");
        let sql = format!("REPLACE INTO `deletions` (`q`,`timestamp`) VALUES {updates}");
        let timeout = self.db_timeout;
        let pool = &self.wdrc_pool;
        Self::with_timeout(timeout, "update_recent_deletions write", async {
            pool.get_conn().await?.exec_drop(&sql, ()).await?;
            Ok(())
        })
        .await?;
        self.set_key_value("timestamp_deletion", &new_ts).await?;
        Ok(())
    }

    async fn update_recent_deletions_get_updates(&self) -> Result<(Vec<String>, String)> {
        let oldest = self
            .get_key_value("timestamp_deletion")
            .await?
            .unwrap_or_else(|| "20000101000000".to_string());
        let results = self.get_recent_deletions(&oldest).await?;
        let mut updates = vec![];
        let mut new_ts = oldest;
        for result in &results {
            let q = match Self::make_id_numeric(result.q()) {
                Ok(q) => q,
                Err(_) => continue,
            };
            let ts = result.timestamp().to_string();
            if new_ts < ts {
                new_ts = ts;
            }
            if Self::sanitize_timestamp(result.timestamp()).is_err() {
                continue;
            }
            updates.push(format!("({q},'{}')", result.timestamp()));
        }
        Ok((updates, new_ts))
    }

    async fn get_recent_deletions(&self, oldest: &str) -> Result<Vec<RecentDeletions>> {
        let sql = "SELECT `log_title` AS `q`,`log_timestamp` AS `timestamp` FROM `logging` WHERE `log_type`='delete' AND `log_action`='delete' AND `log_timestamp`>=? AND `log_namespace`=0 ORDER BY `log_timestamp` LIMIT 5000";
        let results: Vec<RecentDeletions> = self
            .wikidata_pool
            .get_conn()
            .await?
            .exec_iter(sql, (oldest,))
            .await?
            .map_and_drop(RecentDeletions::from_row)
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(results)
    }

    fn build_statement_sql(changes: &[Change]) -> Option<String> {
        let values = changes
            .iter()
            .filter(|c| c.subject == ChangeSubject::Claims)
            .filter_map(|c| c.get_statement_log().ok())
            .collect::<Vec<String>>();
        if values.is_empty() {
            return None;
        }
        Some(format!("INSERT IGNORE INTO `statements` (`item`,`revision`,`property`,`timestamp`,`change_type`) VALUES {}", values.join(",")))
    }

    fn build_labels_sql(
        changes: &[Change],
        text_cache: &HashMap<String, TextId>,
        key_field: impl Fn(&Change) -> &str,
        filter: impl Fn(&Change) -> bool,
    ) -> Option<String> {
        let parts: Vec<String> = changes
            .iter()
            .filter(|c| filter(c))
            .filter_map(|ci| {
                let key = key_field(ci);
                let text_id = text_cache.get(key)?;
                ci.get_label_log(*text_id).ok()
            })
            .collect();
        if parts.is_empty() {
            return None;
        }
        Some(format!(
            "INSERT IGNORE INTO `labels` (`item`,`revision`,`type`,`timestamp`,`change_type`,`language`) VALUES {}",
            parts.join(",")
        ))
    }

    async fn log_changes(&mut self, changes: &[Change]) -> Result<()> {
        // Phase 1: Ensure all text IDs are resolved (sequential, needs &mut self)
        self.cache_texts_in_memory().await?;
        for ci in changes {
            match ci.subject {
                ChangeSubject::Sitelinks => {
                    let _ = self.get_or_create_text_id(&ci.site).await;
                }
                ChangeSubject::Labels | ChangeSubject::Descriptions | ChangeSubject::Aliases => {
                    let _ = self.get_or_create_text_id(&ci.language).await;
                }
                _ => {}
            }
        }

        // Phase 2: Build all SQL statements (pure computation, no I/O)
        let stmt_sql = Self::build_statement_sql(changes);
        let sitelinks_sql = Self::build_labels_sql(
            changes,
            &self.text_cache,
            |ci| &ci.site,
            |c| c.subject == ChangeSubject::Sitelinks,
        );
        let labels_sql = Self::build_labels_sql(
            changes,
            &self.text_cache,
            |ci| &ci.language,
            |c| {
                c.subject == ChangeSubject::Labels
                    || c.subject == ChangeSubject::Descriptions
                    || c.subject == ChangeSubject::Aliases
            },
        );

        // Phase 3: Execute all DB writes in parallel
        let timeout = self.db_timeout;
        let pool = &self.wdrc_pool;

        let stmt_fut = async {
            if let Some(sql) = &stmt_sql {
                Self::with_timeout(timeout, "log_statement_changes", async {
                    pool.get_conn().await?.exec_drop(sql.as_str(), ()).await?;
                    Ok(())
                })
                .await
            } else {
                Ok(())
            }
        };
        let sitelinks_fut = async {
            if let Some(sql) = &sitelinks_sql {
                Self::with_timeout(timeout, "log_sitelinks_changes", async {
                    pool.get_conn().await?.exec_drop(sql.as_str(), ()).await?;
                    Ok(())
                })
                .await
            } else {
                Ok(())
            }
        };
        let labels_fut = async {
            if let Some(sql) = &labels_sql {
                Self::with_timeout(timeout, "log_label_changes", async {
                    pool.get_conn().await?.exec_drop(sql.as_str(), ()).await?;
                    Ok(())
                })
                .await
            } else {
                Ok(())
            }
        };

        let (r1, r2, r3) = join!(stmt_fut, sitelinks_fut, labels_fut);
        r1?;
        r2?;
        r3?;
        Ok(())
    }

    async fn get_or_create_text_id(&mut self, text: &str) -> Result<TextId> {
        self.cache_texts_in_memory().await?;
        match self.text_cache.get(text) {
            Some(id) => Ok(*id),
            None => {
                let sql = "INSERT INTO `texts` (`value`) VALUES (?)";
                let timeout = self.db_timeout;
                let pool = &self.wdrc_pool;
                let id = Self::with_timeout(timeout, "get_or_create_text_id", async {
                    let mut conn = pool.get_conn().await?;
                    conn.exec_drop(sql, (text,))
                        .await
                        .map_err(|e| anyhow!("Error inserting text: {}", e))?;
                    let id = conn
                        .last_insert_id()
                        .ok_or_else(|| anyhow!("No text row inserted"))?;
                    Ok(id)
                })
                .await?;
                self.text_cache.insert(text.to_string(), id);
                Ok(id)
            }
        }
    }

    async fn cache_texts_in_memory(&mut self) -> Result<()> {
        if self.text_cache.is_empty() {
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
        }
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

    async fn set_key_value(&self, key: &str, value: &str) -> Result<()> {
        let sql = "UPDATE `meta` SET `value`=? WHERE `key`=?";
        let timeout = self.db_timeout;
        let pool = &self.wdrc_pool;
        Self::with_timeout(timeout, &format!("set_key_value({key})"), async {
            let mut conn = pool.get_conn().await?;
            conn.exec_drop(sql, (value, key)).await?;
            Ok(())
        })
        .await
    }

    fn read_config(config_file: &str) -> Value {
        let file =
            File::open(config_file).unwrap_or_else(|e| panic!("Reading {config_file} failed: {e}"));
        let reader = BufReader::new(file);
        serde_json::from_reader(reader)
            .unwrap_or_else(|e| panic!("Parsing {config_file} failed: {e}"))
    }

    fn prepare_wd() -> Arc<Wikidata> {
        let mut wd = Wikidata::new();
        wd.set_user_agent("wdrc-rs/0.1.0");
        Arc::new(wd)
    }

    /// Creates the connection pools for the Wikidata replica and the tool's own database.
    fn prepare_pools(config: &Value) -> (Pool, Pool) {
        let config_wikidata = config.get("wikidata").expect("Missing wikidata config");
        let config_wdrc = config.get("wdrc").expect("Missing wdrc config");
        let my_cnf = Self::my_cnf_path(config);
        let wikidata_pool = Self::prepare_pool("wikidata", config_wikidata, || {
            Self::wikidata_url(config_wikidata, my_cnf.clone())
        });
        let wdrc_pool = Self::prepare_pool("wdrc", config_wdrc, || {
            Self::wdrc_url(config_wdrc, my_cnf.as_deref())
        });
        (wikidata_pool, wdrc_pool)
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
    fn prepare_pool(name: &str, config: &Value, url: impl FnOnce() -> Result<String>) -> Pool {
        let url = match config.get("url").and_then(|v| v.as_str()) {
            Some(url) => url.to_string(),
            None => url().unwrap_or_else(|e| panic!("No {name} connection info: {e}")),
        };
        Pool::from_url(&url).unwrap_or_else(|e| panic!("Creating {name} pool failed: {e}"))
    }

    pub async fn run_once(&mut self) -> Result<RunResult> {
        let run_timeout = self.run_timeout;
        Self::with_timeout(run_timeout, "run_once", self.run_once_inner()).await
    }

    async fn run_once_inner(&mut self) -> Result<RunResult> {
        let future1 = self.update_recent_deletions();
        let future2 = self.update_recent_redirects();
        let (r1, r2) = join!(future1, future2);
        if let Err(e) = r1 {
            eprintln!("update_recent_deletions error: {e}");
        }
        if let Err(e) = r2 {
            eprintln!("update_recent_redirects error: {e}");
        }

        let rc = self.get_recent_changes().await?;
        let batch_full =
            rc.changed_items().len() + rc.new_items().len() >= self.max_recent_changes as usize;

        self.log_recent_changes(&rc).await?;
        self.log_new_items(&rc).await?;

        // self.purge_old_entries().await?;
        Ok(if batch_full {
            RunResult::MoreWork
        } else {
            RunResult::CaughtUp
        })
    }

    // pub async fn purge_old_entries(&self) -> Result<()> {
    //     todo!()
    // }
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
    #[ignore = "requires a local config.json and live access to the wdrc database"]
    async fn test_get_or_create_text_id() {
        let mut wdrc = WdRc::new("config.json");
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
