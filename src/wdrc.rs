use crate::{
    change::{Change, ChangeSubject},
    recent_changes::{RecentChanges, RecentChangesResults, RecentDeletions, RecentRedirects},
    revision_compare::RevisionCompare,
};
use anyhow::{anyhow, Result};
use futures::{join, StreamExt};
use serde_json::{json, Value};
use std::{collections::HashMap, fs::File, io::BufReader, sync::Arc, time::Duration};
use wikimisc::{
    mysql_async::{from_row, prelude::Queryable},
    timestamp::TimeStamp,
    toolforge_db::ToolforgeDB,
    wikidata::Wikidata,
};

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
    db: ToolforgeDB,
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
        WdRc {
            text_cache: HashMap::new(),
            wd: Self::prepare_wd(),
            db: Self::prepare_db(&config),
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
        let db = &self.db;
        let max_rc = &self.max_recent_changes;
        Self::with_timeout(timeout, "get_next_recent_changes_batch", async {
            let mut conn = db.get_connection("wikidata").await?;
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

        // Write changes to DB
        let timeout = self.db_timeout;
        let db = &self.db;
        Self::with_timeout(timeout, "log_new_items", async {
            let mut conn = db.get_connection("wdrc").await?;

            let sql = format!("REPLACE INTO `creations` (`q`,`timestamp`) VALUES {updates}");
            conn.exec_drop(&sql, ()).await?;

            let sql = format!("DELETE FROM `deletions` WHERE `q` IN  ({delete_from_deleted})");
            conn.exec_drop(&sql, ()).await?;
            Ok(())
        })
        .await
    }

    pub async fn log_recent_changes(&mut self, rc: &RecentChangesResults) -> Result<()> {
        if rc.changed_items().is_empty() {
            return Ok(());
        }
        let mut rcs = vec![];
        for _ci in rc.changed_items() {
            let revision_compare = RevisionCompare::new(self.wd.clone());
            rcs.push(revision_compare);
        }

        let mut futures = vec![];
        for (ci, revision_compare) in rc.changed_items().iter().zip(rcs.iter_mut()) {
            let future = revision_compare.run(ci);
            futures.push(future);
        }
        let stream = futures::stream::iter(futures).buffer_unordered(self.max_api_concurrent);
        let changes = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|r| r.ok())
            .flatten()
            .collect::<Vec<_>>();
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
        let db = &self.db;
        Self::with_timeout(timeout, "update_recent_redirects write", async {
            db.get_connection("wdrc").await?.exec_drop(&sql, ()).await?;
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
            .db
            .get_connection("wikidata")
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
        let db = &self.db;
        Self::with_timeout(timeout, "update_recent_deletions write", async {
            db.get_connection("wdrc").await?.exec_drop(&sql, ()).await?;
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
            .db
            .get_connection("wikidata")
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
        let db = &self.db;

        let stmt_fut = async {
            if let Some(sql) = &stmt_sql {
                Self::with_timeout(timeout, "log_statement_changes", async {
                    db.get_connection("wdrc")
                        .await?
                        .exec_drop(sql.as_str(), ())
                        .await?;
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
                    db.get_connection("wdrc")
                        .await?
                        .exec_drop(sql.as_str(), ())
                        .await?;
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
                    db.get_connection("wdrc")
                        .await?
                        .exec_drop(sql.as_str(), ())
                        .await?;
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
                let db = &self.db;
                let id = Self::with_timeout(timeout, "get_or_create_text_id", async {
                    let mut conn = db.get_connection("wdrc").await?;
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
            let db = &self.db;
            let result: Vec<(String, TextId)> =
                Self::with_timeout(timeout, "cache_texts_in_memory", async {
                    let mut conn = db.get_connection("wdrc").await?;
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
        let db = &self.db;
        Self::with_timeout(timeout, &format!("get_key_value({key})"), async {
            let mut conn = db.get_connection("wdrc").await?;
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
        let db = &self.db;
        Self::with_timeout(timeout, &format!("set_key_value({key})"), async {
            let mut conn = db.get_connection("wdrc").await?;
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

    fn prepare_db(config: &Value) -> ToolforgeDB {
        let mut db = ToolforgeDB::default();
        let config_wikidata = config.get("wikidata").expect("Missing wikidata config");
        let config_wdrc = config.get("wdrc").expect("Missing wdrc config");
        db.add_mysql_pool("wikidata", config_wikidata)
            .expect("Adding wikidata pool failed");
        db.add_mysql_pool("wdrc", config_wdrc)
            .expect("Adding wdrc pool failed");
        db
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

    #[tokio::test]
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
