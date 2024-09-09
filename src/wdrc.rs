use crate::{
    change::{Change, ChangeSubject},
    recent_changes::RecentChanges,
    revision_compare::{RevisionCompare, RevisionId},
};
use anyhow::{anyhow, Result};
use futures::{join, StreamExt};
use serde_json::{json, Value};
use std::{collections::HashMap, fs::File, io::BufReader, sync::Arc, time::Duration};
use wikimisc::{
    mysql_async::{from_row, prelude::Queryable, Row},
    timestamp::TimeStamp,
    toolforge_db::ToolforgeDB,
    wikidata::Wikidata,
};

pub type TextId = u64;
pub type ItemId = u64;

const MAX_RECENT_CHANGES: u64 = 500;
const MAX_API_CONCURRENT: usize = 50;

#[derive(Clone, Debug)]
struct RecentRedirects {
    source: String,
    target: String,
    timestamp: String,
}

impl RecentRedirects {
    pub fn from_row(row: Row) -> Option<Self> {
        Some(Self {
            source: row.get("source")?,
            target: row.get("target")?,
            timestamp: row.get("timestamp")?,
        })
    }
}

#[derive(Clone, Debug)]
struct RecentDeletions {
    q: String,
    timestamp: String,
}

impl RecentDeletions {
    pub fn from_row(row: Row) -> Option<Self> {
        Some(Self {
            q: row.get("q")?,
            timestamp: row.get("timestamp")?,
        })
    }
}

#[derive(Debug)]
pub struct NewItem {
    pub q: String,
    pub timestamp: String,
}

#[derive(Debug)]
pub struct ChangedItem {
    pub q: String,
    pub old: RevisionId,
    pub new: RevisionId,
    pub timestamp: String,
}

#[derive(Debug)]
pub struct RecentChangesResults {
    new_items: Vec<NewItem>,
    changed_items: Vec<ChangedItem>,
}

impl RecentChangesResults {
    fn new(results: &Vec<RecentChanges>) -> Self {
        let mut new_items: HashMap<String, NewItem> = HashMap::new();
        let mut changed_items: HashMap<String, ChangedItem> = HashMap::new();
        for result in results {
            let q = result.rc_title.clone();
            let timestamp = result.rc_timestamp.clone();
            if result.rc_new {
                new_items.insert(q.clone(), NewItem { q, timestamp });
            } else {
                let old = result.rc_last_oldid;
                let new = result.rc_this_oldid;
                match changed_items.get_mut(&q) {
                    Some(ci) => {
                        if ci.new < new {
                            ci.new = new;
                        }
                    }
                    None => {
                        changed_items.insert(
                            q.clone(),
                            ChangedItem {
                                q,
                                timestamp,
                                new,
                                old,
                            },
                        );
                    }
                }
            }
        }
        Self {
            new_items: new_items.into_values().collect(),
            changed_items: changed_items.into_values().collect(),
        }
    }

    /// Returns the last timestamp of the changed items, or the given oldest timestamp as fallback.
    fn get_last_rc_timetamp(&self, oldest: &str) -> String {
        match self.changed_items.iter().map(|r| &r.timestamp).max() {
            Some(t) => t.to_owned(),
            None => oldest.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct WdRc {
    text_cache: HashMap<String, usize>,
    wd: Arc<Wikidata>,
    db: ToolforgeDB,
    logging: bool,
    max_recent_changes: u64,
}

impl WdRc {
    pub fn new(config_file: &str) -> WdRc {
        let config = Self::read_config(config_file);
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
        }
    }

    fn log(&self, msg: String) {
        if self.logging {
            println!("{}", msg);
        }
    }

    pub async fn get_recent_changes(&self) -> Result<RecentChangesResults> {
        let oldest = self.get_key_value("timestamp").await?.unwrap_or_default();
        let results = self.get_next_recent_changes_batch(&oldest).await?;
        let rc = RecentChangesResults::new(&results);
        self.log(format!(
            "New: {}, changed:{}",
            rc.new_items.len(),
            rc.changed_items.len()
        ));

        // Determine and set new oldest timestamp
        Ok(rc)
    }

    async fn get_next_recent_changes_batch(&self, oldest: &String) -> Result<Vec<RecentChanges>> {
        let upper_limit = TimeStamp::from_str(oldest)
            .map(|dt| dt + Duration::from_secs(60 * 60))
            .map(|dt| TimeStamp::datetime(&dt))
            .unwrap_or("99991231235900".to_string());
        let sql = "SELECT * FROM `recentchanges` WHERE `rc_namespace`=0 AND `rc_timestamp`>=? AND rc_timestamp<=? ORDER BY `rc_timestamp`,`rc_title`,`rc_id` LIMIT ?";
        let mut conn = self.db.get_connection("wikidata").await?;
        let results: Vec<RecentChanges> = conn
            .exec_iter(sql, (oldest, &upper_limit, &self.max_recent_changes))
            .await?
            .map_and_drop(RecentChanges::from_row)
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(results)
    }

    pub fn make_id_numeric(id: &str) -> Result<ItemId> {
        let q = &id[1..];
        let q = q.parse::<ItemId>()?;
        if q == 0 {
            return Err(anyhow!("Bad ID: {id:?}"));
        }
        Ok(q)
    }

    pub async fn log_new_items(&self, rc: &RecentChangesResults) -> Result<()> {
        if rc.new_items.is_empty() {
            return Ok(());
        }
        let mut updates = vec![];
        let mut delete_from_deleted = vec![];
        for new_item in &rc.new_items {
            let q = Self::make_id_numeric(&new_item.q)?;
            delete_from_deleted.push(format!("{q}"));
            updates.push(format!("({q},'{}')", new_item.timestamp));
        }
        let updates = updates.join(",");
        let delete_from_deleted = delete_from_deleted.join(",");

        // Write changes to DB
        let mut conn = self.db.get_connection("wdrc").await?;

        let sql = format!("REPLACE INTO `creations` (`q`,`timestamp`) VALUES {updates}");
        conn.exec_drop(&sql, ()).await?;

        let sql = format!("DELETE FROM `deletions` WHERE `q` IN  ({delete_from_deleted})");
        conn.exec_drop(&sql, ()).await?;

        Ok(())
    }

    pub async fn log_recent_changes(&mut self, rc: &RecentChangesResults) -> Result<()> {
        if rc.changed_items.is_empty() {
            return Ok(());
        }
        let mut rcs = vec![];
        for _ci in &rc.changed_items {
            let revision_compare = RevisionCompare::new(self.wd.clone());
            rcs.push(revision_compare);
        }

        let mut futures = vec![];
        for (ci, revision_compare) in rc.changed_items.iter().zip(rcs.iter_mut()) {
            let future = revision_compare.run(&ci.q, ci.old, ci.new, &ci.timestamp);
            futures.push(future);
        }
        let stream = futures::stream::iter(futures).buffer_unordered(MAX_API_CONCURRENT);
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
        let oldest = self
            .get_key_value("timestamp_redirect")
            .await?
            .unwrap_or_else(|| "20000101000000".to_string());

        let results = self.get_recent_redirects(&oldest).await?;

        let mut updates = vec![];
        let mut new_ts = &oldest;
        for result in &results {
            let source = match Self::make_id_numeric(&result.source) {
                Ok(q) => q,
                Err(_) => continue,
            };
            let target = match Self::make_id_numeric(&result.target) {
                Ok(q) => q,
                Err(_) => continue,
            };
            if *new_ts < result.timestamp {
                new_ts = &result.timestamp;
            }
            updates.push(format!("({source},{target},'{}')", result.timestamp));
        }
        if updates.is_empty() {
            return Ok(());
        }
        self.log(format!("REDIRECTS: {} changes", updates.len()));

        let updates = updates.join(",");
        let sql =
            format!("REPLACE INTO `redirects` (`source`,`target`,`timestamp`) VALUES {updates}");
        self.db
            .get_connection("wdrc")
            .await?
            .exec_drop(&sql, ())
            .await?;
        self.set_key_value("timestamp_redirect", new_ts).await?;
        Ok(())
    }

    async fn get_recent_redirects(&self, oldest: &String) -> Result<Vec<RecentRedirects>> {
        let sql = "SELECT `rc_title` AS `source`,`rd_title` AS `target`,max(`rc_timestamp`) AS `timestamp` FROM `recentchanges`,`redirect`
			WHERE `rc_namespace`=0 AND `rd_from`=`rc_cur_id` AND `rd_namespace`=0 AND `rc_timestamp`>=? GROUP BY `source`,`target`";
        let results: Vec<RecentRedirects> = self
            .db
            .get_connection("wikidata")
            .await?
            .exec_iter(sql, (oldest,))
            .await?
            .map_and_drop(RecentRedirects::from_row)
            .await?
            .iter()
            .filter_map(|r| r.to_owned())
            .collect();
        Ok(results)
    }

    pub async fn update_recent_deletions(&self) -> Result<()> {
        let oldest = self
            .get_key_value("timestamp_deletion")
            .await?
            .unwrap_or_else(|| "20000101000000".to_string());

        let results = self.get_recent_deletions(&oldest).await?;

        let mut updates = vec![];
        let mut new_ts = &oldest;
        for result in &results {
            let q = match Self::make_id_numeric(&result.q) {
                Ok(q) => q,
                Err(_) => continue,
            };
            if *new_ts < result.timestamp {
                new_ts = &result.timestamp;
            }
            updates.push(format!("({q},'{}')", result.timestamp));
        }
        if updates.is_empty() {
            return Ok(());
        }
        self.log(format!("DELETIONS: {} changes", updates.len()));

        let updates = updates.join(",");
        let sql = format!("REPLACE INTO `deletions` (`q`,`timestamp`) VALUES {updates}");
        self.db
            .get_connection("wdrc")
            .await?
            .exec_drop(&sql, ())
            .await?;
        self.set_key_value("timestamp_deletion", new_ts).await?;
        Ok(())
    }

    async fn get_recent_deletions(&self, oldest: &String) -> Result<Vec<RecentDeletions>> {
        let sql = "SELECT `log_title` AS `q`,`log_timestamp` AS `timestamp` FROM `logging` WHERE `log_type`='delete' AND `log_action`='delete' AND `log_timestamp`>=? AND `log_namespace`=0";
        let results: Vec<RecentDeletions> = self
            .db
            .get_connection("wikidata")
            .await?
            .exec_iter(sql, (oldest,))
            .await?
            .map_and_drop(RecentDeletions::from_row)
            .await?
            .iter()
            .filter_map(|r| r.to_owned())
            .collect();
        Ok(results)
    }

    async fn log_statement_changes(&self, changes: &[Change]) -> Result<()> {
        let values = changes
            .iter()
            .filter(|c| c.subject == ChangeSubject::Claims)
            .filter_map(|c| c.get_statement_log().ok())
            .collect::<Vec<String>>();
        if !values.is_empty() {
            let sql = format!("INSERT IGNORE INTO `statements` (`item`,`revision`,`property`,`timestamp`,`change_type`) VALUES {}",values.join(",")) ;
            self.db
                .get_connection("wdrc")
                .await?
                .exec_drop(&sql, ())
                .await?;
        }
        Ok(())
    }

    async fn log_sitelinks_changes(&mut self, changes: &[Change]) -> Result<()> {
        let changes: Vec<&Change> = changes
            .iter()
            .filter(|c| c.subject == ChangeSubject::Sitelinks)
            .collect();
        let mut parts = vec![];
        for ci in changes {
            let text_id = match self.get_or_create_text_id(&ci.site).await {
                Ok(text_id) => text_id,
                Err(_) => continue,
            };
            let part = ci.get_label_log(text_id);
            parts.push(part);
        }
        if !parts.is_empty() {
            let sql = format!(
				"INSERT IGNORE INTO `labels` (`item`,`revision`,`type`,`timestamp`,`change_type`,`language`) VALUES {}",
				parts.join(",")
			);
            self.db
                .get_connection("wdrc")
                .await?
                .exec_drop(&sql, ())
                .await?;
        }
        Ok(())
    }

    async fn log_label_changes(&mut self, changes: &[Change]) -> Result<()> {
        let changes: Vec<&Change> = changes
            .iter()
            .filter(|c| {
                c.subject == ChangeSubject::Labels
                    || c.subject == ChangeSubject::Descriptions
                    || c.subject == ChangeSubject::Aliases
            })
            .collect();
        let mut parts = vec![];
        for ci in changes {
            let text_id = match self.get_or_create_text_id(&ci.language).await {
                Ok(text_id) => text_id,
                Err(_) => continue,
            };
            let part = ci.get_label_log(text_id);
            parts.push(part);
        }
        if !parts.is_empty() {
            let sql = format!(
				"INSERT IGNORE INTO `labels` (`item`,`revision`,`type`,`timestamp`,`change_type`,`language`) VALUES {}",
				parts.join(",")
			);
            self.db
                .get_connection("wdrc")
                .await?
                .exec_drop(&sql, ())
                .await?;
        }
        Ok(())
    }

    async fn log_changes(&mut self, changes: &[Change]) -> Result<()> {
        self.log_statement_changes(changes).await?;
        self.log_sitelinks_changes(changes).await?;
        self.log_label_changes(changes).await?;
        Ok(())
    }

    async fn get_or_create_text_id(&mut self, text: &str) -> Result<TextId> {
        self.chache_texts_in_memory().await?;
        match self.text_cache.get(text) {
            Some(id) => Ok(*id as TextId),
            None => {
                let sql = "INSERT INTO `texts` (`value`) VALUES (?)";
                let mut conn = self.db.get_connection("wdrc").await?;
                conn.exec_drop(sql, (text,))
                    .await
                    .map_err(|e| anyhow!("Error inserting text: {}", e))?;
                let id = conn
                    .last_insert_id()
                    .ok_or_else(|| anyhow!("No text row inserted"))?;
                self.text_cache.insert(text.to_string(), id as usize);
                Ok(id)
            }
        }
    }

    async fn chache_texts_in_memory(&mut self) -> Result<()> {
        if self.text_cache.is_empty() {
            let sql = "SELECT `value`,`id` FROM `texts`";
            let mut conn = self.db.get_connection("wdrc").await?;
            let result: Vec<(String, usize)> = conn
                .exec_iter(sql, ())
                .await?
                .map_and_drop(from_row::<(String, usize)>)
                .await?;
            self.text_cache = result.into_iter().collect();
        }
        Ok(())
    }

    async fn get_key_value(&self, key: &str) -> Result<Option<String>> {
        let sql = "SELECT value FROM `meta` WHERE `key`=?";
        let mut conn = self.db.get_connection("wdrc").await?;
        let result: Vec<String> = conn
            .exec_iter(sql, (key,))
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        Ok(result.first().map(|s| s.to_string()))
    }

    async fn set_key_value(&self, key: &str, value: &str) -> Result<()> {
        let sql = "UPDATE `meta` SET `value`=? WHERE `key`=?";
        let mut conn = self.db.get_connection("wdrc").await?;
        conn.exec_drop(sql, (value, key)).await?;
        Ok(())
    }

    fn read_config(config_file: &str) -> Value {
        let file = File::open(config_file).expect("Reading {config_file} failed");
        let reader = BufReader::new(file);
        serde_json::from_reader(reader).expect("Parsing {config_file} failed")
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

    pub async fn run_once(&mut self) -> Result<()> {
        let future1 = self.update_recent_deletions();
        let future2 = self.update_recent_redirects();
        let _ = join!(future1, future2); // Ignore errors

        let rc = self.get_recent_changes().await?;
        self.log_recent_changes(&rc).await?;

        self.log_new_items(&rc).await?;

        // self.purge_old_entries().await?;
        Ok(())
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
}
