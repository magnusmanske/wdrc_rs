use std::collections::HashMap;

use wikimisc::mysql_async::Row;

use crate::{revision_compare::RevisionId, ItemId, WdRc};

pub struct RecentChanges {
    item_id: ItemId,
    // rc_id: u64,
    pub rc_timestamp: String,
    // pub rc_actor: u64,
    // pub rc_namespace: u64,
    pub rc_title: String,
    // pub rc_comment_id: String,
    // pub rc_minor: bool,
    // pub rc_bot: bool,
    pub rc_new: bool,
    // pub rc_cur_id: u64,
    pub rc_this_oldid: u64,
    pub rc_last_oldid: u64,
    // pub rc_type: u64,
    // pub rc_source: String,
    // pub rc_patrolled: bool,
    // pub rc_ip: Option<String>,
    // pub rc_old_len: Option<u64>,
    // pub rc_new_len: Option<u64>,
    // pub rc_deleted: u64,
    // pub rc_logid: u64,
    // pub rc_log_type: Option<String>,
    // pub rc_log_action: Option<String>,
    // pub rc_params: Option<String>,
}

impl RecentChanges {
    pub fn from_row(row: Row) -> Option<RecentChanges> {
        let mut ret = RecentChanges {
            item_id: 0,
            // rc_id: row.get("rc_id")?,
            rc_timestamp: row.get("rc_timestamp")?,
            // rc_actor: row.get("rc_actor")?,
            // rc_namespace: row.get("rc_namespace")?,
            rc_title: row.get("rc_title")?,
            // rc_comment_id: row.get("rc_comment_id")?,
            // rc_minor: row.get("rc_minor")?,
            // rc_bot: row.get("rc_bot")?,
            rc_new: row.get("rc_new")?,
            // rc_cur_id: row.get("rc_cur_id")?,
            rc_this_oldid: row.get("rc_this_oldid")?,
            rc_last_oldid: row.get("rc_last_oldid")?,
            // rc_type: row.get("rc_type")?,
            // rc_source: row.get("rc_source")?,
            // rc_patrolled: row.get("rc_patrolled")?,
            // rc_ip: row.get("rc_ip"),
            // rc_old_len: row.get("rc_old_len"),
            // rc_new_len: row.get("rc_new_len"),
            // rc_deleted: row.get("rc_deleted")?,
            // rc_logid: row.get("rc_logid")?,
            // rc_log_type: row.get("rc_log_type"),
            // rc_log_action: row.get("rc_log_action"),
            // rc_params: row.get("rc_params"),
        };
        ret.item_id = WdRc::make_id_numeric(&ret.rc_title).ok()?;
        Some(ret)
    }
}

#[derive(Debug)]
pub struct NewItem {
    q: String,
    timestamp: String,
}

impl NewItem {
    pub fn q(&self) -> &str {
        &self.q
    }

    pub fn timestamp(&self) -> &str {
        &self.timestamp
    }
}

#[derive(Debug)]
pub struct ChangedItem {
    q: String,
    old: RevisionId,
    new: RevisionId,
    timestamp: String,
}

impl ChangedItem {
    pub fn q(&self) -> &str {
        &self.q
    }

    pub fn rev_old(&self) -> RevisionId {
        self.old
    }

    pub fn rev_new(&self) -> RevisionId {
        self.new
    }

    pub fn timestamp(&self) -> &str {
        &self.timestamp
    }
}

#[derive(Debug)]
pub struct RecentChangesResults {
    new_items: Vec<NewItem>,
    changed_items: Vec<ChangedItem>,
}

impl RecentChangesResults {
    pub fn new(results: &Vec<RecentChanges>) -> Self {
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
    pub fn get_last_rc_timetamp(&self, oldest: &str) -> String {
        match self.changed_items.iter().map(|r| &r.timestamp).max() {
            Some(t) => t.to_owned(),
            None => oldest.to_string(),
        }
    }

    pub fn new_items(&self) -> &Vec<NewItem> {
        &self.new_items
    }

    pub fn changed_items(&self) -> &Vec<ChangedItem> {
        &self.changed_items
    }
}

#[derive(Clone, Debug)]
pub struct RecentRedirects {
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

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    pub fn timestamp(&self) -> &str {
        &self.timestamp
    }
}

#[derive(Clone, Debug)]
pub struct RecentDeletions {
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

    pub fn q(&self) -> &str {
        &self.q
    }

    pub fn timestamp(&self) -> &str {
        &self.timestamp
    }
}
