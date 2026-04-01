use std::collections::HashMap;

use wikimisc::mysql_async::Row;

use crate::{revision_compare::RevisionId, ItemId, WdRc};

pub struct RecentChanges {
    pub(crate) item_id: ItemId,
    // rc_id: u64,
    pub rc_timestamp: String,
    // pub rc_actor: u64,
    // pub rc_namespace: u64,
    pub rc_title: String,
    // pub rc_comment_id: String,
    // pub rc_minor: bool,
    // pub rc_bot: bool,
    pub rc_new: bool, // Derived from rc_source
    // pub rc_cur_id: u64,
    pub rc_this_oldid: u64,
    pub rc_last_oldid: u64,
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
        let rc_source: String = row.get("rc_source")?;
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
            rc_new: rc_source == "mw.new",
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
    pub fn new(results: &[RecentChanges]) -> Self {
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
                        if ci.old > old {
                            ci.old = old;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_rc(title: &str, rc_new: bool, last_oldid: u64, this_oldid: u64) -> RecentChanges {
        RecentChanges {
            item_id: 0,
            rc_timestamp: "20240101000000".to_string(),
            rc_title: title.to_string(),
            rc_new,
            rc_this_oldid: this_oldid,
            rc_last_oldid: last_oldid,
        }
    }

    #[test]
    fn test_changed_items_tracks_minimum_old_revision() {
        // Results arrive in reverse order: newer change first, older change second.
        // rev 101->102 arrives before rev 100->101.
        let results = vec![
            make_rc("Q42", false, 101, 102),
            make_rc("Q42", false, 100, 101),
        ];
        let rcr = RecentChangesResults::new(&results);
        assert_eq!(rcr.changed_items().len(), 1);
        let ci = &rcr.changed_items()[0];
        assert_eq!(ci.q(), "Q42");
        assert_eq!(ci.rev_old(), 100, "old should be the minimum old revision");
        assert_eq!(ci.rev_new(), 102, "new should be the maximum new revision");
    }

    #[test]
    fn test_changed_items_in_order() {
        // Results arrive in natural order: older change first.
        let results = vec![
            make_rc("Q42", false, 100, 101),
            make_rc("Q42", false, 101, 102),
        ];
        let rcr = RecentChangesResults::new(&results);
        assert_eq!(rcr.changed_items().len(), 1);
        let ci = &rcr.changed_items()[0];
        assert_eq!(ci.q(), "Q42");
        assert_eq!(ci.rev_old(), 100);
        assert_eq!(ci.rev_new(), 102);
    }

    #[test]
    fn test_new_items_are_tracked() {
        let results = vec![make_rc("Q99", true, 0, 50)];
        let rcr = RecentChangesResults::new(&results);
        assert_eq!(rcr.new_items().len(), 1);
        assert_eq!(rcr.new_items()[0].q(), "Q99");
        assert!(rcr.changed_items().is_empty());
    }

    #[test]
    fn test_get_last_rc_timetamp_with_items() {
        let results = vec![
            make_rc("Q1", false, 100, 101),
            make_rc("Q2", false, 200, 201),
        ];
        let rcr = RecentChangesResults::new(&results);
        // Should return max timestamp (they're all the same "20240101000000" from make_rc)
        let ts = rcr.get_last_rc_timetamp("19990101000000");
        assert_eq!(ts, "20240101000000");
    }

    #[test]
    fn test_get_last_rc_timetamp_empty() {
        let rcr = RecentChangesResults::new(&[]);
        let ts = rcr.get_last_rc_timetamp("19990101000000");
        assert_eq!(ts, "19990101000000"); // Falls back to oldest
    }

    #[test]
    fn test_get_last_rc_timetamp_only_new_items() {
        let results = vec![make_rc("Q99", true, 0, 50)];
        let rcr = RecentChangesResults::new(&results);
        // Only new items, no changed items, should return fallback
        let ts = rcr.get_last_rc_timetamp("19990101000000");
        assert_eq!(ts, "19990101000000");
    }

    #[test]
    fn test_multiple_items_different_timestamps() {
        let mut rc1 = make_rc("Q1", false, 100, 101);
        rc1.rc_timestamp = "20240101000000".to_string();
        let mut rc2 = make_rc("Q2", false, 200, 201);
        rc2.rc_timestamp = "20240601000000".to_string();
        let results = vec![rc1, rc2];
        let rcr = RecentChangesResults::new(&results);
        let ts = rcr.get_last_rc_timetamp("19990101000000");
        assert_eq!(ts, "20240601000000");
    }

    #[test]
    fn test_duplicate_new_items_last_wins() {
        let mut rc1 = make_rc("Q99", true, 0, 50);
        rc1.rc_timestamp = "20240101000000".to_string();
        let mut rc2 = make_rc("Q99", true, 0, 51);
        rc2.rc_timestamp = "20240601000000".to_string();
        let results = vec![rc1, rc2];
        let rcr = RecentChangesResults::new(&results);
        assert_eq!(rcr.new_items().len(), 1);
        // The HashMap insert means last one wins
        assert_eq!(rcr.new_items()[0].q(), "Q99");
    }
}
