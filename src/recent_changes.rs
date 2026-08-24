use std::collections::HashMap;

use mysql_async::Row;

use crate::{revision_compare::RevisionId, WdRc};

/// Position in `recentchanges`. Rows are read in `(rc_timestamp, rc_id)` order,
/// so both parts are needed to resume exactly after the last row read: a
/// timestamp alone would re-read (and re-store) every row of its second.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct RcCursor {
    pub timestamp: String,
    pub rc_id: u64,
}

pub struct RecentChanges {
    pub rc_id: u64,
    pub rc_timestamp: String,
    pub rc_title: String,
    /// Derived from `rc_source`.
    pub rc_new: bool,
    pub rc_this_oldid: u64,
    pub rc_last_oldid: u64,
}

impl RecentChanges {
    pub fn from_row(row: Row) -> Option<RecentChanges> {
        let rc_source: String = row.get("rc_source")?;
        Some(RecentChanges {
            rc_id: row.get("rc_id")?,
            rc_timestamp: row.get("rc_timestamp")?,
            rc_title: row.get("rc_title")?,
            rc_new: rc_source == "mw.new",
            rc_this_oldid: row.get("rc_this_oldid")?,
            rc_last_oldid: row.get("rc_last_oldid")?,
        })
    }

    fn cursor(&self) -> RcCursor {
        RcCursor {
            timestamp: self.rc_timestamp.clone(),
            rc_id: self.rc_id,
        }
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

    #[cfg(test)]
    pub fn new_for_test(q: &str, old: RevisionId, new: RevisionId, timestamp: &str) -> Self {
        Self {
            q: q.to_string(),
            old,
            new,
            timestamp: timestamp.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct RecentChangesResults {
    new_items: Vec<NewItem>,
    changed_items: Vec<ChangedItem>,
    /// Position of the last row read, which is where the next batch resumes.
    cursor: Option<RcCursor>,
    /// Rows read from `recentchanges`, before collapsing them per item.
    /// Compared against the batch size to tell whether more work is waiting.
    row_count: usize,
}

impl RecentChangesResults {
    pub fn new(results: &[RecentChanges]) -> Self {
        let mut new_items: HashMap<String, NewItem> = HashMap::new();
        let mut changed_items: HashMap<String, ChangedItem> = HashMap::new();
        let mut cursor: Option<RcCursor> = None;
        for result in results {
            // The cursor tracks every row, including any this tool ignores, so
            // that no row is ever read twice.
            let row_cursor = result.cursor();
            if cursor.as_ref().is_none_or(|c| *c < row_cursor) {
                cursor = Some(row_cursor);
            }
            if WdRc::make_id_numeric(&result.rc_title).is_err() {
                continue;
            }
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
            cursor,
            row_count: results.len(),
        }
    }

    /// Where the next batch should resume, or `None` if this batch was empty.
    /// Never fall back to a fixed position here: that would rewind the cursor.
    pub fn cursor(&self) -> Option<&RcCursor> {
        self.cursor.as_ref()
    }

    pub fn row_count(&self) -> usize {
        self.row_count
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

    /// `rc_id` defaults to a value derived from the revision, so that rows in a
    /// test are distinct without every case having to spell it out.
    fn make_rc(title: &str, rc_new: bool, last_oldid: u64, this_oldid: u64) -> RecentChanges {
        RecentChanges {
            rc_id: this_oldid,
            rc_timestamp: "20240101000000".to_string(),
            rc_title: title.to_string(),
            rc_new,
            rc_this_oldid: this_oldid,
            rc_last_oldid: last_oldid,
        }
    }

    fn cursor_of(rcr: &RecentChangesResults) -> Option<(&str, u64)> {
        rcr.cursor().map(|c| (c.timestamp.as_str(), c.rc_id))
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
        let results = vec![
            make_rc("Q42", false, 100, 101),
            make_rc("Q42", false, 101, 102),
        ];
        let rcr = RecentChangesResults::new(&results);
        assert_eq!(rcr.changed_items().len(), 1);
        let ci = &rcr.changed_items()[0];
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
    fn test_cursor_is_the_last_row() {
        let results = vec![
            make_rc("Q1", false, 100, 101),
            make_rc("Q2", false, 200, 201),
        ];
        let rcr = RecentChangesResults::new(&results);
        assert_eq!(cursor_of(&rcr), Some(("20240101000000", 201)));
        assert_eq!(rcr.row_count(), 2);
    }

    #[test]
    fn test_cursor_empty() {
        let rcr = RecentChangesResults::new(&[]);
        // No cursor means the caller must leave the stored one where it is.
        assert_eq!(cursor_of(&rcr), None);
        assert_eq!(rcr.row_count(), 0);
    }

    #[test]
    fn test_cursor_tracks_ignored_rows() {
        // A row this tool has no use for still moves the cursor, or it would be
        // read again on every pass.
        let results = vec![
            make_rc("Q1", false, 100, 101),
            make_rc("Not_an_item", false, 200, 201),
        ];
        let rcr = RecentChangesResults::new(&results);
        assert_eq!(rcr.changed_items().len(), 1);
        assert_eq!(rcr.row_count(), 2);
        assert_eq!(cursor_of(&rcr), Some(("20240101000000", 201)));
    }

    #[test]
    fn test_cursor_picks_max_across_new_and_changed() {
        let mut rc_new = make_rc("Q99", true, 0, 50);
        rc_new.rc_timestamp = "20240701000000".to_string();
        let mut rc_changed = make_rc("Q1", false, 100, 101);
        rc_changed.rc_timestamp = "20240601000000".to_string();
        let results = vec![rc_changed, rc_new];
        let rcr = RecentChangesResults::new(&results);
        assert_eq!(cursor_of(&rcr), Some(("20240701000000", 50)));
    }

    #[test]
    fn test_cursor_prefers_timestamp_over_rc_id() {
        // A later timestamp wins even when its row has the lower `rc_id`.
        let mut older = make_rc("Q1", false, 100, 101);
        older.rc_id = 900;
        let mut newer = make_rc("Q2", false, 200, 201);
        newer.rc_timestamp = "20240601000000".to_string();
        newer.rc_id = 10;
        let rcr = RecentChangesResults::new(&[older, newer]);
        assert_eq!(cursor_of(&rcr), Some(("20240601000000", 10)));
    }

    #[test]
    fn test_row_count_counts_rows_not_items() {
        // Three rows for the same item collapse into one changed item,
        // but the row count must still reflect the rows read.
        let results = vec![
            make_rc("Q1", false, 100, 101),
            make_rc("Q1", false, 101, 102),
            make_rc("Q1", false, 102, 103),
        ];
        let rcr = RecentChangesResults::new(&results);
        assert_eq!(rcr.changed_items().len(), 1);
        assert_eq!(rcr.row_count(), 3);
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
        assert_eq!(rcr.new_items()[0].q(), "Q99");
        assert_eq!(rcr.new_items()[0].timestamp(), "20240601000000");
    }
}
