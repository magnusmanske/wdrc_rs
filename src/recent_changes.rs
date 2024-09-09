use wikimisc::mysql_async::Row;

use crate::{ItemId, WdRc};

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
