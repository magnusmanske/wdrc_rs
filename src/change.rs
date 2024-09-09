use crate::{revision_compare::RevisionId, ItemId, TextId, WdRc};
use anyhow::{anyhow, Result};
use wikimisc::{
    mysql_async::{prelude::*, Conn},
    timestamp::TimeStamp,
};

#[derive(Debug, Default, Clone, PartialEq)]
pub enum ChangeSubject {
    #[default]
    Labels,
    Descriptions,
    Sitelinks,
    Aliases,
    Claims,
}

impl ChangeSubject {
    pub fn as_str(&self) -> &str {
        match self {
            ChangeSubject::Labels => "labels",
            ChangeSubject::Descriptions => "descriptions",
            ChangeSubject::Aliases => "aliases",
            ChangeSubject::Claims => "claims",
            ChangeSubject::Sitelinks => "sitelinks",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub enum ChangeType {
    #[default]
    Changed,
    Removed,
    Added,
}

impl ChangeType {
    pub fn as_str(&self) -> &str {
        match self {
            ChangeType::Changed => "changed",
            ChangeType::Removed => "removed",
            ChangeType::Added => "added",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Change {
    pub subject: ChangeSubject,
    pub change_type: ChangeType,
    pub language: String,
    pub text: String,
    pub site: String,
    pub title: String,
    pub property: String, // TODO numeric?
    pub id: String,
    pub item_id: ItemId,
    pub revision_id: RevisionId,
}

impl Change {
    pub async fn log_statement_change(&self, conn: &mut Conn) -> Result<()> {
        let property = WdRc::make_id_numeric(&self.property)?;
        let timestamp = TimeStamp::now();
        let sql = "INSERT IGNORE INTO `statements` (`item`,`revision`,`property`,`timestamp`,`change_type`) VALUES (?,?,?,?,?)";
        conn.exec_drop(
            sql,
            (
                self.item_id,
                self.revision_id,
                property,
                timestamp,
                self.change_type.as_str(),
            ),
        )
        .await
        .map_err(|e| anyhow!("Error logging change: {}", e))?;
        Ok(())
    }

    /// This logs labels, descriptions, aliases, and sitelinks
    pub async fn log_label_change(&self, text_id: TextId, conn: &mut Conn) -> Result<()> {
        let timestamp = TimeStamp::now();
        let sql = "INSERT IGNORE INTO `labels` (`item`,`revision`,`type`,`timestamp`,`change_type`,`language`) VALUES (?,?,?,?,?,?)";
        conn.exec_drop(
            sql,
            (
                self.item_id,
                self.revision_id,
                self.subject.as_str(),
                timestamp,
                self.change_type.as_str(),
                text_id,
            ),
        )
        .await
        .map_err(|e| anyhow!("Error logging change: {}", e))?;
        Ok(())
    }
}
