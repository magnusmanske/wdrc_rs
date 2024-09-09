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
    pub property: String,
    pub id: String,
    pub item_id: ItemId,
    pub revision_id: RevisionId,
    pub timestamp: String,
}

impl Change {
    pub fn get_statement_log(&self) -> Result<String> {
        let property = WdRc::make_id_numeric(&self.property)?;
        Ok(format!(
            "({},{},{property},'{}','{}')",
            self.item_id,
            self.revision_id,
            self.timestamp,
            self.change_type.as_str()
        ))
    }

    pub fn get_label_log(&self, text_id: TextId) -> String {
        format!(
            "({},{},'{}','{}','{}',{})",
            self.item_id,
            self.revision_id,
            self.subject.as_str(),
            self.timestamp,
            self.change_type.as_str(),
            text_id
        )
    }
}
