use crate::{revision_compare::RevisionId, ItemId, TextId, WdRc};
use anyhow::Result;

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
        if !self.timestamp.chars().all(|c| c.is_ascii_digit()) {
            return Err(anyhow::anyhow!("Invalid timestamp: {:?}", self.timestamp));
        }
        Ok(format!(
            "({},{},{property},'{}','{}')",
            self.item_id,
            self.revision_id,
            self.timestamp,
            self.change_type.as_str()
        ))
    }

    pub fn get_label_log(&self, text_id: TextId) -> Result<String> {
        if !self.timestamp.chars().all(|c| c.is_ascii_digit()) {
            return Err(anyhow::anyhow!("Invalid timestamp: {:?}", self.timestamp));
        }
        Ok(format!(
            "({},{},'{}','{}','{}',{})",
            self.item_id,
            self.revision_id,
            self.subject.as_str(),
            self.timestamp,
            self.change_type.as_str(),
            text_id
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_change_subject_as_str() {
        assert_eq!(ChangeSubject::Labels.as_str(), "labels");
        assert_eq!(ChangeSubject::Descriptions.as_str(), "descriptions");
        assert_eq!(ChangeSubject::Aliases.as_str(), "aliases");
        assert_eq!(ChangeSubject::Claims.as_str(), "claims");
        assert_eq!(ChangeSubject::Sitelinks.as_str(), "sitelinks");
    }

    #[test]
    fn test_change_type_as_str() {
        assert_eq!(ChangeType::Changed.as_str(), "changed");
        assert_eq!(ChangeType::Removed.as_str(), "removed");
        assert_eq!(ChangeType::Added.as_str(), "added");
    }

    #[test]
    fn test_get_statement_log_valid() {
        let change = Change {
            item_id: 42,
            revision_id: 100,
            timestamp: "20240101000000".to_string(),
            subject: ChangeSubject::Claims,
            change_type: ChangeType::Added,
            property: "P31".to_string(),
            id: "Q42$abc".to_string(),
            ..Default::default()
        };
        let log = change.get_statement_log().unwrap();
        assert_eq!(log, "(42,100,31,'20240101000000','added')");
    }

    #[test]
    fn test_get_statement_log_invalid_property() {
        let change = Change {
            property: "".to_string(),
            timestamp: "20240101000000".to_string(),
            ..Default::default()
        };
        assert!(change.get_statement_log().is_err());
    }

    #[test]
    fn test_get_statement_log_invalid_timestamp() {
        let change = Change {
            property: "P31".to_string(),
            timestamp: "2024-01-01".to_string(),
            ..Default::default()
        };
        assert!(change.get_statement_log().is_err());
    }

    #[test]
    fn test_get_label_log_valid() {
        let change = Change {
            item_id: 42,
            revision_id: 100,
            timestamp: "20240101000000".to_string(),
            subject: ChangeSubject::Labels,
            change_type: ChangeType::Changed,
            ..Default::default()
        };
        let log = change.get_label_log(555).unwrap();
        assert_eq!(log, "(42,100,'labels','20240101000000','changed',555)");
    }

    #[test]
    fn test_get_label_log_invalid_timestamp() {
        let change = Change {
            timestamp: "bad'; DROP TABLE--".to_string(),
            ..Default::default()
        };
        assert!(change.get_label_log(1).is_err());
    }
}
