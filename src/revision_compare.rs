use anyhow::Result;
use serde_json::{Map, Value};
use std::collections::{BTreeSet, HashMap};

use crate::{
    change::{Change, ChangeSubject, ChangeType},
    recent_changes::ChangedItem,
    ItemId, WdRc,
};

pub type RevisionId = u64;

/// Compares two revisions of one item and reports the differences.
/// Purely computational: revision content is fetched by `WikidataApi`.
pub struct RevisionCompare {
    item_id: ItemId,
    revision_id: RevisionId,
    timestamp: String,
}

impl RevisionCompare {
    /// Fails if the changed item does not carry a usable item ID.
    pub fn new(ci: &ChangedItem) -> Result<Self> {
        Ok(Self {
            item_id: WdRc::make_id_numeric(ci.q())?,
            revision_id: ci.rev_new(),
            timestamp: ci.timestamp().to_string(),
        })
    }

    fn create_label_change(
        &self,
        subject: &ChangeSubject,
        change_type: ChangeType,
        language: &str,
        text: &str,
    ) -> Change {
        Change {
            item_id: self.item_id,
            revision_id: self.revision_id,
            timestamp: self.timestamp.to_owned(),
            subject: subject.to_owned(),
            change_type,
            language: language.to_owned(),
            text: text.to_string(),
            ..Default::default()
        }
    }

    fn compare_labels_descriptions(
        &self,
        rev_old: &Value,
        rev_new: &Value,
        key: ChangeSubject,
    ) -> Vec<Change> {
        let mut ret = vec![];
        let old = Self::json_object(rev_old, key.as_str());
        let new = Self::json_object(rev_new, key.as_str());
        for (language, label) in old {
            let label = match label["value"].as_str() {
                Some(label) => label,
                None => continue,
            };
            if let Some(new_label) = new.get(language) {
                let new_label = match new_label["value"].as_str() {
                    Some(new_label) => new_label,
                    None => continue,
                };
                if label != new_label {
                    ret.push(self.create_label_change(
                        &key,
                        ChangeType::Changed,
                        language,
                        new_label,
                    ));
                }
            } else {
                ret.push(self.create_label_change(&key, ChangeType::Removed, language, label));
            }
        }
        for (language, label) in new {
            if !old.contains_key(language) {
                let label = match label["value"].as_str() {
                    Some(label) => label,
                    None => continue,
                };
                ret.push(self.create_label_change(&key, ChangeType::Added, language, label));
            }
        }
        ret
    }

    fn compare_labels(&self, rev_old: &Value, rev_new: &Value) -> Vec<Change> {
        self.compare_labels_descriptions(rev_old, rev_new, ChangeSubject::Labels)
    }

    fn compare_descriptions(&self, rev_old: &Value, rev_new: &Value) -> Vec<Change> {
        self.compare_labels_descriptions(rev_old, rev_new, ChangeSubject::Descriptions)
    }

    fn compare_aliases_in_language(
        &self,
        language: &str,
        old_aliases: &[String],
        new_aliases: &[String],
    ) -> Vec<Change> {
        let mut ret = vec![];
        if old_aliases == new_aliases {
            return ret;
        }
        for alias in old_aliases {
            if !new_aliases.contains(alias) {
                ret.push(self.create_label_change(
                    &ChangeSubject::Aliases,
                    ChangeType::Removed,
                    language,
                    alias,
                ));
            }
        }
        for alias in new_aliases {
            if !old_aliases.contains(alias) {
                ret.push(self.create_label_change(
                    &ChangeSubject::Aliases,
                    ChangeType::Added,
                    language,
                    alias,
                ));
            }
        }
        ret
    }

    fn compare_aliases(&self, rev_old: &Value, rev_new: &Value) -> Vec<Change> {
        let mut ret = vec![];
        let old = Self::json_object(rev_old, "aliases");
        let new = Self::json_object(rev_new, "aliases");
        let all_languages: BTreeSet<&str> =
            old.keys().chain(new.keys()).map(|s| s.as_str()).collect();

        for language in all_languages {
            let old_aliases = Self::extract_aliases_from_map(old, language);
            let new_aliases = Self::extract_aliases_from_map(new, language);
            ret.append(&mut self.compare_aliases_in_language(language, &old_aliases, &new_aliases));
        }
        ret
    }

    fn create_sitelink_change(&self, change_type: ChangeType, site: &str, title: &str) -> Change {
        Change {
            item_id: self.item_id,
            revision_id: self.revision_id,
            timestamp: self.timestamp.to_owned(),
            subject: ChangeSubject::Sitelinks,
            change_type,
            site: site.to_owned(),
            title: title.to_string(),
            ..Default::default()
        }
    }

    fn compare_sitelinks(&self, rev_old: &Value, rev_new: &Value) -> Vec<Change> {
        let mut ret = vec![];
        let old = Self::json_object(rev_old, "sitelinks");
        let new = Self::json_object(rev_new, "sitelinks");
        for (site, link) in old {
            let link = match link["title"].as_str() {
                Some(link) => link,
                None => continue,
            };
            if let Some(new_link) = new.get(site) {
                let new_link = match new_link["title"].as_str() {
                    Some(new_link) => new_link,
                    None => continue,
                };
                if link != new_link {
                    ret.push(self.create_sitelink_change(ChangeType::Changed, site, new_link));
                }
            } else {
                ret.push(self.create_sitelink_change(ChangeType::Removed, site, link));
            }
        }
        for (site, link) in new {
            if !old.contains_key(site) {
                let link = match link["title"].as_str() {
                    Some(link) => link,
                    None => continue,
                };
                ret.push(self.create_sitelink_change(ChangeType::Added, site, link));
            }
        }

        ret
    }

    #[cfg(test)]
    fn get_claim_by_id(claim_id: &str, claims: &Map<String, Value>) -> Option<Value> {
        for (_property, prop_claims) in claims.iter() {
            for claim in prop_claims.as_array().unwrap_or(&vec![]) {
                if claim.get("id").and_then(|v| v.as_str()) == Some(claim_id) {
                    return Some(claim.to_owned());
                }
            }
        }
        None
    }

    /// Build a HashMap from claim ID to (property, claim Value) for O(1) lookups.
    fn build_claim_index(claims: &Map<String, Value>) -> HashMap<&str, (&str, &Value)> {
        let mut index = HashMap::new();
        for (property, prop_claims) in claims {
            if let Some(arr) = prop_claims.as_array() {
                for claim in arr {
                    if let Some(id) = claim.get("id").and_then(|v| v.as_str()) {
                        index.insert(id, (property.as_str(), claim));
                    }
                }
            }
        }
        index
    }

    fn create_claim_change(&self, change_type: ChangeType, property: &str, id: &str) -> Change {
        Change {
            item_id: self.item_id,
            revision_id: self.revision_id,
            timestamp: self.timestamp.to_owned(),
            subject: ChangeSubject::Claims,
            change_type,
            property: property.to_owned(),
            id: id.to_string(),
            ..Default::default()
        }
    }

    fn compare_statements(&self, rev_old: &Value, rev_new: &Value) -> Vec<Change> {
        let mut ret = vec![];
        let old_claims = Self::json_object(rev_old, "claims");
        let new_claims = Self::json_object(rev_new, "claims");

        // Build O(1) lookup indexes instead of scanning all claims per lookup
        let old_index = Self::build_claim_index(old_claims);
        let new_index = Self::build_claim_index(new_claims);

        // Find removed and changed claims
        for (claim_id, (property, old_claim)) in &old_index {
            match new_index.get(claim_id) {
                None => {
                    ret.push(self.create_claim_change(ChangeType::Removed, property, claim_id));
                }
                Some((_new_prop, new_claim)) => {
                    if old_claim != new_claim {
                        ret.push(self.create_claim_change(ChangeType::Changed, property, claim_id));
                    }
                }
            }
        }

        // Find added claims
        for (claim_id, (property, _new_claim)) in &new_index {
            if !old_index.contains_key(claim_id) {
                ret.push(self.create_claim_change(ChangeType::Added, property, claim_id));
            }
        }

        ret
    }

    pub fn compare_revisions(&self, rev_old: &Value, rev_new: &Value) -> Vec<Change> {
        let mut ret = vec![];
        ret.append(&mut self.compare_labels(rev_old, rev_new));
        ret.append(&mut self.compare_descriptions(rev_old, rev_new));
        ret.append(&mut self.compare_aliases(rev_old, rev_new));
        ret.append(&mut self.compare_statements(rev_old, rev_new));
        ret.append(&mut self.compare_sitelinks(rev_old, rev_new));
        ret
    }

    fn json_object<'a>(j: &'a Value, key: &str) -> &'a Map<String, Value> {
        static EMPTY_MAP: std::sync::LazyLock<Map<String, Value>> =
            std::sync::LazyLock::new(Map::new);
        j.get(key).and_then(|v| v.as_object()).unwrap_or(&EMPTY_MAP)
    }

    fn extract_aliases_from_map(aliases: &Map<String, Value>, language: &str) -> Vec<String> {
        let aliases = match aliases.get(language).and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return vec![],
        };
        let aliases: Vec<String> = aliases
            .iter()
            .filter_map(|v| v["value"].as_str())
            .map(|s| s.to_string())
            .collect();
        aliases
    }
}

#[cfg(test)]
mod tests {
    use crate::change::{Change, ChangeSubject, ChangeType};
    use serde_json::json;

    use super::*;

    /// A comparer with neutral item/revision/timestamp, so that the expected
    /// `Change`s can be written as `Default`s.
    fn comparer() -> RevisionCompare {
        RevisionCompare {
            item_id: 0,
            revision_id: 0,
            timestamp: String::new(),
        }
    }

    #[test]
    fn test_compare_labels() {
        let old = json!({"labels":{
            "en": {"value": "old"},
            "de": {"value": "alt"},
            "fr": {"value": "ancien"},}
        });
        let new = json!({"labels":{
            "en": {"value": "new"},
            "de": {"value": "alt"},
            "it": {"value":"nuovo"}}
        });
        let rc = comparer();
        let changes = rc.compare_labels(&old, &new);
        let expected = vec![
            Change {
                subject: ChangeSubject::Labels,
                change_type: ChangeType::Changed,
                language: "en".to_string(),
                text: "new".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Labels,
                change_type: ChangeType::Removed,
                language: "fr".to_string(),
                text: "ancien".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Labels,
                change_type: ChangeType::Added,
                language: "it".to_string(),
                text: "nuovo".to_string(),
                ..Default::default()
            },
            // json!({"change":"changed","language":"en","text":"new","subject":"labels"}),
            //     json!({"change":"removed","language":"fr","text":"ancien","subject":"labels"}),
            //     json!({"change":"added","language":"it","text":"nuovo","subject":"labels"}),
        ];
        assert_eq!(changes, expected);
    }

    #[test]
    fn test_compare_descriptions() {
        let old = json!({"descriptions":{
            "en": {"value": "old"},
            "de": {"value": "alt"},
            "fr": {"value": "ancien"},}
        });
        let new = json!({"descriptions":{
            "en": {"value":"new"},
            "de": {"value": "alt"},
            "it": {"value":"nuovo"}}
        });
        let rc = comparer();
        let changes = rc.compare_descriptions(&old, &new);
        let expected = vec![
            Change {
                subject: ChangeSubject::Descriptions,
                change_type: ChangeType::Changed,
                language: "en".to_string(),
                text: "new".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Descriptions,
                change_type: ChangeType::Removed,
                language: "fr".to_string(),
                text: "ancien".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Descriptions,
                change_type: ChangeType::Added,
                language: "it".to_string(),
                text: "nuovo".to_string(),
                ..Default::default()
            },
            // json!({"change":"changed","language":"en","text":"new","subject":"descriptions"}),
            // json!({"change":"removed","language":"fr","text":"ancien","subject":"descriptions"}),
            // json!({"change":"added","language":"it","text":"nuovo","subject":"descriptions"}),
        ];
        assert_eq!(changes, expected);
    }

    #[test]
    fn test_compare_aliases() {
        let old = json!({"aliases":{
            "en": [{"value":"old"},{"value":"older"}],
            "de": [{"value":"alt"}],
            "fr": [{"value":"ancien"}]}
        });
        let new = json!({"aliases":{
            "en": [{"value":"new"},{"value":"older"}],
            "de": [{"value":"alt"}],
            "it": [{"value":"nuovo"}]}
        });
        let rc = comparer();
        let changes = rc.compare_aliases(&old, &new);
        let expected = vec![
            Change {
                subject: ChangeSubject::Aliases,
                change_type: ChangeType::Removed,
                language: "en".to_string(),
                text: "old".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Aliases,
                change_type: ChangeType::Added,
                language: "en".to_string(),
                text: "new".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Aliases,
                change_type: ChangeType::Removed,
                language: "fr".to_string(),
                text: "ancien".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Aliases,
                change_type: ChangeType::Added,
                language: "it".to_string(),
                text: "nuovo".to_string(),
                ..Default::default()
            },
            // json!({"change": "removed","language": "en","text": "old","subject": "aliases"}),
            // json!({"change": "added","language": "en","text": "new","subject": "aliases"}),
            // json!({"change": "removed","language": "fr","text": "ancien","subject": "aliases"}),
            // json!({"change": "added","language": "it","text": "nuovo","subject": "aliases"}),
        ];
        assert_eq!(changes, expected);
    }

    #[test]
    fn test_compare_sitelinks() {
        let old = json!({"sitelinks":{
            "enwiki": {"title":"old"},
            "dewiki": {"title":"alt"},
            "frwiki": {"title":"ancien"}}
        });
        let new = json!({"sitelinks":{
            "enwiki": {"title":"new"},
            "dewiki": {"title":"alt"},
            "itwiki": {"title":"nuovo"}}
        });
        let rc = comparer();
        let changes = rc.compare_sitelinks(&old, &new);
        let expected = vec![
            Change {
                subject: ChangeSubject::Sitelinks,
                change_type: ChangeType::Changed,
                site: "enwiki".to_string(),
                title: "new".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Sitelinks,
                change_type: ChangeType::Removed,
                site: "frwiki".to_string(),
                title: "ancien".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Sitelinks,
                change_type: ChangeType::Added,
                site: "itwiki".to_string(),
                title: "nuovo".to_string(),
                ..Default::default()
            },
            // json!({"change":"changed","site":"enwiki","title":"new","subject":"sitelinks"}),
            //    json!({"change":"removed","site":"frwiki","title":"ancien","subject":"sitelinks"}),
            //    json!({"change":"added","site":"itwiki","title":"nuovo","subject":"sitelinks"}),
        ];
        assert_eq!(changes, expected);
    }

    #[test]
    fn test_compare_claims() {
        let old = json!({"claims":{
            "P1": [
                {"id": "Q1$123", "mainsnak": {"snaktype": "value", "datavalue": {"value": "old"}}},
                {"id": "Q1$124", "mainsnak": {"snaktype": "value", "datavalue": {"value": "old2"}}},
                {"id": "Q1$125", "mainsnak": {"snaktype": "value", "datavalue": {"value": "old3"}}},
            ],
            "P2": [
                {"id": "Q1$126", "mainsnak": {"snaktype": "value", "datavalue": {"value": "old"}}},
            ],
        }});
        let new = json!({"claims":{
            "P1": [
                {"id": "Q1$123", "mainsnak": {"snaktype": "value", "datavalue": {"value": "new"}}},
                {"id": "Q1$124", "mainsnak": {"snaktype": "value", "datavalue": {"value": "old2"}}},
                {"id": "Q1$127", "mainsnak": {"snaktype": "value", "datavalue": {"value": "new2"}}},
            ],
            "P3": [
                {"id": "Q1$128", "mainsnak": {"snaktype": "value", "datavalue": {"value": "new"}}},
            ],
        }});
        let rc = comparer();
        let mut changes = rc.compare_statements(&old, &new);
        changes.sort_by(|a, b| a.id.cmp(&b.id));
        let expected = vec![
            Change {
                subject: ChangeSubject::Claims,
                change_type: ChangeType::Changed,
                property: "P1".to_string(),
                id: "Q1$123".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Claims,
                change_type: ChangeType::Removed,
                property: "P1".to_string(),
                id: "Q1$125".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Claims,
                change_type: ChangeType::Removed,
                property: "P2".to_string(),
                id: "Q1$126".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Claims,
                change_type: ChangeType::Added,
                property: "P1".to_string(),
                id: "Q1$127".to_string(),
                ..Default::default()
            },
            Change {
                subject: ChangeSubject::Claims,
                change_type: ChangeType::Added,
                property: "P3".to_string(),
                id: "Q1$128".to_string(),
                ..Default::default()
            },
            // json!({"subject": "claims","change": "changed","property": "P1","id": "Q1$123"}),
            // json!({"subject": "claims","change": "removed","property": "P1","id": "Q1$125"}),
            // json!({"subject": "claims","change": "removed","property": "P2","id": "Q1$126"}),
            // json!({"subject": "claims","change": "added","property": "P1","id": "Q1$127"}),
            // json!({"subject": "claims","change": "added","property": "P3","id": "Q1$128"}),
        ];
        let mut expected = expected;
        expected.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(changes, expected);
    }

    #[test]
    fn test_get_claim_by_id() {
        // Normal case: claim with valid "id" field
        let mut claims = Map::new();
        claims.insert(
            "P1".to_string(),
            json!([
                {"id": "Q1$100", "mainsnak": {"snaktype": "value"}},
                {"id": "Q1$101", "mainsnak": {"snaktype": "value"}},
            ]),
        );
        let result = RevisionCompare::get_claim_by_id("Q1$101", &claims);
        assert!(result.is_some());
        assert_eq!(result.unwrap()["id"], "Q1$101");

        // Claim missing the "id" field entirely — should return None, not panic
        let mut claims_no_id = Map::new();
        claims_no_id.insert(
            "P1".to_string(),
            json!([
                {"mainsnak": {"snaktype": "value"}},
            ]),
        );
        let result = RevisionCompare::get_claim_by_id("Q1$100", &claims_no_id);
        assert!(result.is_none());

        // Claim where "id" is not a string — should return None, not panic
        let mut claims_bad_id = Map::new();
        claims_bad_id.insert(
            "P1".to_string(),
            json!([
                {"id": 12345, "mainsnak": {"snaktype": "value"}},
            ]),
        );
        let result = RevisionCompare::get_claim_by_id("12345", &claims_bad_id);
        assert!(result.is_none());
    }

    #[test]
    fn test_compare_revisions_empty() {
        let rc = comparer();
        let old = json!({});
        let new = json!({});
        let changes = rc.compare_revisions(&old, &new);
        assert!(changes.is_empty());
    }
}
