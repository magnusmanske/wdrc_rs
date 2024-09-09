use anyhow::{anyhow, Result};
use serde_json::{json, Map, Value};
use std::{collections::HashMap, sync::Arc};
use wikimisc::wikidata::Wikidata;

use crate::{
    change::{Change, ChangeSubject, ChangeType},
    ItemId, WdRc,
};

pub type RevisionId = u64;

pub struct RevisionCompare {
    wd: Arc<Wikidata>,
    item_id: ItemId,
    revision_id: RevisionId,
}

impl RevisionCompare {
    pub fn new(wd: Arc<Wikidata>) -> RevisionCompare {
        RevisionCompare {
            wd,
            item_id: 0,
            revision_id: 0,
        }
    }

    pub async fn run(
        &mut self,
        q: &str,
        rev_id_old: RevisionId,
        rev_id_new: RevisionId,
    ) -> Result<Vec<Change>> {
        self.item_id = WdRc::make_id_numeric(q)?;
        self.revision_id = rev_id_new;

        let revisions = self
            .get_revisions_for_item(q, rev_id_old, rev_id_new)
            .await?;
        let rev_old = revisions
            .get(&rev_id_old)
            .ok_or_else(|| anyhow!("Could not load {q} old revision {rev_id_old}"))?;
        let rev_new = revisions
            .get(&rev_id_new)
            .ok_or_else(|| anyhow!("Could not load {q} new revision {rev_id_new}"))?;

        let ret = self.compare_revisions(rev_old, rev_new);
        Ok(ret)
    }

    fn get_revisions_url(q: &str, rev_id_old: RevisionId, rev_id_new: RevisionId) -> String {
        format!("https://www.wikidata.org/w/api.php?action=query&prop=revisions&titles={q}&rvprop=ids|content&rvstartid={rev_id_new}&rvendid={rev_id_old}&rvslots=main&format=json")
    }

    fn extract_revisions(
        rev_id_old: RevisionId,
        rev_id_new: RevisionId,
        j: &Value,
    ) -> HashMap<RevisionId, Value> {
        let mut ret = HashMap::new();
        let pages = match j.get("query") {
            Some(pages) => pages,
            None => return ret,
        };
        let pages = Self::json_object(pages, "pages");
        for page in pages.values() {
            for revision in Self::json_array(page, "revisions") {
                if let Some(rev_id) = revision["revid"].as_u64() {
                    if rev_id == rev_id_old || rev_id == rev_id_new {
                        if let Some(text) = revision["slots"]["main"]["*"].as_str() {
                            if let Ok(j) = serde_json::from_str::<Value>(text) {
                                ret.insert(rev_id, j);
                            }
                        }
                    }
                }
            }
        }
        ret
    }

    async fn get_revisions_for_item(
        &self,
        q: &str,
        rev_id_old: RevisionId,
        rev_id_new: RevisionId,
    ) -> Result<HashMap<RevisionId, Value>> {
        let url = Self::get_revisions_url(q, rev_id_old, rev_id_new);
        let client = self.wd.reqwest_client()?;
        let j = client.get(url).send().await?.json().await?;
        let revisions = Self::extract_revisions(rev_id_old, rev_id_new, &j);
        Ok(revisions)
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
        for (language, label) in old.iter() {
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
                    ret.push(Change {
                        item_id: self.item_id,
                        revision_id: self.revision_id,
                        subject: key.to_owned(),
                        change_type: ChangeType::Changed,
                        language: language.to_owned(),
                        text: new_label.to_string(),
                        ..Default::default()
                    });
                }
            } else {
                ret.push(Change {
                    item_id: self.item_id,
                    revision_id: self.revision_id,
                    subject: key.to_owned(),
                    change_type: ChangeType::Removed,
                    language: language.to_owned(),
                    text: label.to_string(),
                    ..Default::default()
                });
            }
        }
        for (language, label) in new.iter() {
            if !old.contains_key(language) {
                let label = match label["value"].as_str() {
                    Some(label) => label,
                    None => continue,
                };
                ret.push(Change {
                    item_id: self.item_id,
                    revision_id: self.revision_id,
                    subject: key.to_owned(),
                    change_type: ChangeType::Added,
                    language: language.to_owned(),
                    text: label.to_string(),
                    ..Default::default()
                });
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
        old_aliases: &Vec<String>,
        new_aliases: &Vec<String>,
    ) -> Vec<Change> {
        let mut ret = vec![];
        if old_aliases == new_aliases {
            return ret;
        }
        for alias in old_aliases {
            if !new_aliases.contains(alias) {
                ret.push(Change {
                    item_id: self.item_id,
                    revision_id: self.revision_id,
                    subject: ChangeSubject::Aliases,
                    change_type: ChangeType::Removed,
                    language: language.to_string(),
                    text: alias.to_string(),
                    ..Default::default()
                });
            }
        }
        for alias in new_aliases {
            if !old_aliases.contains(alias) {
                ret.push(Change {
                    item_id: self.item_id,
                    revision_id: self.revision_id,
                    subject: ChangeSubject::Aliases,
                    change_type: ChangeType::Added,
                    language: language.to_string(),
                    text: alias.to_string(),
                    ..Default::default()
                });
            }
        }
        ret
    }

    fn compare_aliases(&self, rev_old: &Value, rev_new: &Value) -> Vec<Change> {
        let mut ret = vec![];
        let old = Self::json_object(rev_old, "aliases");
        let new = Self::json_object(rev_new, "aliases");
        let mut all_languages: Vec<String> = old.keys().map(|s| s.to_owned()).collect();
        all_languages.append(&mut new.keys().map(|s| s.to_owned()).collect());
        all_languages.sort();
        all_languages.dedup();

        println!("{old:?}");
        for language in all_languages {
            let old_aliases = Self::extract_aliases_from_map(&old, &language);
            let new_aliases = Self::extract_aliases_from_map(&new, &language);
            ret.append(&mut self.compare_aliases_in_language(
                &language,
                &old_aliases,
                &new_aliases,
            ));
        }
        ret
    }

    fn compare_sitelinks(&self, rev_old: &Value, rev_new: &Value) -> Vec<Change> {
        let mut ret = vec![];
        let old = Self::json_object(rev_old, "sitelinks");
        let new = Self::json_object(rev_new, "sitelinks");
        for (site, link) in old.iter() {
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
                    ret.push(Change {
                        item_id: self.item_id,
                        revision_id: self.revision_id,
                        subject: ChangeSubject::Sitelinks,
                        change_type: ChangeType::Changed,
                        site: site.to_string(),
                        title: new_link.to_string(),
                        ..Default::default()
                    });
                }
            } else {
                ret.push(Change {
                    item_id: self.item_id,
                    revision_id: self.revision_id,
                    subject: ChangeSubject::Sitelinks,
                    change_type: ChangeType::Removed,
                    site: site.to_string(),
                    title: link.to_string(),
                    ..Default::default()
                });
            }
        }
        for (site, link) in new.iter() {
            if !old.contains_key(site) {
                let link = match link["title"].as_str() {
                    Some(link) => link,
                    None => continue,
                };
                ret.push(Change {
                    item_id: self.item_id,
                    revision_id: self.revision_id,
                    subject: ChangeSubject::Sitelinks,
                    change_type: ChangeType::Added,
                    site: site.to_string(),
                    title: link.to_string(),
                    ..Default::default()
                });
            }
        }

        ret
    }

    fn get_claim_by_id(claim_id: &str, claims: &Map<String, Value>) -> Option<Value> {
        for (_property, prop_claims) in claims.iter() {
            for claim in prop_claims.as_array().unwrap_or(&vec![]) {
                if claim.get("id").unwrap().as_str().unwrap() == claim_id {
                    return Some(claim.to_owned());
                }
            }
        }
        None
    }

    fn compare_statements(&self, rev_old: &Value, rev_new: &Value) -> Vec<Change> {
        let mut ret = vec![];
        let old_claims = Self::json_object(rev_old, "claims");
        let new_claims = Self::json_object(rev_new, "claims");

        let mut all_properties: Vec<String> = old_claims.keys().map(|s| s.to_owned()).collect();
        all_properties.append(&mut new_claims.keys().map(|s| s.to_owned()).collect());
        all_properties.sort();
        all_properties.dedup();

        for (property, prop_claims) in old_claims.iter() {
            for claim in prop_claims.as_array().unwrap_or(&vec![]) {
                let claim_id = claim.get("id").unwrap().as_str().unwrap();
                let new_claim = Self::get_claim_by_id(claim_id, &new_claims);
                if new_claim.is_none() {
                    ret.push(Change {
                        item_id: self.item_id,
                        revision_id: self.revision_id,
                        subject: ChangeSubject::Claims,
                        change_type: ChangeType::Removed,
                        property: property.to_string(),
                        id: claim_id.to_string(),
                        ..Default::default()
                    });
                } else {
                    let new_claim = new_claim.unwrap();
                    if claim != &new_claim {
                        ret.push(Change {
                            item_id: self.item_id,
                            revision_id: self.revision_id,
                            subject: ChangeSubject::Claims,
                            change_type: ChangeType::Changed,
                            property: property.to_string(),
                            id: claim_id.to_string(),
                            ..Default::default()
                        });
                    }
                }
            }
        }
        for (property, prop_claims) in new_claims.iter() {
            for claim in prop_claims.as_array().unwrap_or(&vec![]) {
                let claim_id = claim.get("id").unwrap().as_str().unwrap();
                let old_claim = Self::get_claim_by_id(claim_id, &old_claims);
                if old_claim.is_none() {
                    ret.push(Change {
                        item_id: self.item_id,
                        revision_id: self.revision_id,
                        subject: ChangeSubject::Claims,
                        change_type: ChangeType::Added,
                        property: property.to_string(),
                        id: claim_id.to_string(),
                        ..Default::default()
                    });
                }
            }
        }

        ret
    }

    fn compare_revisions(&self, rev_old: &Value, rev_new: &Value) -> Vec<Change> {
        let mut ret = vec![];
        ret.append(&mut self.compare_labels(rev_old, rev_new));
        ret.append(&mut self.compare_descriptions(rev_old, rev_new));
        ret.append(&mut self.compare_aliases(rev_old, rev_new));
        ret.append(&mut self.compare_statements(rev_old, rev_new));
        ret.append(&mut self.compare_sitelinks(rev_old, rev_new));
        ret
    }

    fn json_object(j: &Value, key: &str) -> Map<String, Value> {
        let o = match j.get(key) {
            Some(v) => v,
            None => return serde_json::Map::new(),
        };
        o.as_object()
            .map(|v| v.to_owned())
            .unwrap_or(serde_json::Map::new())
    }

    fn json_array(j: &Value, key: &str) -> Vec<Value> {
        let o = match j.get(key) {
            Some(v) => v,
            None => return vec![],
        };
        o.as_array().map(|v| v.to_owned()).unwrap_or(vec![])
    }

    fn extract_aliases_from_map(aliases: &Map<String, Value>, language: &String) -> Vec<String> {
        let aliases = aliases
            .get(language)
            .map(|v| v.to_owned())
            .unwrap_or(json!([]));
        let aliases = match aliases.as_array() {
            Some(aliases) => aliases,
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

    use super::*;

    #[tokio::test]
    async fn test_get_revisions_for_item() {
        let wd = Arc::new(Wikidata::new());
        let wdrc = RevisionCompare::new(wd);
        let q = "Q42";
        let rev_id_old = 2208025531;
        let rev_id_new = 2208025540;
        let revisions = wdrc
            .get_revisions_for_item(q, rev_id_old, rev_id_new)
            .await
            .unwrap();
        assert_eq!(revisions.len(), 2);
        assert_eq!(
            revisions.get(&2208025531).unwrap()["id"].as_str().unwrap(),
            "Q42"
        );
        assert_eq!(
            revisions.get(&2208025540).unwrap()["id"].as_str().unwrap(),
            "Q42"
        );
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
        let wd = Arc::new(Wikidata::new());
        let rc = RevisionCompare::new(wd);
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
        let wd = Arc::new(Wikidata::new());
        let rc = RevisionCompare::new(wd);
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
        let wd = Arc::new(Wikidata::new());
        let rc = RevisionCompare::new(wd);
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
        let wd = Arc::new(Wikidata::new());
        let rc = RevisionCompare::new(wd);
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
        let wd = Arc::new(Wikidata::new());
        let rc = RevisionCompare::new(wd);
        let changes = rc.compare_statements(&old, &new);
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
        assert_eq!(changes, expected);
    }
}
