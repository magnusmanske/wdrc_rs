use crate::revision_compare::RevisionId;
use anyhow::{anyhow, Result};
use reqwest::Client;
use serde_json::Value;
use std::{collections::HashMap, time::Duration};

/// The API rejects more than this many revision IDs per request for
/// anonymous clients (`apihighlimits` would raise it to 500).
pub const MAX_REVIDS_PER_REQUEST: usize = 50;

const API_URL: &str = "https://www.wikidata.org/w/api.php";
const USER_AGENT: &str = concat!(
    "wdrc_rs/",
    env!("CARGO_PKG_VERSION"),
    " (https://wdrc.toolforge.org/)"
);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Wikidata API client, cheap to clone.
///
/// The inner `reqwest::Client` owns the connection pool, so a single instance is
/// shared by all requests: building one per request would cost a TLS handshake
/// each time and prevent connection reuse.
#[derive(Debug, Clone)]
pub struct WikidataApi {
    client: Client,
    api_url: String,
}

impl WikidataApi {
    /// `max_idle_per_host` should match the number of concurrent requests.
    pub fn new(max_idle_per_host: usize) -> Result<Self> {
        Self::with_url(API_URL, max_idle_per_host)
    }

    fn with_url(api_url: &str, max_idle_per_host: usize) -> Result<Self> {
        let client = Client::builder()
            .user_agent(USER_AGENT)
            .timeout(REQUEST_TIMEOUT)
            // Entity JSON compresses roughly 6-fold, and this is the bulk of our traffic.
            .gzip(true)
            .pool_max_idle_per_host(max_idle_per_host)
            .build()?;
        Ok(Self {
            client,
            api_url: api_url.to_string(),
        })
    }

    /// Fetches the entity JSON of the given revisions in a single request.
    ///
    /// Revisions the API does not return (deleted, suppressed, or with
    /// unparsable content) are simply absent from the result.
    pub async fn get_revisions(
        &self,
        revision_ids: &[RevisionId],
    ) -> Result<HashMap<RevisionId, Value>> {
        if revision_ids.is_empty() {
            return Ok(HashMap::new());
        }
        if revision_ids.len() > MAX_REVIDS_PER_REQUEST {
            return Err(anyhow!(
                "{} revision IDs requested, the API allows {MAX_REVIDS_PER_REQUEST}",
                revision_ids.len()
            ));
        }
        let revids = revision_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join("|");
        let response: Value = self
            .client
            .get(&self.api_url)
            .query(&[
                ("action", "query"),
                ("prop", "revisions"),
                ("rvprop", "ids|content"),
                ("rvslots", "main"),
                ("format", "json"),
                ("formatversion", "2"),
                ("revids", &revids),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Self::extract_revisions(&response)
    }

    fn extract_revisions(response: &Value) -> Result<HashMap<RevisionId, Value>> {
        if let Some(error) = response.get("error") {
            return Err(anyhow!("Wikidata API error: {error}"));
        }
        let pages = match response["query"]["pages"].as_array() {
            Some(pages) => pages,
            None => return Err(anyhow!("Unexpected API response: {response}")),
        };
        let revisions = pages
            .iter()
            .filter_map(|page| page["revisions"].as_array())
            .flatten()
            .filter_map(Self::parse_revision)
            .collect();
        Ok(revisions)
    }

    fn parse_revision(revision: &Value) -> Option<(RevisionId, Value)> {
        let revision_id = revision["revid"].as_u64()?;
        let content = revision["slots"]["main"]["content"].as_str()?;
        Some((revision_id, serde_json::from_str(content).ok()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_revisions() {
        let response = json!({"query":{"pages":[
            {"title":"Q1","revisions":[
                {"revid":11,"slots":{"main":{"content":"{\"id\":\"Q1\"}"}}},
                {"revid":12,"slots":{"main":{"content":"not json"}}},
            ]},
            {"title":"Q2","revisions":[
                {"revid":21,"slots":{"main":{"content":"{\"id\":\"Q2\"}"}}},
                {"revid":22,"texthidden":true},
            ]},
        ]}});
        let revisions = WikidataApi::extract_revisions(&response).unwrap();
        // Revisions across several pages are merged; unusable ones are skipped.
        assert_eq!(revisions.len(), 2);
        assert_eq!(revisions[&11]["id"], json!("Q1"));
        assert_eq!(revisions[&21]["id"], json!("Q2"));
    }

    #[test]
    fn test_extract_revisions_errors() {
        let error = json!({"error":{"code":"toomanyvalues","info":"..."}});
        assert!(WikidataApi::extract_revisions(&error).is_err());
        assert!(WikidataApi::extract_revisions(&json!({})).is_err());
    }

    #[tokio::test]
    async fn test_get_revisions_rejects_oversized_batch() {
        let api = WikidataApi::new(1).unwrap();
        let too_many: Vec<RevisionId> = (0..=MAX_REVIDS_PER_REQUEST as u64).collect();
        assert!(api.get_revisions(&too_many).await.is_err());
        assert!(api.get_revisions(&[]).await.unwrap().is_empty());
    }

    #[tokio::test]
    #[ignore = "requires network access to www.wikidata.org"]
    async fn test_get_revisions_live() {
        let api = WikidataApi::new(1).unwrap();
        let revisions = api.get_revisions(&[2208025531, 2208025540]).await.unwrap();
        assert_eq!(revisions.len(), 2);
        assert_eq!(revisions[&2208025531]["id"], json!("Q42"));
        assert_eq!(revisions[&2208025540]["id"], json!("Q42"));
    }
}
