//! The read API, as offered by `api.php` of the original PHP tool: request
//! parameters parsed into typed actions, and the queries answering them.

use crate::change::{ChangeSubject, ChangeType};
use crate::output::Record;
use serde::Deserialize;
use std::time::Duration;
use toolforge::pool::mysql_async::{from_row_opt, FromRowError, Row, Value};
use wikimisc::sparql_client::{SparqlClient, SparqlError};
use wikimisc::sparql_value::SparqlValue;

/// The actions, as listed by the `info` action.
pub const ACTIONS: [&str; 7] = [
    "lag",
    "properties",
    "text",
    "items",
    "redirects",
    "creations",
    "deletions",
];
pub const FORMATS: [&str; 3] = ["json", "jsonl", "html"];

/// Items per `items` request, so that a query stays below MariaDB's limit
/// of 65535 placeholders per statement.
const MAX_ITEMS: usize = 50_000;
/// Separators of list parameters; `items` comes from a textarea.
const LIST_SEPARATORS: [char; 4] = [',', ';', '|', '\n'];
const SINCE_REQUIRED: &str = "\"since\" parameter required (eg 20220101020304)";
const ITEMS_REQUIRED: &str = "\"items\" parameter is required and must be non-empty";

/// The request parameters, by their `api.php` names.
#[derive(Debug, Default, Deserialize)]
pub struct Params {
    pub action: Option<String>,
    pub format: Option<String>,
    pub callback: Option<String>,
    properties: Option<String>,
    #[serde(rename = "type")]
    change_type: Option<String>,
    since: Option<String>,
    until: Option<String>,
    text: Option<String>,
    element: Option<String>,
    items: Option<String>,
    sparql: Option<String>,
}

/// Why a request could not be answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    /// The request is at fault.
    BadRequest(String),
    /// The SPARQL service failed.
    Sparql(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(message) => f.write_str(message),
            Self::Sparql(message) => write!(f, "SPARQL query failed: {message}"),
        }
    }
}

fn bad_request(message: impl Into<String>) -> ApiError {
    ApiError::BadRequest(message.into())
}

/// A MediaWiki timestamp, given as a prefix of at least the year and padded
/// with zeros. Anything but digits is dropped, so `2022-07-07` works too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timestamp(String);

impl Timestamp {
    fn parse(value: &str) -> Option<Self> {
        let digits: String = value.chars().filter(char::is_ascii_digit).collect();
        (4..=14)
            .contains(&digits.len())
            .then(|| Self(format!("{digits:0<14}")))
    }

    /// The `name` parameter, which is optional.
    fn optional(name: &str, value: Option<&str>) -> Result<Option<Self>, ApiError> {
        match value.map(str::trim).filter(|v| !v.is_empty()) {
            None => Ok(None),
            Some(value) => Self::parse(value).map(Some).ok_or_else(|| {
                bad_request(format!("Faulty timestamp parameter '{name}': {value}"))
            }),
        }
    }

    fn since(params: &Params) -> Result<Self, ApiError> {
        Self::optional("since", params.since.as_deref())?.ok_or_else(|| bad_request(SINCE_REQUIRED))
    }
}

/// A time span: `since` is required, `until` is not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Period {
    since: Timestamp,
    until: Option<Timestamp>,
}

impl Period {
    fn parse(params: &Params) -> Result<Self, ApiError> {
        Ok(Self {
            since: Timestamp::since(params)?,
            until: Timestamp::optional("until", params.until.as_deref())?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Items {
    Numbers(Vec<u64>),
    /// A SPARQL query, whose first variable holds the items.
    Sparql(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// What the API offers; for any unknown action.
    Info,
    /// How far the data lags behind Wikidata.
    Lag,
    Changes(Changes),
}

/// The actions that read changes; empty filter lists mean "any".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Changes {
    Properties {
        properties: Vec<u64>,
        types: Vec<ChangeType>,
        since: Timestamp,
    },
    Text {
        texts: Vec<String>,
        elements: Vec<ChangeSubject>,
        types: Vec<ChangeType>,
        since: Timestamp,
    },
    Items {
        items: Items,
        since: Timestamp,
    },
    Redirects(Period),
    Creations(Period),
    Deletions(Period),
}

impl Action {
    pub fn parse(params: &Params) -> Result<Self, ApiError> {
        let changes = match params.action.as_deref().unwrap_or_default() {
            "lag" => return Ok(Self::Lag),
            "properties" => Changes::Properties {
                properties: non_empty(
                    list(&params.properties, "property", |p| number(p, 'P'))?,
                    "Property ID required",
                )?,
                types: change_types(params)?,
                since: Timestamp::since(params)?,
            },
            "text" => Changes::Text {
                texts: list(&params.text, "text", |t| Some(t.to_string()))?,
                elements: list(&params.element, "element", |e| {
                    e.parse().ok().filter(|e| *e != ChangeSubject::Claims)
                })?,
                types: change_types(params)?,
                since: Timestamp::since(params)?,
            },
            "items" => Changes::Items {
                items: Self::items(params)?,
                since: Timestamp::since(params)?,
            },
            "redirects" => Changes::Redirects(Period::parse(params)?),
            "creations" => Changes::Creations(Period::parse(params)?),
            "deletions" => Changes::Deletions(Period::parse(params)?),
            _ => return Ok(Self::Info),
        };
        Ok(Self::Changes(changes))
    }

    /// For metrics: one of a fixed set.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Lag => "lag",
            Self::Changes(changes) => match changes {
                Changes::Properties { .. } => "properties",
                Changes::Text { .. } => "text",
                Changes::Items { .. } => "items",
                Changes::Redirects(_) => "redirects",
                Changes::Creations(_) => "creations",
                Changes::Deletions(_) => "deletions",
            },
        }
    }

    /// The listed items, or else the SPARQL query.
    fn items(params: &Params) -> Result<Items, ApiError> {
        let numbers = list(&params.items, "item", |q| number(q, 'Q'))?;
        let sparql = params.sparql.as_deref().map(str::trim).unwrap_or_default();
        match (numbers.is_empty(), sparql.is_empty()) {
            (false, _) => Ok(Items::Numbers(numbers)),
            (true, false) => Ok(Items::Sparql(sparql.to_string())),
            (true, true) => Err(bad_request(ITEMS_REQUIRED)),
        }
    }
}

fn change_types(params: &Params) -> Result<Vec<ChangeType>, ApiError> {
    list(&params.change_type, "type", |t| t.parse().ok())
}

/// The entries of a list parameter, each parsed with `parse`.
fn list<T>(
    value: &Option<String>,
    name: &str,
    parse: impl Fn(&str) -> Option<T>,
) -> Result<Vec<T>, ApiError> {
    value
        .as_deref()
        .unwrap_or_default()
        .split(LIST_SEPARATORS)
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| parse(entry).ok_or_else(|| bad_request(format!("Invalid {name}: {entry:?}"))))
        .collect()
}

fn non_empty<T>(values: Vec<T>, message: &str) -> Result<Vec<T>, ApiError> {
    match values.is_empty() {
        true => Err(bad_request(message)),
        false => Ok(values),
    }
}

/// The number of an entity ID with the given prefix (`Q42` → 42), which may
/// be left out (`42`).
fn number(id: &str, prefix: char) -> Option<u64> {
    let digits = id
        .strip_prefix(prefix)
        .or_else(|| id.strip_prefix(prefix.to_ascii_lowercase()))
        .unwrap_or(id);
    digits.parse().ok().filter(|n| *n > 0)
}

impl Changes {
    /// The queries answering this request, in order. Items given by a SPARQL
    /// query are looked up with `sparql`.
    pub async fn queries(self, sparql: &SparqlClient) -> Result<Vec<Query>, ApiError> {
        let period =
            |table, period: Period| vec![Query::new(table, &period.since).until(period.until)];
        Ok(match self {
            Self::Properties {
                properties,
                types,
                since,
            } => vec![Query::new(Table::Statements, &since)
                .any_of("`property`", properties)
                .any_of("`change_type`", type_names(&types))],
            Self::Text {
                texts,
                elements,
                types,
                since,
            } => vec![Query::new(Table::Labels, &since)
                .any_of("`change_type`", type_names(&types))
                .any_of("`type`", elements.iter().map(|e| e.as_str().to_string()))
                .any_of("`texts`.`value`", texts)],
            Self::Items { items, since } => {
                let mut items = match items {
                    Items::Numbers(numbers) => numbers,
                    Items::Sparql(query) => sparql_items(sparql, &query).await?,
                };
                items.sort_unstable();
                items.dedup();
                let items = non_empty(items, ITEMS_REQUIRED)?;
                if items.len() > MAX_ITEMS {
                    return Err(bad_request(format!(
                        "{} items requested, at most {MAX_ITEMS} are allowed",
                        items.len()
                    )));
                }
                Table::ALL
                    .into_iter()
                    .map(|table| {
                        Query::new(table, &since).any_of(table.item_column(), items.clone())
                    })
                    .collect()
            }
            Self::Redirects(p) => period(Table::Redirects, p),
            Self::Creations(p) => period(Table::Creations, p),
            Self::Deletions(p) => period(Table::Deletions, p),
        })
    }
}

fn type_names(types: &[ChangeType]) -> Vec<String> {
    types.iter().map(|t| t.as_str().to_string()).collect()
}

/// The items in the first variable of the result of a SPARQL query.
async fn sparql_items(client: &SparqlClient, sparql: &str) -> Result<Vec<u64>, ApiError> {
    let result = client.query(sparql).await.map_err(|e| match e {
        // The query is at fault.
        SparqlError::Endpoint {
            status: 400..=499, ..
        } => bad_request(format!("SPARQL query failed: {e}")),
        e => ApiError::Sparql(e.to_string()),
    })?;
    let Some(variable) = result.head().get("vars").and_then(|vars| vars.first()) else {
        return Ok(vec![]);
    };
    Ok(result
        .bindings()
        .iter()
        .filter_map(|row| match row.get(variable)? {
            SparqlValue::Entity(id) => number(id, 'Q'),
            _ => None,
        })
        .collect())
}

/// The tables the API reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Table {
    Labels,
    Statements,
    Redirects,
    Creations,
    Deletions,
}

impl Table {
    /// In the order the `items` action reports them.
    const ALL: [Table; 5] = [
        Table::Labels,
        Table::Statements,
        Table::Redirects,
        Table::Creations,
        Table::Deletions,
    ];

    /// The columns `record` reads.
    fn select(self) -> &'static str {
        match self {
            Self::Labels => "SELECT `item`,`revision`,`timestamp`,`change_type`,`type`,`texts`.`value` FROM `labels` JOIN `texts` ON `texts`.`id`=`language`",
            Self::Statements => "SELECT `item`,`property`,`revision`,`timestamp`,`change_type` FROM `statements`",
            Self::Redirects => "SELECT `source`,`target`,`timestamp` FROM `redirects`",
            Self::Creations => "SELECT `q`,`timestamp` FROM `creations`",
            Self::Deletions => "SELECT `q`,`timestamp` FROM `deletions`",
        }
    }

    fn item_column(self) -> &'static str {
        match self {
            Self::Labels | Self::Statements => "`item`",
            Self::Redirects => "`source`",
            Self::Creations | Self::Deletions => "`q`",
        }
    }

    pub fn record(self, row: Row) -> Result<Record, FromRowError> {
        let q = |item: u64| format!("Q{item}");
        Ok(match self {
            Self::Labels => {
                let (item, revision, timestamp, change_type, element, text) = from_row_opt(row)?;
                Record {
                    item: q(item),
                    revision: Some(revision),
                    timestamp,
                    change_type,
                    element: Some(element),
                    text: Some(text),
                    ..Default::default()
                }
            }
            Self::Statements => {
                let (item, property, revision, timestamp, change_type): (u64, u64, _, _, _) =
                    from_row_opt(row)?;
                Record {
                    item: q(item),
                    property: Some(format!("P{property}")),
                    revision: Some(revision),
                    timestamp,
                    change_type,
                    ..Default::default()
                }
            }
            Self::Redirects => {
                let (source, target, timestamp) = from_row_opt(row)?;
                Record {
                    item: q(source),
                    target: Some(q(target)),
                    timestamp,
                    change_type: "redirected".to_string(),
                    ..Default::default()
                }
            }
            Self::Creations | Self::Deletions => {
                let (item, timestamp) = from_row_opt(row)?;
                let change_type = match self {
                    Self::Creations => "item_created",
                    _ => "item_deleted",
                };
                Record {
                    item: q(item),
                    timestamp,
                    change_type: change_type.to_string(),
                    ..Default::default()
                }
            }
        })
    }
}

/// A SELECT on one table, by timestamp, with positional parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub table: Table,
    conditions: Vec<String>,
    pub params: Vec<Value>,
}

impl Query {
    fn new(table: Table, since: &Timestamp) -> Self {
        Self {
            table,
            conditions: vec!["`timestamp`>=?".to_string()],
            params: vec![since.0.clone().into()],
        }
    }

    fn until(mut self, until: Option<Timestamp>) -> Self {
        if let Some(until) = until {
            self.conditions.push("`timestamp`<=?".to_string());
            self.params.push(until.0.into());
        }
        self
    }

    /// Restricts `column` to `values`, unless there are none.
    fn any_of<T: Into<Value>>(mut self, column: &str, values: impl IntoIterator<Item = T>) -> Self {
        let start = self.params.len();
        self.params.extend(values.into_iter().map(Into::into));
        let count = self.params.len() - start;
        if count > 0 {
            let placeholders = vec!["?"; count].join(",");
            self.conditions
                .push(format!("{column} IN ({placeholders})"));
        }
        self
    }

    pub fn sql(&self) -> String {
        format!(
            "{} WHERE {} ORDER BY `timestamp`",
            self.table.select(),
            self.conditions.join(" AND ")
        )
    }
}

/// `lag`, as days, hours, minutes and seconds, leaving out leading zeros.
pub fn describe_lag(lag: Duration) -> String {
    let seconds = lag.as_secs();
    let parts = [
        (seconds / 86400, "days"),
        (seconds / 3600 % 24, "hours"),
        (seconds / 60 % 60, "minutes"),
        (seconds % 60, "seconds"),
    ];
    let first = parts
        .iter()
        .position(|(n, _)| *n > 0)
        .unwrap_or(parts.len() - 1);
    parts[first..]
        .iter()
        .map(|(n, unit)| format!("{n} {unit}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use wikimisc::sparql_endpoint::SparqlEndpoint;

    fn params(query: &str) -> Params {
        serde_json::from_value(
            query
                .split('&')
                .filter_map(|pair| pair.split_once('='))
                .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
                .collect(),
        )
        .unwrap()
    }

    fn parse(query: &str) -> Result<Action, ApiError> {
        Action::parse(&params(query))
    }

    /// Never contacted: the tests run no SPARQL queries.
    fn sparql() -> SparqlClient {
        SparqlClient::new(SparqlEndpoint::new("http://localhost:1/sparql"), "test").unwrap()
    }

    async fn queries(query: &str) -> Result<Vec<Query>, ApiError> {
        match parse(query)? {
            Action::Changes(changes) => changes.queries(&sparql()).await,
            other => panic!("{other:?}"),
        }
    }

    fn ts(s: &str) -> Value {
        Value::from(s)
    }

    #[test]
    fn test_timestamp() {
        assert_eq!(Timestamp::parse("2022").unwrap().0, "20220000000000");
        assert_eq!(
            Timestamp::parse("2022-07-07 09:57").unwrap().0,
            "20220707095700"
        );
        assert_eq!(
            Timestamp::parse("20220707095700").unwrap().0,
            "20220707095700"
        );
        for invalid in ["", "202", "202207070957001", "'; DROP TABLE x"] {
            assert_eq!(Timestamp::parse(invalid), None, "{invalid:?}");
        }
    }

    #[test]
    fn test_info_and_lag() {
        assert_eq!(parse(""), Ok(Action::Info));
        assert_eq!(parse("action=nonsense"), Ok(Action::Info));
        assert_eq!(parse("action=lag"), Ok(Action::Lag));
    }

    #[tokio::test]
    async fn test_properties() {
        let queries =
            queries("action=properties&properties=P31,p279,17&since=2024&type=added,removed")
                .await
                .unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(
            queries[0].sql(),
            "SELECT `item`,`property`,`revision`,`timestamp`,`change_type` FROM `statements` WHERE `timestamp`>=? AND `property` IN (?,?,?) AND `change_type` IN (?,?) ORDER BY `timestamp`"
        );
        assert_eq!(
            queries[0].params,
            vec![
                ts("20240000000000"),
                Value::from(31u64),
                Value::from(279u64),
                Value::from(17u64),
                ts("added"),
                ts("removed")
            ]
        );
    }

    #[test]
    fn test_properties_errors() {
        let error = |query| parse(query).unwrap_err().to_string();
        assert_eq!(
            error("action=properties&since=2024"),
            "Property ID required"
        );
        assert_eq!(error("action=properties&properties=P31"), SINCE_REQUIRED);
        assert_eq!(
            error("action=properties&properties=Q31&since=2024"),
            "Invalid property: \"Q31\""
        );
        assert_eq!(
            error("action=properties&properties=P31&since=2024&type=deleted"),
            "Invalid type: \"deleted\""
        );
        assert_eq!(
            error("action=properties&properties=P31&since=20"),
            "Faulty timestamp parameter 'since': 20"
        );
    }

    #[tokio::test]
    async fn test_text() {
        let queries = queries("action=text&text=de,enwiki&element=labels,sitelinks&since=2024")
            .await
            .unwrap();
        assert_eq!(
            queries[0].sql(),
            "SELECT `item`,`revision`,`timestamp`,`change_type`,`type`,`texts`.`value` FROM `labels` JOIN `texts` ON `texts`.`id`=`language` WHERE `timestamp`>=? AND `type` IN (?,?) AND `texts`.`value` IN (?,?) ORDER BY `timestamp`"
        );
        assert_eq!(
            queries[0].params,
            vec![
                ts("20240000000000"),
                ts("labels"),
                ts("sitelinks"),
                ts("de"),
                ts("enwiki")
            ]
        );
        // Statements are not in the `labels` table.
        assert!(parse("action=text&element=claims&since=2024").is_err());
    }

    #[tokio::test]
    async fn test_items() {
        let queries = queries("action=items&items=Q2;q1|Q1\nQ3 ,,&since=2024")
            .await
            .unwrap();
        let tables: Vec<Table> = queries.iter().map(|q| q.table).collect();
        assert_eq!(tables, Table::ALL);
        let params = vec![
            ts("20240000000000"),
            Value::from(1u64),
            Value::from(2u64),
            Value::from(3u64),
        ];
        assert!(queries.iter().all(|q| q.params == params));
        assert!(queries[2]
            .sql()
            .contains("FROM `redirects` WHERE `timestamp`>=? AND `source` IN (?,?,?)"));
        assert!(queries[3]
            .sql()
            .contains("FROM `creations` WHERE `timestamp`>=? AND `q` IN (?,?,?)"));
    }

    #[test]
    fn test_items_sources() {
        assert_eq!(
            parse("action=items&items=Q1&sparql=SELECT&since=2024"),
            Ok(Action::Changes(Changes::Items {
                items: Items::Numbers(vec![1]),
                since: Timestamp("20240000000000".to_string()),
            }))
        );
        assert_eq!(
            parse("action=items&sparql= SELECT ?q {} &since=2024"),
            Ok(Action::Changes(Changes::Items {
                items: Items::Sparql("SELECT ?q {}".to_string()),
                since: Timestamp("20240000000000".to_string()),
            }))
        );
        assert_eq!(
            parse("action=items&since=2024"),
            Err(bad_request(ITEMS_REQUIRED))
        );
    }

    #[tokio::test]
    async fn test_too_many_items() {
        let too_many: Vec<String> = (1..=MAX_ITEMS + 1).map(|n| format!("Q{n}")).collect();
        let query = format!("action=items&since=2024&items={}", too_many.join(","));
        assert!(queries(&query).await.is_err());
    }

    #[tokio::test]
    async fn test_periods() {
        let redirects = queries("action=redirects&since=2023&until=20230706")
            .await
            .unwrap();
        assert_eq!(
            redirects[0].sql(),
            "SELECT `source`,`target`,`timestamp` FROM `redirects` WHERE `timestamp`>=? AND `timestamp`<=? ORDER BY `timestamp`"
        );
        assert_eq!(
            redirects[0].params,
            vec![ts("20230000000000"), ts("20230706000000")]
        );
        let deletions = queries("action=deletions&since=2023").await.unwrap();
        assert_eq!(deletions[0].params.len(), 1);
        assert!(parse("action=creations&since=2023&until=x").is_err());
    }

    #[test]
    fn test_number() {
        assert_eq!(number("Q42", 'Q'), Some(42));
        assert_eq!(number("q42", 'Q'), Some(42));
        assert_eq!(number("42", 'Q'), Some(42));
        for invalid in ["", "Q", "Q0", "P42", "Q-1", "Q4 2"] {
            assert_eq!(number(invalid, 'Q'), None, "{invalid:?}");
        }
    }

    #[test]
    fn test_describe_lag() {
        assert_eq!(describe_lag(Duration::from_secs(0)), "0 seconds");
        assert_eq!(
            describe_lag(Duration::from_secs(65)),
            "1 minutes, 5 seconds"
        );
        assert_eq!(
            describe_lag(Duration::from_secs(2 * 86400 + 5)),
            "2 days, 0 hours, 0 minutes, 5 seconds"
        );
    }
}
