//! The API's output formats. Output is rendered piece by piece (`begin`, one
//! `record` each, `end`), so that large results can be streamed; the status
//! comes last, so an error in the middle of a result can still be reported.

use serde::Serialize;
use wikimisc::escape;
use wikimisc::jsonp::{self, CallbackRules};

/// The status of a request that went through.
pub const STATUS_OK: &str = "OK";

const WIKIDATA: &str = "https://www.wikidata.org";

/// One change, as reported to clients. The fields are serialized in this
/// order, and only where set.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Record {
    /// The item, e.g. `Q42`.
    pub item: String,
    /// The redirect target, e.g. `Q42`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// The property of a statement, e.g. `P31`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
    pub timestamp: String,
    /// `added`, `changed`, `removed`, `redirected`, `item_created` or `item_deleted`.
    #[serde(rename = "type")]
    pub change_type: String,
    /// `labels`, `descriptions`, `aliases` or `sitelinks`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub element: Option<String>,
    /// The language or site.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

impl Record {
    fn html(&self) -> String {
        let item = escape::html(&self.item);
        let field = |key: &str, value: Option<&str>| {
            value.map(|value| format!("{key}: {}", escape::html(value)))
        };
        let fields = [
            Some(format!(
                "<a href='{WIKIDATA}/wiki/{item}' target='_blank'>{item}</a>"
            )),
            field("target", self.target.as_deref()),
            field("property", self.property.as_deref()),
            self.revision.map(|revision| format!(
                "<a href='{WIKIDATA}/w/index.php?title={item}&amp;oldid={revision}' target='_blank'>rev {revision}</a>"
            )),
            field("timestamp", Some(&self.timestamp)),
            field("type", Some(&self.change_type)),
            field("element", self.element.as_deref()),
            field("text", self.text.as_deref()),
        ];
        let spans: String = fields
            .iter()
            .flatten()
            .map(|field| format!("<span style='margin-right:0.5rem;'>{field}</span>"))
            .collect();
        format!("<div>{spans}</div>\n")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Format {
    /// `{"data":[…],"status":"OK"}`, wrapped in `callback(…)` for JSONP.
    Json { callback: Option<String> },
    /// One JSON object per line; the status follows only if it is not OK.
    Jsonl,
    /// One `<div>` per record; the status follows only if it is not OK.
    Html,
}

impl Format {
    /// The `format` parameter, JSONL by default, and the JSONP `callback`.
    pub fn parse(format: Option<&str>, callback: Option<&str>) -> Result<Self, String> {
        match format.unwrap_or("jsonl") {
            "json" => {
                let callback = callback.filter(|c| !c.is_empty());
                match callback {
                    Some(c) if !jsonp::is_valid_callback(c, CallbackRules::default()) => {
                        Err("Invalid callback name".to_string())
                    }
                    _ => Ok(Self::Json {
                        callback: callback.map(str::to_string),
                    }),
                }
            }
            "jsonl" => Ok(Self::Jsonl),
            "html" => Ok(Self::Html),
            other => Err(format!(
                "Unknown format {other:?}, expected 'json', 'jsonl' or 'html'"
            )),
        }
    }

    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Json { callback: None } => "application/json",
            Self::Json { callback: Some(_) } => jsonp::CONTENT_TYPE,
            Self::Jsonl => "text/plain; charset=utf-8",
            Self::Html => "text/html; charset=utf-8",
        }
    }

    pub fn begin(&self) -> String {
        match self {
            Self::Json { callback } => match callback {
                Some(callback) => format!("{callback}({{\"data\":["),
                None => "{\"data\":[".to_string(),
            },
            Self::Jsonl | Self::Html => String::new(),
        }
    }

    /// The `index`th record (counting from 0) of the response.
    pub fn record(&self, record: &Record, index: usize) -> String {
        // Serializing a `Record` cannot fail: it holds strings and numbers only.
        let json = || serde_json::to_string(record).unwrap_or_default();
        match self {
            Self::Json { .. } if index > 0 => format!(",{}", json()),
            Self::Json { .. } => json(),
            Self::Jsonl => format!("{}\n", json()),
            Self::Html => record.html(),
        }
    }

    pub fn end(&self, status: &str) -> String {
        match self {
            Self::Json { callback } => {
                let status = serde_json::to_string(status).unwrap_or_default();
                let close = if callback.is_some() { ")" } else { "" };
                format!("],\"status\":{status}}}{close}")
            }
            _ if status == STATUS_OK => String::new(),
            Self::Jsonl => format!("{status}\n"),
            Self::Html => format!("<div>{}</div>\n", escape::html(status)),
        }
    }

    /// A complete response without records, e.g. for an error.
    pub fn status_only(&self, status: &str) -> String {
        self.begin() + &self.end(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn statement() -> Record {
        Record {
            item: "Q42".to_string(),
            property: Some("P31".to_string()),
            revision: Some(123),
            timestamp: "20240101000000".to_string(),
            change_type: "added".to_string(),
            ..Default::default()
        }
    }

    fn render(format: &Format, records: &[Record], status: &str) -> String {
        let mut out = format.begin();
        for (index, record) in records.iter().enumerate() {
            out += &format.record(record, index);
        }
        out + &format.end(status)
    }

    #[test]
    fn test_parse() {
        assert_eq!(Format::parse(None, None), Ok(Format::Jsonl));
        assert_eq!(Format::parse(Some("html"), Some("cb")), Ok(Format::Html));
        assert_eq!(
            Format::parse(Some("json"), Some("")),
            Ok(Format::Json { callback: None })
        );
        assert_eq!(
            Format::parse(Some("json"), Some("cb")),
            Ok(Format::Json {
                callback: Some("cb".to_string())
            })
        );
        assert!(Format::parse(Some("json"), Some("alert(1);x")).is_err());
        assert!(Format::parse(Some("xml"), None).is_err());
    }

    #[test]
    fn test_json() {
        let records = [statement(), statement()];
        let out = render(&Format::Json { callback: None }, &records, STATUS_OK);
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed["status"], "OK");
        assert_eq!(parsed["data"].as_array().unwrap().len(), 2);
        assert_eq!(
            parsed["data"][0],
            serde_json::json!({"item":"Q42","property":"P31","revision":123,"timestamp":"20240101000000","type":"added"})
        );
        let out = render(&Format::Json { callback: None }, &[], "Bad \"since\"");
        assert_eq!(out, r#"{"data":[],"status":"Bad \"since\""}"#);
        let out = render(
            &Format::Json {
                callback: Some("cb".to_string()),
            },
            &[],
            STATUS_OK,
        );
        assert_eq!(out, r#"cb({"data":[],"status":"OK"})"#);
    }

    #[test]
    fn test_jsonl() {
        let out = render(&Format::Jsonl, &[statement(), statement()], STATUS_OK);
        assert_eq!(out.lines().count(), 2);
        assert!(out.starts_with(r#"{"item":"Q42","property":"P31","#));
        let out = render(&Format::Jsonl, &[statement()], "Timeout");
        assert!(out.ends_with("}\nTimeout\n"));
    }

    #[test]
    fn test_html() {
        let record = Record {
            text: Some("<b>".to_string()),
            ..statement()
        };
        let out = render(&Format::Html, &[record], "a<b");
        assert!(out.contains("<a href='https://www.wikidata.org/wiki/Q42' target='_blank'>Q42</a>"));
        assert!(out.contains("title=Q42&amp;oldid=123' target='_blank'>rev 123</a>"));
        assert!(out.contains("property: P31"));
        assert!(out.contains("text: &lt;b&gt;"));
        assert!(out.ends_with("<div>a&lt;b</div>\n"));
        assert!(!render(&Format::Html, &[], STATUS_OK).contains("OK"));
    }
}
