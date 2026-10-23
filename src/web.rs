//! The web service: the read API at `/api.php` (the URL of the original PHP
//! tool), and the page describing it.

use crate::api::{Action, ApiError, Params, Query, ACTIONS, FORMATS};
use crate::output::{Format, STATUS_OK};
use crate::wdrc::{WdRc, META_RC_TIMESTAMP};
use crate::wikidata_api::USER_AGENT;
use anyhow::{anyhow, Result};
use axum::body::Body;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Form, Router};
use futures::StreamExt;
use serde_json::{json, Value};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use toolforge::pool::mysql_async::{prelude::Queryable, Conn, Row};
use toolforge::pool::ToolsDbPool;
use wikimisc::server::{self, health, jsonp::Jsonp};
use wikimisc::sparql_client::SparqlClient;
use wikimisc::sparql_endpoint::SparqlEndpoint;

const INDEX_HTML: &str = include_str!("../html/index.html");
/// Overridden by `$WDRC_PORT`, and the address by `$WDRC_ADDRESS`.
const DEFAULT_PORT: u16 = 8000;
/// Default for the `web_max_connections` config key. The bot needs its own
/// connections within the tool's limit.
const DEFAULT_MAX_CONNECTIONS: u64 = 4;
/// How long a request waits for a free database connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Queries running longer than this are killed by the server.
const STATEMENT_TIME_LIMIT: Duration = Duration::from_secs(300);
/// Output is sent in chunks of about this size.
const CHUNK_BYTES: usize = 16 * 1024;
/// Chunks buffered ahead of a slow client.
const CHUNKS_BUFFERED: usize = 4;

struct AppState {
    pool: ToolsDbPool,
    database: String,
    sparql: SparqlClient,
}

/// Serves until SIGTERM or SIGINT.
pub async fn run(config_file: &str) -> Result<()> {
    let config = WdRc::read_config(config_file)?;
    let max_connections = config
        .get("web_max_connections")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_MAX_CONNECTIONS);
    let builder = ToolsDbPool::builder()
        .pool_max(max_connections as usize)
        .connect_timeout(CONNECT_TIMEOUT)
        .max_statement_time(STATEMENT_TIME_LIMIT);
    let (pool, database) = WdRc::tool_db(&config, builder)?;
    let sparql = SparqlClient::new(SparqlEndpoint::wdqs(), USER_AGENT)?;
    let state = Arc::new(AppState {
        pool,
        database,
        sparql,
    });
    crate::telemetry::start_web();
    let address = server::listen_addr("WDRC", DEFAULT_PORT);
    let listener = tokio::net::TcpListener::bind(address).await?;
    eprintln!("Listening on {address}");
    server::serve(listener, router(state)).await?;
    Ok(())
}

fn router(state: Arc<AppState>) -> Router {
    let router = Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/api.php", get(api).post(api))
        .route("/healthz", get(health::ok))
        .with_state(state);
    server::request_metrics(
        server::cors_any(server::standard_layers(router)),
        &["/healthz"],
    )
}

async fn api(State(state): State<Arc<AppState>>, Form(params): Form<Params>) -> Response {
    let format = match Format::parse(params.format.as_deref(), params.callback.as_deref()) {
        Ok(format) => format,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    let action = match Action::parse(&params) {
        Ok(action) => action,
        Err(error) => return status_response(&format, StatusCode::BAD_REQUEST, &error.to_string()),
    };
    crate::telemetry::record_api_request(action.name());
    match action {
        Action::Info => Jsonp::new(
            json!({"actions": ACTIONS, "format": FORMATS, "status": STATUS_OK}),
            params.callback,
        )
        .into_response(),
        Action::Lag => lag(&state, params.callback).await,
        Action::Changes(changes) => {
            let queries = match changes.queries(&state.sparql).await {
                Ok(queries) => queries,
                Err(error) => {
                    let status = match error {
                        ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
                        ApiError::Sparql(_) => StatusCode::BAD_GATEWAY,
                    };
                    return status_response(&format, status, &error.to_string());
                }
            };
            match state.pool.connect_readonly(&state.database).await {
                Ok(conn) => stream_records(conn, format, queries),
                Err(e) => {
                    eprintln!("No database connection: {e}");
                    let message = "Database unavailable, please try again later";
                    status_response(&format, StatusCode::SERVICE_UNAVAILABLE, message)
                }
            }
        }
    }
}

fn status_response(format: &Format, status: StatusCode, message: &str) -> Response {
    let headers = [(header::CONTENT_TYPE, format.content_type())];
    (status, headers, format.status_only(message)).into_response()
}

/// How far the processed changes lag behind Wikidata.
async fn lag(state: &AppState, callback: Option<String>) -> Response {
    match cursor_timestamp(state).await {
        Ok(timestamp) => {
            let lag = crate::telemetry::lag(&timestamp).unwrap_or_default();
            let value = json!({
                "lag": crate::api::describe_lag(lag),
                "lag_seconds": lag.as_secs(),
                "timestamp": timestamp,
                "status": STATUS_OK,
            });
            Jsonp::new(value, callback).into_response()
        }
        Err(e) => {
            eprintln!("Lag unavailable: {e}");
            let value = json!({"status": "Lag unavailable, please try again later"});
            (StatusCode::SERVICE_UNAVAILABLE, Jsonp::new(value, callback)).into_response()
        }
    }
}

async fn cursor_timestamp(state: &AppState) -> Result<String> {
    let mut conn = state.pool.connect_readonly(&state.database).await?;
    let rows: Vec<String> = conn
        .exec(
            "SELECT `value` FROM `meta` WHERE `key`=?",
            (META_RC_TIMESTAMP,),
        )
        .await?;
    rows.into_iter()
        .next()
        .ok_or_else(|| anyhow!("No {META_RC_TIMESTAMP:?} in meta"))
}

/// Streams the records of `queries` while they are read. Errors from here
/// on can only be reported in the status at the end of the output.
fn stream_records(mut conn: Conn, format: Format, queries: Vec<Query>) -> Response {
    let (tx, mut rx) = mpsc::channel(CHUNKS_BUFFERED);
    let content_type = format.content_type();
    tokio::spawn(async move {
        let mut out = ChunkSender::new(tx);
        let result = async {
            out.push(&format.begin()).await?;
            send_records(&mut conn, &format, &queries, &mut out).await
        }
        .await;
        let status = match result {
            Ok(()) => STATUS_OK.to_string(),
            Err(e) => {
                eprintln!("API request failed: {e}");
                e.to_string()
            }
        };
        // The client may be gone already.
        let _ = out.push(&format.end(&status)).await;
        let _ = out.flush().await;
    });
    // Unlike `stream::unfold`, this may be polled again after the end, as
    // the compression layer does.
    let chunks = futures::stream::poll_fn(move |cx| {
        rx.poll_recv(cx).map(|chunk| chunk.map(Ok::<_, Infallible>))
    });
    (
        [(header::CONTENT_TYPE, content_type)],
        Body::from_stream(chunks),
    )
        .into_response()
}

async fn send_records(
    conn: &mut Conn,
    format: &Format,
    queries: &[Query],
    out: &mut ChunkSender,
) -> Result<()> {
    let mut index = 0;
    for query in queries {
        let mut rows = conn
            .exec_stream::<Row, _, _>(query.sql(), query.params.clone())
            .await?;
        while let Some(row) = rows.next().await {
            let record = query.table.record(row?)?;
            out.push(&format.record(&record, index)).await?;
            index += 1;
        }
    }
    Ok(())
}

/// Collects output into chunks of about `CHUNK_BYTES` for the response body.
struct ChunkSender {
    tx: mpsc::Sender<String>,
    chunk: String,
}

impl ChunkSender {
    fn new(tx: mpsc::Sender<String>) -> Self {
        Self {
            tx,
            chunk: String::new(),
        }
    }

    async fn push(&mut self, text: &str) -> Result<()> {
        self.chunk.push_str(text);
        if self.chunk.len() >= CHUNK_BYTES {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if !self.chunk.is_empty() {
            let chunk = std::mem::take(&mut self.chunk);
            self.tx
                .send(chunk)
                .await
                .map_err(|_| anyhow!("Client went away"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use std::path::PathBuf;
    use std::sync::OnceLock;
    use tower::ServiceExt;

    /// A state whose database and SPARQL service are never contacted.
    fn state() -> Arc<AppState> {
        // Written once, as the tests run in parallel.
        static MY_CNF: OnceLock<PathBuf> = OnceLock::new();
        let my_cnf = MY_CNF.get_or_init(|| {
            let dir = std::env::temp_dir().join("wdrc_test_web");
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("replica.my.cnf");
            std::fs::write(&path, "[client]\nuser='u1'\npassword='pw'\n").unwrap();
            path
        });
        Arc::new(AppState {
            pool: ToolsDbPool::builder().my_cnf_path(my_cnf).build().unwrap(),
            database: "s1__wdrc_p".to_string(),
            sparql: SparqlClient::new(SparqlEndpoint::new("http://localhost:1/sparql"), "test")
                .unwrap(),
        })
    }

    async fn get(uri: &str) -> (StatusCode, String, String) {
        let request = Request::get(uri).body(Body::empty()).unwrap();
        let response = router(state()).oneshot(request).await.unwrap();
        let status = response.status();
        let content_type = response.headers()[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .to_string();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        (
            status,
            content_type,
            String::from_utf8(body.to_vec()).unwrap(),
        )
    }

    #[tokio::test]
    async fn test_index() {
        let (status, content_type, body) = get("/").await;
        assert_eq!(status, StatusCode::OK);
        assert!(content_type.starts_with("text/html"));
        assert!(body.contains("Wikidata Recent Changes API"));
        assert_eq!(get("/healthz").await.2, "ok");
    }

    #[tokio::test]
    async fn test_info() {
        let (status, _, body) = get("/api.php").await;
        assert_eq!(status, StatusCode::OK);
        let info: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(info["actions"].as_array().unwrap().len(), ACTIONS.len());
        let (_, content_type, body) = get("/api.php?action=x&callback=cb").await;
        assert!(content_type.starts_with("application/javascript"));
        assert!(body.starts_with("cb({"));
    }

    #[tokio::test]
    async fn test_bad_requests() {
        let (status, content_type, body) = get("/api.php?action=properties&since=2024").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(content_type.starts_with("text/plain"));
        assert_eq!(body, "Property ID required\n");

        let (status, _, body) =
            get("/api.php?action=redirects&since=x&format=json&callback=cb").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            r#"cb({"data":[],"status":"Faulty timestamp parameter 'since': x"})"#
        );

        let (status, _, _) = get("/api.php?action=lag&format=json&callback=alert(1)").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _, _) = get("/api.php?format=xml").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
