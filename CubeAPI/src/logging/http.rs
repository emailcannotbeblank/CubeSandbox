// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

//! HTTP Webhook log backend.
//!
//! `HttpLogger::log()` only enqueues events into a bounded Tokio channel. HTTP
//! delivery, retry and optional signing are handled by background worker tasks,
//! so slow or unreachable receivers do not block sandbox lifecycle APIs.

use super::{LogEvent, Logger};
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::Deserialize;
use sha2::Sha256;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, warn};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

const DEFAULT_QUEUE_SIZE: usize = 1024;
const DEFAULT_WORKERS: usize = 4;
const DEFAULT_TIMEOUT_SECS: u64 = 3;
const DEFAULT_MAX_RETRIES: usize = 3;
const INITIAL_BACKOFF_MS: u64 = 500;
const MAX_BACKOFF_MS: u64 = 10_000;
const ALL_EVENTS: &str = "*";

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct WebhookEndpointConfig {
    /// Optional diagnostic name used in logs.
    #[serde(default)]
    pub name: Option<String>,
    /// Full URL to POST a single event payload to.
    pub url: String,
    /// Subscribed event names. Use `"*"` or an empty list to receive all events.
    #[serde(default)]
    pub events: Vec<String>,
    /// Optional HMAC-SHA256 shared secret.
    #[serde(default)]
    pub secret: Option<String>,
}

impl WebhookEndpointConfig {
    fn subscribes_to(&self, event: &str) -> bool {
        self.events.is_empty()
            || self
                .events
                .iter()
                .any(|configured| configured == ALL_EVENTS || configured == event)
    }

    fn label(&self) -> &str {
        self.name.as_deref().unwrap_or(self.url.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct HttpLoggerConfig {
    pub endpoints: Vec<WebhookEndpointConfig>,
    pub queue_size: usize,
    pub workers: usize,
    pub timeout_secs: u64,
    pub max_retries: usize,
}

impl HttpLoggerConfig {
    pub fn from_server_config(cfg: &crate::config::ServerConfig) -> anyhow::Result<Option<Self>> {
        if !cfg.webhook_enabled {
            return Ok(None);
        }

        let Some(raw) = cfg
            .webhook_endpoints_json
            .as_deref()
            .map(str::trim)
            .filter(|raw| !raw.is_empty())
        else {
            warn!("webhook enabled but CUBE_API_WEBHOOK_ENDPOINTS_JSON is empty; webhook disabled");
            return Ok(None);
        };

        let endpoints: Vec<WebhookEndpointConfig> = serde_json::from_str(raw)?;
        let endpoints: Vec<_> = endpoints
            .into_iter()
            .filter(|endpoint| {
                let keep = !endpoint.url.trim().is_empty();
                if !keep {
                    warn!("dropping webhook endpoint with empty url");
                }
                keep
            })
            .collect();

        if endpoints.is_empty() {
            warn!("webhook enabled but no valid endpoints were configured; webhook disabled");
            return Ok(None);
        }

        Ok(Some(Self {
            endpoints,
            queue_size: cfg.webhook_queue_size.max(1),
            workers: cfg.webhook_workers.max(1),
            timeout_secs: cfg.webhook_timeout_secs.max(1),
            max_retries: cfg.webhook_max_retries,
        }))
    }
}

impl Default for HttpLoggerConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            queue_size: DEFAULT_QUEUE_SIZE,
            workers: DEFAULT_WORKERS,
            timeout_secs: DEFAULT_TIMEOUT_SECS,
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }
}

enum Msg {
    Event(LogEvent),
    Flush(oneshot::Sender<()>),
}

#[derive(Clone)]
pub struct HttpLogger {
    tx: mpsc::Sender<Msg>,
    subscribed_events: Arc<HashSet<String>>,
}

impl HttpLogger {
    pub fn new(config: HttpLoggerConfig) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()?;
        let endpoints = Arc::new(config.endpoints);
        let subscribed_events = Arc::new(subscribed_event_set(&endpoints));
        let (tx, rx) = mpsc::channel::<Msg>(config.queue_size);
        let rx = Arc::new(Mutex::new(rx));
        let workers = config.workers.max(1);

        for worker_id in 0..workers {
            let rx = rx.clone();
            let endpoints = endpoints.clone();
            let client = client.clone();
            let max_retries = config.max_retries;
            tokio::spawn(async move {
                worker_loop(worker_id, rx, endpoints, client, max_retries).await;
            });
        }

        Ok(Self {
            tx,
            subscribed_events,
        })
    }

    fn should_enqueue(&self, event: &LogEvent) -> bool {
        self.subscribed_events.contains(ALL_EVENTS) || self.subscribed_events.contains(&event.event)
    }
}

#[async_trait]
impl Logger for HttpLogger {
    async fn log(&self, event: LogEvent) {
        if !self.should_enqueue(&event) {
            return;
        }

        match self.tx.try_send(Msg::Event(event)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(msg)) => {
                if let Msg::Event(event) = msg {
                    warn!(event = %event.event, "webhook queue full; dropping event");
                }
            }
            Err(mpsc::error::TrySendError::Closed(msg)) => {
                if let Msg::Event(event) = msg {
                    warn!(event = %event.event, "webhook worker stopped; dropping event");
                }
            }
        }
    }

    async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Msg::Flush(tx)).await.is_ok() {
            let _ = rx.await;
        }
    }

    fn name(&self) -> &'static str {
        "http"
    }
}

fn subscribed_event_set(endpoints: &[WebhookEndpointConfig]) -> HashSet<String> {
    let mut events = HashSet::new();
    for endpoint in endpoints {
        if endpoint.events.is_empty() {
            events.insert(ALL_EVENTS.to_string());
            continue;
        }
        events.extend(endpoint.events.iter().cloned());
    }
    events
}

async fn worker_loop(
    worker_id: usize,
    rx: Arc<Mutex<mpsc::Receiver<Msg>>>,
    endpoints: Arc<Vec<WebhookEndpointConfig>>,
    client: reqwest::Client,
    max_retries: usize,
) {
    loop {
        let msg = {
            let mut rx = rx.lock().await;
            rx.recv().await
        };

        match msg {
            Some(Msg::Event(event)) => {
                deliver_event(&client, &endpoints, &event, max_retries).await;
            }
            Some(Msg::Flush(reply)) => {
                let _ = reply.send(());
            }
            None => {
                debug!(worker_id, "webhook worker exiting");
                return;
            }
        }
    }
}

async fn deliver_event(
    client: &reqwest::Client,
    endpoints: &[WebhookEndpointConfig],
    event: &LogEvent,
    max_retries: usize,
) {
    for endpoint in endpoints
        .iter()
        .filter(|endpoint| endpoint.subscribes_to(&event.event))
    {
        if let Err(err) = deliver_with_retry(client, endpoint, event, max_retries).await {
            warn!(
                endpoint = %endpoint.label(),
                event = %event.event,
                error = %err,
                "webhook delivery failed"
            );
        }
    }
}

async fn deliver_with_retry(
    client: &reqwest::Client,
    endpoint: &WebhookEndpointConfig,
    event: &LogEvent,
    max_retries: usize,
) -> anyhow::Result<()> {
    let payload = serde_json::to_vec(event)?;
    let delivery_id = Uuid::new_v4().to_string();
    let attempts = max_retries.saturating_add(1);

    for attempt in 0..attempts {
        match post_once(client, endpoint, event, &payload, &delivery_id).await {
            Ok(()) => return Ok(()),
            Err(err) if attempt + 1 < attempts => {
                let delay = retry_delay(attempt);
                warn!(
                    endpoint = %endpoint.label(),
                    event = %event.event,
                    attempt = attempt + 1,
                    next_retry_ms = delay.as_millis(),
                    error = %err,
                    "webhook delivery attempt failed; retrying"
                );
                tokio::time::sleep(delay).await;
            }
            Err(err) => return Err(err),
        }
    }

    Ok(())
}

async fn post_once(
    client: &reqwest::Client,
    endpoint: &WebhookEndpointConfig,
    event: &LogEvent,
    payload: &[u8],
    delivery_id: &str,
) -> anyhow::Result<()> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert("X-CubeSandbox-Event", HeaderValue::from_str(&event.event)?);
    headers.insert(
        "X-CubeSandbox-Timestamp",
        HeaderValue::from_str(&event.timestamp.to_rfc3339())?,
    );
    headers.insert(
        "X-CubeSandbox-Delivery",
        HeaderValue::from_str(delivery_id)?,
    );

    if let Some(secret) = endpoint
        .secret
        .as_deref()
        .filter(|secret| !secret.is_empty())
    {
        headers.insert(
            "X-CubeSandbox-Signature",
            HeaderValue::from_str(&signature_header(
                secret,
                &event.timestamp.to_rfc3339(),
                payload,
            ))?,
        );
    }

    let response = client
        .post(endpoint.url.as_str())
        .headers(headers)
        .body(payload.to_vec())
        .send()
        .await?;

    if response.status().is_success() {
        debug!(
            endpoint = %endpoint.label(),
            event = %event.event,
            "webhook delivered"
        );
        return Ok(());
    }

    Err(anyhow::anyhow!("HTTP {}", response.status()))
}

fn signature_header(secret: &str, timestamp: &str, payload: &[u8]) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(payload);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

fn retry_delay(attempt: usize) -> Duration {
    let multiplier = 1u64.checked_shl(attempt.min(10) as u32).unwrap_or(1);
    Duration::from_millis((INITIAL_BACKOFF_MS.saturating_mul(multiplier)).min(MAX_BACKOFF_MS))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::LogLevel;

    #[test]
    fn endpoint_subscription_supports_explicit_empty_and_wildcard() {
        let explicit = WebhookEndpointConfig {
            name: None,
            url: "http://localhost/hook".to_string(),
            events: vec!["sandbox.created".to_string()],
            secret: None,
        };
        assert!(explicit.subscribes_to("sandbox.created"));
        assert!(!explicit.subscribes_to("sandbox.deleted"));

        let all_empty = WebhookEndpointConfig {
            events: vec![],
            ..explicit.clone()
        };
        assert!(all_empty.subscribes_to("sandbox.deleted"));

        let all_wildcard = WebhookEndpointConfig {
            events: vec![ALL_EVENTS.to_string()],
            ..explicit
        };
        assert!(all_wildcard.subscribes_to("sandbox.paused"));
    }

    #[test]
    fn signature_uses_timestamp_dot_payload() {
        let signature = signature_header("secret", "2026-07-01T00:00:00Z", br#"{"event":"x"}"#);
        assert_eq!(
            signature,
            "sha256=c482dd63520aaae0c6e0a45afa7f987ce96247ee18f94f3857169fd1e25c0a4f"
        );
    }

    #[tokio::test]
    async fn log_returns_quickly_when_receiver_is_slow() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let _ = tokio::io::AsyncWriteExt::write_all(
                    &mut stream,
                    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n",
                )
                .await;
            }
        });

        let logger = HttpLogger::new(HttpLoggerConfig {
            endpoints: vec![WebhookEndpointConfig {
                name: None,
                url: format!("http://{addr}/webhook"),
                events: vec!["sandbox.created".to_string()],
                secret: None,
            }],
            queue_size: 8,
            workers: 1,
            timeout_secs: 5,
            max_retries: 0,
        })
        .unwrap();

        let start = std::time::Instant::now();
        logger
            .log(LogEvent::new(LogLevel::Info, "sandbox.created"))
            .await;

        assert!(start.elapsed() < Duration::from_millis(100));
    }
}
