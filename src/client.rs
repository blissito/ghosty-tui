//! Cliente SSE contra el endpoint público de easybits.
//! Mismo shape de frames que `app/lib/easybits.server.ts:684-694` en ghosty-studio:
//! `data: {"type":"chunk"|"token"|"error"|"done", value?, message?}`.

use anyhow::Result;
use futures_util::StreamExt;
use serde_json::json;
use tokio::sync::mpsc;

const BASE_URL: &str = "https://www.easybits.cloud";

/// Lo que el bucle de UI recibe del stream.
pub enum Frame {
    Token(String),
    Error(String),
    Done,
}

/// Abre el stream y empuja frames por `tx`. Cualquier error de transporte
/// se reporta como `Frame::Error` para que la UI nunca paniquee.
pub async fn stream_message(
    client: reqwest::Client,
    agent: String,
    token: String,
    session: String,
    content: String,
    tx: mpsc::Sender<Frame>,
) {
    if let Err(e) = run(&client, &agent, &token, &session, &content, &tx).await {
        let _ = tx.send(Frame::Error(e.to_string())).await;
    }
}

async fn run(
    client: &reqwest::Client,
    agent: &str,
    token: &str,
    session: &str,
    content: &str,
    tx: &mpsc::Sender<Frame>,
) -> Result<()> {
    let url = format!("{BASE_URL}/api/v2/agents/{agent}/message");
    let resp = client
        .post(&url)
        .bearer_auth(token)
        .json(&json!({ "content": content, "sessionId": session }))
        .send()
        .await?;

    if !resp.status().is_success() {
        let code = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("HTTP {code}: {}", body.chars().take(200).collect::<String>());
    }

    // Acumulamos bytes (no String) para no partir un char UTF-8 en el borde de un chunk.
    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();

    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk?);
        // Procesa cada frame SSE completo (separados por blank line "\n\n").
        while let Some(idx) = find_subslice(&buf, b"\n\n") {
            let frame: Vec<u8> = buf.drain(..idx + 2).collect();
            let text = String::from_utf8_lossy(&frame[..idx]);
            for line in text.lines() {
                let Some(data) = line.trim_start().strip_prefix("data:") else {
                    continue;
                };
                let data = data.trim();
                if data.is_empty() {
                    continue;
                }
                let Ok(v) = serde_json::from_str::<serde_json::Value>(data) else {
                    continue;
                };
                match v.get("type").and_then(|t| t.as_str()) {
                    Some("chunk") | Some("token") => {
                        if let Some(val) = v.get("value").and_then(|x| x.as_str()) {
                            // Si el receptor murió (UI cerrada), dejamos de trabajar.
                            if tx.send(Frame::Token(val.to_string())).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    Some("error") => {
                        let m = v
                            .get("message")
                            .and_then(|x| x.as_str())
                            .unwrap_or("stream error")
                            .to_string();
                        let _ = tx.send(Frame::Error(m)).await;
                        return Ok(());
                    }
                    Some("done") => {
                        let _ = tx.send(Frame::Done).await;
                        return Ok(());
                    }
                    _ => {}
                }
            }
        }
    }

    // El stream terminó sin un frame "done" explícito.
    let _ = tx.send(Frame::Done).await;
    Ok(())
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}
