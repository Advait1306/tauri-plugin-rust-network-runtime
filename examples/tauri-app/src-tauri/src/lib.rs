use std::{
    convert::Infallible,
    net::{SocketAddr, TcpListener},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use axum::{
    body::Bytes,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Json, Query, State,
    },
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::get,
    Router,
};
use futures_util::{stream::Stream, SinkExt};
use serde::{Deserialize, Serialize};
use tauri::Manager;

#[derive(Clone)]
struct DemoServer {
    origin: String,
}

#[derive(Clone, Default)]
struct DemoAppState {
    beacons: Arc<Mutex<Vec<BeaconRecord>>>,
}

#[derive(Debug, Deserialize)]
struct DemoQuery {
    from: Option<String>,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DemoPostBody {
    from: Option<String>,
    payload: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DemoResponse {
    routed_by: &'static str,
    method: &'static str,
    path: &'static str,
    message: Option<String>,
    payload: Option<String>,
    header_count: usize,
    observed_headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct BeaconRecord {
    routed_by: &'static str,
    path: &'static str,
    content_type: Option<String>,
    payload: String,
    observed_headers: Vec<(String, String)>,
}

#[tauri::command]
fn demo_server_origin(server: tauri::State<'_, DemoServer>) -> String {
    server.origin.clone()
}

async fn inspect_get(Query(query): Query<DemoQuery>, headers: HeaderMap) -> Json<DemoResponse> {
    Json(DemoResponse {
        routed_by: "Rust plugin runtime -> Rust demo server",
        method: "GET",
        path: "/api/inspect",
        message: query.message.or(query.from),
        payload: None,
        header_count: headers.len(),
        observed_headers: collect_headers(&headers),
    })
}

async fn inspect_post(headers: HeaderMap, Json(body): Json<DemoPostBody>) -> Json<DemoResponse> {
    Json(DemoResponse {
        routed_by: "Rust plugin runtime -> Rust demo server",
        method: "POST",
        path: "/api/inspect",
        message: body.from,
        payload: body.payload,
        header_count: headers.len(),
        observed_headers: collect_headers(&headers),
    })
}

async fn websocket_upgrade(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_websocket)
}

async fn handle_websocket(mut socket: WebSocket) {
    let greeting = serde_json::json!({
        "routedBy": "Rust plugin runtime -> Rust demo server",
        "phase": "open",
        "message": "websocket-connected"
    });
    if socket
        .send(Message::Text(greeting.to_string().into()))
        .await
        .is_err()
    {
        return;
    }

    while let Some(result) = socket.recv().await {
        match result {
            Ok(Message::Text(text)) => {
                let payload = serde_json::json!({
                    "routedBy": "Rust plugin runtime -> Rust demo server",
                    "phase": "echo",
                    "message": text.to_string()
                });
                let _ = socket.send(Message::Text(payload.to_string().into())).await;
                let _ = socket.close().await;
                break;
            }
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }
}

async fn sse_stream() -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = async_stream::stream! {
        yield Ok(Event::default().data(
            serde_json::json!({
                "routedBy": "Rust plugin runtime -> Rust demo server",
                "phase": "open",
                "message": "sse-connected"
            }).to_string()
        ));
        tokio::time::sleep(Duration::from_millis(150)).await;
        yield Ok(Event::default().data(
            serde_json::json!({
                "routedBy": "Rust plugin runtime -> Rust demo server",
                "phase": "message",
                "message": "tick-1"
            }).to_string()
        ));
        tokio::time::sleep(Duration::from_millis(150)).await;
        yield Ok(Event::default().event("demo").data(
            serde_json::json!({
                "routedBy": "Rust plugin runtime -> Rust demo server",
                "phase": "named",
                "message": "custom-event"
            }).to_string()
        ));
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn beacon_post(
    State(state): State<DemoAppState>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let record = BeaconRecord {
        routed_by: "Rust plugin runtime -> Rust demo server",
        path: "/beacon",
        content_type: headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
        payload: String::from_utf8_lossy(&body).into_owned(),
        observed_headers: collect_headers(&headers),
    };

    let mut beacons = state.beacons.lock().expect("beacon log poisoned");
    beacons.push(record);
    if beacons.len() > 10 {
        beacons.remove(0);
    }

    StatusCode::NO_CONTENT
}

async fn beacon_log(State(state): State<DemoAppState>) -> Json<Option<BeaconRecord>> {
    let latest = state
        .beacons
        .lock()
        .expect("beacon log poisoned")
        .last()
        .cloned();
    Json(latest)
}

fn collect_headers(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value.to_str().unwrap_or("<non-utf8>").to_string(),
            )
        })
        .collect()
}

fn demo_router(state: DemoAppState) -> Router {
    Router::new()
        .route("/api/inspect", get(inspect_get).post(inspect_post))
        .route("/ws", get(websocket_upgrade))
        .route("/sse", get(sse_stream))
        .route("/beacon", axum::routing::post(beacon_post))
        .route("/api/beacon-log", get(beacon_log))
        .route(
            "/api/health",
            get(|| async { Json(serde_json::json!({ "ok": true })) }),
        )
        .with_state(state)
}

fn bind_demo_server() -> Result<(SocketAddr, TcpListener), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    listener.set_nonblocking(true)?;
    let addr = listener.local_addr()?;
    Ok((addr, listener))
}

fn serve_demo_server(listener: TcpListener) {
    let state = DemoAppState::default();
    thread::spawn(move || {
        let thread_start = Instant::now();
        eprintln!("[startup] demo server thread spawned");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("failed to build demo tokio runtime");
        eprintln!(
            "[startup] demo tokio runtime built in {}ms",
            thread_start.elapsed().as_millis()
        );

        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener)
                .expect("failed to attach demo server listener to tokio");
            eprintln!(
                "[startup] demo server attached to tokio in {}ms",
                thread_start.elapsed().as_millis()
            );
            if let Err(error) = axum::serve(listener, demo_router(state)).await {
                eprintln!("demo server exited: {error}");
            }
        });
    });
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let startup = Instant::now();
    eprintln!("[startup] example run() entered");
    let (demo_server_addr, demo_listener) = bind_demo_server().expect("failed to bind demo server");
    eprintln!(
        "[startup] demo server bound to {demo_server_addr} in {}ms",
        startup.elapsed().as_millis()
    );
    let demo_origin = format!("http://{demo_server_addr}");

    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![demo_server_origin])
        .setup(move |app| {
            eprintln!(
                "[startup] tauri setup entered at {}ms",
                startup.elapsed().as_millis()
            );
            if let Some(window) = app.get_webview_window("main") {
                window.open_devtools();
                eprintln!(
                    "[startup] devtools opened at {}ms",
                    startup.elapsed().as_millis()
                );
            }
            app.manage(DemoServer {
                origin: demo_origin.clone(),
            });
            serve_demo_server(demo_listener);
            eprintln!(
                "[startup] tauri setup finished at {}ms",
                startup.elapsed().as_millis()
            );
            Ok(())
        })
        .plugin(tauri_plugin_rust_network_runtime::init())
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
