use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use tauri::Manager;

#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;

// Persistent in-process DuckDB engine (Phase 1): a loopback HTTP bridge the Node sidecar POSTs to.
mod duckdb_server;

struct SidecarState {
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    reader: Option<BufReader<ChildStdout>>,
}

#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! Universal BI Agent is running.", name)
}

/// Send a JSON-RPC request to the sidecar and return the response.
/// Uses async + spawn_blocking so it never freezes the WebView.
#[tauri::command]
async fn sidecar_rpc(
    method: String,
    params: serde_json::Value,
    id: u64,
    state: tauri::State<'_, Arc<Mutex<SidecarState>>>,
) -> Result<serde_json::Value, String> {
    let state = state.inner().clone();

    // Run blocking I/O on a separate thread so the WebView stays responsive
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| -> Result<serde_json::Value, String> {
            let mut guard = state.lock().map_err(|e| e.to_string())?;
            let sc = &mut *guard;

            // Check if sidecar process is still alive
            if let Some(child) = sc.child.as_mut() {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        return Err(format!("Sidecar exited with status: {}", status));
                    }
                    Ok(None) => {} // still running
                    Err(e) => {
                        return Err(format!("Failed to check sidecar status: {}", e));
                    }
                }
            } else {
                return Err("Sidecar not running".to_string());
            }

            let stdin = sc.stdin.as_mut().ok_or("No stdin")?;
            let reader = sc.reader.as_mut().ok_or("No stdout reader")?;

            // Build JSON-RPC request
            let request = serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params,
            });

            let mut line = serde_json::to_string(&request).map_err(|e| e.to_string())?;
            line.push('\n');

            stdin.write_all(line.as_bytes()).map_err(|e| e.to_string())?;
            stdin.flush().map_err(|e| e.to_string())?;

            // Read response lines, skipping any notifications (messages without "id").
            //
            // Phase 3b (2026-06-08): chunked-response handling. When the sidecar
            // serializes a response result whose JSON > IPC_CHUNK_THRESHOLD_BYTES
            // (default 1MB), it sends a HEADER with `result._chunked: true,
            // chunkId, totalChunks, totalBytes` followed by N `chunk.frame`
            // notifications (no id). We reassemble inline here so the caller
            // gets a single resolved result.
            //
            // The sidecar emits header + frames CONTIGUOUSLY (no other
            // RPC's frames interleave) so we can switch into "collecting
            // chunks" mode as soon as we see the header and stay there
            // until done.
            loop {
                let mut response_line = String::new();
                match reader.read_line(&mut response_line) {
                    Ok(0) => {
                        return Err("Sidecar closed connection (EOF)".to_string());
                    }
                    Ok(_) => {}
                    Err(e) => {
                        return Err(format!("Read error: {}", e));
                    }
                }

                let trimmed = response_line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let response: serde_json::Value =
                    serde_json::from_str(trimmed).map_err(|e| {
                        format!("Invalid JSON from sidecar: {} (raw: {:?})", e, trimmed)
                    })?;

                // Skip notifications (no "id" field) — these are events like event.ready
                if response.get("id").is_none() {
                    eprintln!("[Tauri] Skipping sidecar notification: {}", trimmed);
                    continue;
                }

                if let Some(error) = response.get("error") {
                    return Err(error.to_string());
                }

                let result = response.get("result").cloned().unwrap_or(serde_json::Value::Null);

                // Phase 3b: chunked-response handling. If the header signals
                // _chunked, read N chunk.frame notifications + reassemble.
                if let Some(chunked_flag) = result.get("_chunked") {
                    if chunked_flag.as_bool() == Some(true) {
                        let chunk_id = result
                            .get("chunkId")
                            .and_then(|v| v.as_str())
                            .ok_or("Chunked response missing chunkId")?
                            .to_string();
                        let total_chunks = result
                            .get("totalChunks")
                            .and_then(|v| v.as_u64())
                            .ok_or("Chunked response missing totalChunks")? as usize;
                        let total_bytes = result
                            .get("totalBytes")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0) as usize;

                        // Pre-allocate buffer to expected size. Frames append
                        // their data slices in order; final reassembled string
                        // is parsed once at the end.
                        let mut buf = String::with_capacity(total_bytes);
                        let mut received_frames = 0usize;

                        while received_frames < total_chunks {
                            let mut frame_line = String::new();
                            match reader.read_line(&mut frame_line) {
                                Ok(0) => {
                                    return Err(format!(
                                        "Sidecar closed connection during chunked transfer (chunkId={}, {}/{} frames)",
                                        chunk_id, received_frames, total_chunks
                                    ));
                                }
                                Ok(_) => {}
                                Err(e) => {
                                    return Err(format!("Read error during chunked transfer: {}", e));
                                }
                            }
                            let frame_trimmed = frame_line.trim();
                            if frame_trimmed.is_empty() {
                                continue;
                            }
                            let frame: serde_json::Value =
                                serde_json::from_str(frame_trimmed).map_err(|e| {
                                    format!(
                                        "Invalid JSON in chunk frame: {} (raw: {:?})",
                                        e, frame_trimmed
                                    )
                                })?;
                            // Only accept chunk.frame notifications matching our
                            // chunkId. Anything else (event.fileChanged etc.)
                            // mid-transfer is skipped — sidecar contract says
                            // chunks ship contiguously, but log unexpected
                            // interleaves for debugging.
                            let method = frame.get("method").and_then(|v| v.as_str()).unwrap_or("");
                            if method != "chunk.frame" {
                                eprintln!(
                                    "[Tauri] Unexpected non-chunk message mid-chunked-transfer (chunkId={}): {}",
                                    chunk_id, frame_trimmed
                                );
                                continue;
                            }
                            let params = frame.get("params").ok_or("chunk.frame missing params")?;
                            let frame_chunk_id = params
                                .get("chunkId")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            if frame_chunk_id != chunk_id {
                                eprintln!(
                                    "[Tauri] chunk.frame for wrong chunkId (expected {}, got {})",
                                    chunk_id, frame_chunk_id
                                );
                                continue;
                            }
                            let data = params
                                .get("data")
                                .and_then(|v| v.as_str())
                                .ok_or("chunk.frame missing data")?;
                            buf.push_str(data);
                            received_frames += 1;
                        }

                        // Reassemble — parse the concatenated payload as the
                        // ORIGINAL result JSON. Caller sees a normal result,
                        // no awareness of the chunking transport.
                        let reassembled: serde_json::Value =
                            serde_json::from_str(&buf).map_err(|e| {
                                format!(
                                    "Failed to parse reassembled chunked response (chunkId={}, bytes={}): {}",
                                    chunk_id,
                                    buf.len(),
                                    e
                                )
                            })?;
                        return Ok(reassembled);
                    }
                }

                return Ok(result);
            }
        })();
        let _ = tx.send(result);
    });

    // Wait for the thread to complete (non-blocking to the WebView)
    rx.recv().map_err(|_| "Sidecar RPC thread died".to_string())?
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_deep_link::init())
        .setup(|app| {
            let window = app.get_webview_window("main").unwrap();
            window.set_title("Universal BI Agent").unwrap();

            // Register deep link scheme for dev mode
            #[cfg(any(target_os = "linux", all(debug_assertions, windows)))]
            {
                use tauri_plugin_deep_link::DeepLinkExt;
                let _ = app.deep_link().register_all();
            }

            // Resolve sidecar binary path
            let resource_dir = app.path().resource_dir()
                .expect("Failed to get resource dir");

            let ext = if cfg!(target_os = "windows") { ".exe" } else { "" };
            let target_triple = if cfg!(target_os = "windows") {
                "x86_64-pc-windows-msvc"
            } else if cfg!(target_os = "macos") && cfg!(target_arch = "aarch64") {
                "aarch64-apple-darwin"
            } else if cfg!(target_os = "macos") {
                "x86_64-apple-darwin"
            } else {
                "x86_64-unknown-linux-gnu"
            };

            // Try multiple paths in order:
            // 1. Production (Tauri strips target triple): {resource}/binaries/sidecar[.exe]
            // 2. Root resource dir: {resource}/sidecar[.exe]
            // 3. Dev mode (with target triple): {resource}/binaries/sidecar-{triple}[.exe]
            let candidates = vec![
                resource_dir.join("binaries").join(format!("universal-bi-sidecar{}", ext)),
                resource_dir.join(format!("universal-bi-sidecar{}", ext)),
                resource_dir.join("binaries").join(format!("universal-bi-sidecar-{}{}", target_triple, ext)),
            ];

            eprintln!("[Tauri] Resource dir: {:?}", resource_dir);
            for (i, p) in candidates.iter().enumerate() {
                eprintln!("[Tauri] Candidate {}: {:?} (exists: {})", i, p, p.exists());
            }

            let sidecar_path = candidates.iter()
                .find(|p| p.exists())
                .cloned()
                .unwrap_or_else(|| candidates[0].clone());

            eprintln!("[Tauri] Starting sidecar: {:?}", sidecar_path);

            // Start the in-process DuckDB loopback engine (persistent-DuckDB Phase 1) BEFORE the
            // sidecar so we can hand it the port. On any failure the sidecar simply won't receive
            // AGENT_DUCKDB_RPC_PORT and transparently falls back to the DuckDB CLI / raw path.
            let duckdb_port: Option<u16> = match duckdb_server::start() {
                Ok(p) => {
                    eprintln!("[Tauri] DuckDB engine on 127.0.0.1:{}", p);
                    Some(p)
                }
                Err(e) => {
                    eprintln!("[Tauri] DuckDB engine failed to start (sidecar will use CLI/raw): {}", e);
                    None
                }
            };

            // Spawn sidecar with stdio pipes for JSON-RPC
            //
            // Phase 0 (2026-06-07): raise V8 heap from the default ~1.5GB to 2GB
            // so large file reads (50MB cap × parsing overhead ≈ 200-400MB resident
            // peak for Excel / nested-JSON) don't OOM-kill the sidecar mid-upload.
            // SCOPE.md Phase 0 rationale: a 50MB Excel parses to ~800MB of in-memory
            // workbook objects; the historical default heap silently OOM'd around
            // 30-40MB Excel files. 2048MB gives ~5× headroom under the 50MB cap
            // and is well under any reasonable user's available system RAM.
            let mut cmd = Command::new(&sidecar_path);
            cmd.env("NODE_ENV", "production")
                .env("NODE_OPTIONS", "--max-old-space-size=2048")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());

            // Hand the in-process DuckDB engine's loopback port to the sidecar (Phase 1 bridge).
            if let Some(p) = duckdb_port {
                cmd.env("AGENT_DUCKDB_RPC_PORT", p.to_string());
            }

            // Enable the self-verify legs (Leg 1 JS-oracle / Leg 2 dirty-cast / Leg 3 realigner)
            // in the sidecar's query path (query-server.ts `selfVerifyOn`). WITHOUT this the
            // sidecar never runs the legs → the platform's self-verify shadow records every shape
            // verified=false → no shape ever proves → the big-file self-verify SERVE (incl. the
            // canonical "cost by vendor" projection) never engages, only the raw path. The legs are
            // still platform-gated: the sidecar runs them ONLY when the platform sends fullProfiles
            // (i.e. AGENT_DUCKDB_V2_SELFVERIFY_SHADOW/_ENABLED is on platform-side), so defaulting
            // this on here is safe. An explicit OS-env value overrides (set =false to disable).
            cmd.env(
                "AGENT_DUCKDB_V2_SELFVERIFY",
                std::env::var("AGENT_DUCKDB_V2_SELFVERIFY").unwrap_or_else(|_| "true".to_string()),
            );

            // On Windows, prevent the sidecar from opening a visible console window.
            // CREATE_NO_WINDOW = 0x08000000
            #[cfg(target_os = "windows")]
            cmd.creation_flags(0x08000000);

            match cmd.spawn()
            {
                Ok(mut child) => {
                    eprintln!("[Tauri] Sidecar started (pid: {})", child.id());

                    // Spawn a thread to log sidecar stderr output
                    if let Some(stderr) = child.stderr.take() {
                        std::thread::spawn(move || {
                            let reader = BufReader::new(stderr);
                            for line in reader.lines() {
                                match line {
                                    Ok(l) => eprintln!("[Sidecar] {}", l),
                                    Err(_) => break,
                                }
                            }
                        });
                    }

                    // Take ownership of stdin/stdout for persistent buffered I/O
                    let stdin = child.stdin.take();
                    let stdout = child.stdout.take();
                    let reader = stdout.map(BufReader::new);

                    app.manage(Arc::new(Mutex::new(SidecarState {
                        child: Some(child),
                        stdin,
                        reader,
                    })));
                }
                Err(e) => {
                    eprintln!("[Tauri] Failed to start sidecar: {}", e);
                    eprintln!("[Tauri] Path tried: {:?}", sidecar_path);
                    // Start without sidecar — UI still works, just no sync
                    app.manage(Arc::new(Mutex::new(SidecarState {
                        child: None,
                        stdin: None,
                        reader: None,
                    })));
                }
            }

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![greet, sidecar_rpc])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
