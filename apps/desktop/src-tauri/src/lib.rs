use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use tauri::Manager;

#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;

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

            // Read response lines, skipping any notifications (messages without "id")
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

                return Ok(response.get("result").cloned().unwrap_or(serde_json::Value::Null));
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
        .setup(|app| {
            let window = app.get_webview_window("main").unwrap();
            window.set_title("Universal BI Agent").unwrap();

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

            // Spawn sidecar with stdio pipes for JSON-RPC
            let mut cmd = Command::new(&sidecar_path);
            cmd.env("NODE_ENV", "production")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());

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
