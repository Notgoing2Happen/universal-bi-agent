#!/usr/bin/env node

/**
 * Universal BI Desktop Agent — Node.js Sidecar
 *
 * Runs as a child process of the Tauri app, communicating via JSON-RPC 2.0.
 *
 * In development mode, also starts a WebSocket server on port 9321
 * so the Vite dev server (browser) can communicate without Tauri.
 */

import { AGENT_VERSION } from './version.mjs';

// Phase 1 follow-up (2026-06-07): propagate AGENT_VERSION to the agent-core
// imports BEFORE they execute. agent-core's query-server.ts (/health) and
// ipc-server.ts (event.ready) read process.env.AGENT_VERSION via the
// canonical getAgentVersion() helper. Must be set before the import below
// touches anything that depends on it. See packages/agent-core/src/version.ts.
process.env.AGENT_VERSION = AGENT_VERSION;

import { WebSocketServer } from 'ws';
import {
  startQueryServer,
  stopQueryServer,
  startIpcServer,
  registerHandler,
  getHandler,
  sendEvent,
  setEventSender,
  loadConfig,
  saveConfig,
  initConfig,
  addWatchFolder,
  removeWatchFolder,
  startWatching,
  stopWatching,
  setWatcherEventCallback,
  loadState,
  saveState,
  getStateSummary,
  syncDirectory,
  uploadFile,
  getCachedConcepts,
  cacheConcepts,
} from '@universal-bi/agent-core';

// Wire up watcher events to IPC so they reach the Tauri UI
setWatcherEventCallback((event, data) => {
  sendEvent(event, data);
});

// ─── Register JSON-RPC Handlers ────────────────────────────────────

// Config handlers
registerHandler('config.get', async () => {
  return loadConfig() || { platformUrl: '', apiKey: '', watchFolders: [] };
});

registerHandler('config.set', async (params) => {
  const config = loadConfig() || { platformUrl: '', apiKey: '', watchFolders: [] };
  if (params.platformUrl !== undefined) config.platformUrl = params.platformUrl;
  if (params.apiKey !== undefined) config.apiKey = params.apiKey;
  if (params.googleAiKey !== undefined) config.googleAiKey = params.googleAiKey;
  if (params.saveFolder !== undefined) config.saveFolder = params.saveFolder;
  saveConfig(config);
  return { ok: true };
});

registerHandler('config.init', async (params) => {
  initConfig(params.platformUrl, params.apiKey);
  return { ok: true };
});

registerHandler('config.test', async () => {
  const config = loadConfig();
  if (!config.platformUrl || !config.apiKey) {
    return { ok: false, message: 'Platform URL and API key are required' };
  }
  try {
    const res = await fetch(`${config.platformUrl}/api/agent/health`, {
      headers: { Authorization: `Bearer ${config.apiKey}` },
    });
    if (res.ok) {
      return { ok: true, message: 'Connected' };
    }
    return { ok: false, message: `HTTP ${res.status}` };
  } catch (err) {
    return { ok: false, message: err.message };
  }
});

// Folder handlers
registerHandler('folders.list', async () => {
  const config = loadConfig();
  return config.watchFolders || [];
});

registerHandler('folders.add', async (params) => {
  addWatchFolder(params.path, params.extensions, params.recursive);
  return { ok: true };
});

registerHandler('folders.remove', async (params) => {
  removeWatchFolder(params.path);
  return { ok: true };
});

// Sync handlers
let isWatching = false;

registerHandler('sync.status', async () => {
  const state = loadState();
  const summary = getStateSummary();
  const files = Object.entries(state.files || {}).map(([path, info]) => ({
    path,
    size: info.size || 0,
    lastSynced: info.lastSyncedAt || null,
    status: info.error ? 'error' : info.lastSyncedAt ? 'synced' : 'pending',
  }));
  return {
    watching: isWatching,
    files,
    totalFiles: summary.total,
    syncedFiles: summary.synced,
    errorFiles: summary.errors,
  };
});

registerHandler('sync.start', async () => {
  startWatching();
  isWatching = true;
  return { ok: true };
});

registerHandler('sync.stop', async () => {
  stopWatching();
  isWatching = false;
  return { ok: true };
});

registerHandler('sync.oneShot', async () => {
  const config = loadConfig();

  // Sync watch folders
  for (const folder of config.watchFolders || []) {
    await syncDirectory(folder.path, config);
  }

  // Retry any individually imported files that are still pending
  const state = loadState();
  let retried = 0;
  for (const [filePath, info] of Object.entries(state.files || {})) {
    if (!info.lastSyncedAt) {
      console.error(`[Sidecar] Retrying pending file: ${filePath}`);
      const result = await uploadFile(filePath, config);
      if (result.success) {
        retried++;
        sendEvent('event.syncProgress', {
          name: filePath.split(/[/\\]/).pop() || filePath,
          stage: result.unchanged ? 'unchanged' : 'synced',
        });
      } else {
        sendEvent('event.syncProgress', {
          name: filePath.split(/[/\\]/).pop() || filePath,
          stage: 'error',
          error: result.error,
        });
      }
    }
  }

  return { ok: true, watchFolders: (config.watchFolders || []).length, retriedPending: retried };
});

registerHandler('sync.importFile', async (params) => {
  const config = loadConfig();
  if (!config.platformUrl || !config.apiKey) {
    return { success: false, error: 'Platform URL and API key must be configured first' };
  }

  // Record file in state immediately (even before upload) so it persists
  // across tab switches and can be retried via "Sync Now"
  const fs = require('fs');
  const path = require('path');
  const resolved = path.resolve(params.path);
  const state = loadState();
  if (!state.files[resolved]) {
    state.files[resolved] = {
      hash: '',
      connectionId: null,
      lastSyncedAt: '',  // empty = pending
      size: fs.existsSync(resolved) ? fs.statSync(resolved).size : 0,
    };
    saveState(state);
  }

  const result = await uploadFile(params.path, config);
  if (result.success) {
    sendEvent('event.log', {
      level: 'info',
      message: `Imported: ${params.name || params.path}`,
      timestamp: new Date().toISOString(),
    });
    sendEvent('event.syncProgress', {
      name: params.name || params.path,
      stage: result.unchanged ? 'unchanged' : 'synced',
    });
  } else {
    sendEvent('event.log', {
      level: 'error',
      message: `Import failed: ${params.name || params.path} — ${result.error}`,
      timestamp: new Date().toISOString(),
    });
    sendEvent('event.syncProgress', {
      name: params.name || params.path,
      stage: 'error',
      error: result.error,
    });
  }
  return result;
});

// File management handlers
registerHandler('files.remove', async (params) => {
  const path = require('path');
  const resolved = path.resolve(params.path);

  // Notify server to deactivate the connection
  const config = loadConfig();
  if (config.platformUrl && config.apiKey) {
    try {
      await fetch(`${config.platformUrl}/api/agent/sync`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${config.apiKey}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ fileName: path.basename(resolved), agentFilePath: resolved, deleted: true }),
        signal: AbortSignal.timeout(10000),
      });
    } catch {
      // Server notification failed — remove locally anyway
    }
  }

  // Remove from local state
  const state = loadState();
  delete state.files[resolved];
  saveState(state);

  sendEvent('event.log', {
    level: 'info',
    message: `Removed: ${path.basename(resolved)}`,
    timestamp: new Date().toISOString(),
  });

  return { ok: true, message: `Removed ${path.basename(resolved)}` };
});

// Untrack AND physically delete the file from disk.
// Use when the user wants to clean up a watcher folder. Server-side
// connection is also deactivated. The file watcher's own delete event
// will fire afterward, but state is already cleaned so it's a no-op.
registerHandler('files.delete', async (params) => {
  const path = require('path');
  const fs = require('fs');
  const resolved = path.resolve(params.path);
  const baseName = path.basename(resolved);

  // 1. Notify server to deactivate the connection (best-effort)
  const config = loadConfig();
  if (config.platformUrl && config.apiKey) {
    try {
      await fetch(`${config.platformUrl}/api/agent/sync`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${config.apiKey}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ fileName: baseName, agentFilePath: resolved, deleted: true }),
        signal: AbortSignal.timeout(10000),
      });
    } catch {
      // Server notification failed — proceed with local delete anyway
    }
  }

  // 2. Remove from local tracking state (do this BEFORE unlink so the
  // watcher's delete event can't race a re-sync attempt)
  const state = loadState();
  delete state.files[resolved];
  saveState(state);

  // 3. Physically delete the file from disk
  let deleted = false;
  let deleteError = null;
  try {
    if (fs.existsSync(resolved)) {
      fs.unlinkSync(resolved);
      deleted = true;
    }
  } catch (err) {
    deleteError = err instanceof Error ? err.message : String(err);
  }

  sendEvent('event.log', {
    level: deleteError ? 'error' : 'info',
    message: deleteError
      ? `Deleted from agent (file delete failed: ${deleteError}): ${baseName}`
      : `Deleted: ${baseName}`,
    timestamp: new Date().toISOString(),
  });

  if (deleteError) {
    return { ok: false, error: deleteError, message: `Untracked but couldn't delete file: ${deleteError}` };
  }
  return { ok: true, deleted, message: `Deleted ${baseName}` };
});

// Schema handlers
registerHandler('schema.getConcepts', async () => {
  return getCachedConcepts();
});

registerHandler('schema.cacheConcepts', async (params) => {
  cacheConcepts(params.concepts);
  return { ok: true };
});

// ─── Query Server (serves local file data to platform) ───────────────

const QUERY_SERVER_PORT = 9322;

// Start query server automatically — platform can reach it at localhost:9322
startQueryServer(QUERY_SERVER_PORT).then(() => {
  console.error(`[Sidecar] Query server started on port ${QUERY_SERVER_PORT}`);

  // Register the query server URL with the platform so Cube.js knows where to reach us
  const config = loadConfig();
  if (config.platformUrl && config.apiKey) {
    fetch(`${config.platformUrl}/api/agent/register`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${config.apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        queryServerUrl: `http://localhost:${QUERY_SERVER_PORT}`,
        // Phase 1 follow-up (2026-06-07): was hardcoded '0.1.19' from
        // commit 8855a6f (2026-01-19) and never bumped across 14 releases —
        // the user's reported "platform shows v0.1.19 forever" bug.
        // Reads from the canonical sidecar/version.mjs which CI rewrites
        // from the pushed git tag at build time. Class-shape fix: ONE
        // version source, every reader imports from it.
        agentVersion: AGENT_VERSION,
        // Phase 1 agent aggregation pushdown capability advertisement.
        // Informational for the platform today (the driver learns capability
        // from the per-query response envelope); stored for future pre-flight
        // gating. Bump pushdownContractVersion only on a wire-breaking change.
        capabilities: {
          supportsPushdown: true,
          pushdownContractVersion: 1,
          supportsChunkedResponse: true,
        },
      }),
    }).catch(err => {
      console.error('[Sidecar] Failed to register query server with platform:', err.message);
    });
  }
}).catch(err => {
  console.error('[Sidecar] Query server failed to start:', err.message);
});

registerHandler('query.status', async () => {
  return { running: true, port: QUERY_SERVER_PORT, url: `http://localhost:${QUERY_SERVER_PORT}` };
});

// ─── Query Relay (SSE primary, polling fallback) ────────────────────
// Two transports to receive queries from the platform:
//
// PRIMARY: Server-Sent Events (SSE) — long-lived stream from
// /api/agent/queries/stream. Platform pushes a `query` event the
// moment submitAgentQuery is called. Latency: ~10-50ms (network RTT)
// vs the old 2s polling floor. Combined with the parallel processing
// below, a 10-chart dashboard goes from ~20s to ~1-2s wall-clock.
//
// FALLBACK: Polling — keeps the old 2s loop for the case where the
// SSE connection drops between heartbeats. Bumped to 30s to be
// gentler on the server now that SSE handles the hot path. The
// platform tracks both as heartbeats (recordAgentPoll fires on both
// SSE-connect/heartbeat AND each poll), so the F6/F8 offline detector
// works no matter which transport is live.
//
// 2026-06-09: this replaces the prior 2s polling loop. The processing
// helper is extracted so SSE + polling share the same parallel-batched
// execution path.

/**
 * Process a single relay query: read local file, POST result back.
 * Called from both SSE handler (one at a time, no queue) and the
 * polling loop (in parallel via Promise.allSettled).
 */
async function processRelayQuery(query, config) {
  try {
    const isSequenceRegion = query.extra?.type === 'sequence-region';
    const endpoint = isSequenceRegion ? 'sequence-region' : 'query';
    const requestBody = isSequenceRegion
      ? {
          connectionId: query.connectionId,
          sampleName: query.extra.sampleName,
          start: query.extra.start,
          end: query.extra.end,
          sequenceColumn: query.extra.sequenceColumn,
        }
      : {
          connectionId: query.connectionId,
          filePath: query.filePath,
          columns: query.columns,
          filters: query.filters,
          limit: query.limit,
          offset: query.offset,                    // chunked raw-path paging window
          aggregationSpec: query.aggregationSpec,   // Phase 1 agent aggregation pushdown spec
        };

    console.error(`[Sidecar] Processing relay ${endpoint} query ${query.id}`);

    const queryRes = await fetch(`http://localhost:${QUERY_SERVER_PORT}/${endpoint}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${config.apiKey}`,
      },
      body: JSON.stringify(requestBody),
    });

    let resultBody;
    if (queryRes.ok) {
      const result = await queryRes.json();
      // Forward the FULL envelope (data + pushdown fields aggregationApplied /
      // agentVersion / pushdownContractVersion / _diag) so the platform driver
      // can read them. Stripping to { data } would silently disable pushdown.
      resultBody = isSequenceRegion ? result : { ...result, data: result.data || [] };
    } else {
      resultBody = { error: `Local query failed: ${queryRes.status}` };
    }

    await fetch(`${config.platformUrl}/api/agent/queries/${query.id}/result`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${config.apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(resultBody),
    });

    console.error(`[Sidecar] Relay query ${query.id} completed`);
  } catch (queryErr) {
    console.error(`[Sidecar] Relay query ${query.id} failed:`, queryErr.message);
    try {
      await fetch(`${config.platformUrl}/api/agent/queries/${query.id}/result`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${config.apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ error: queryErr.message }),
      });
    } catch { /* ignore */ }
  }
}

// Tracks query IDs we've started processing. SSE may deliver a query,
// and a concurrent polling pass may also pick it up before the SSE
// processor has POSTed the result — without dedup we'd process twice.
// Cleared opportunistically (entries older than 60s are pruned).
const inFlightQueryIds = new Map(); // id → startedAt
function shouldProcessQuery(queryId) {
  // Prune entries older than 60s on every check (cheap, keeps the Map small)
  const now = Date.now();
  for (const [id, ts] of inFlightQueryIds) {
    if (now - ts > 60_000) inFlightQueryIds.delete(id);
  }
  if (inFlightQueryIds.has(queryId)) return false;
  inFlightQueryIds.set(queryId, now);
  return true;
}

let queryRelayActive = false;

/**
 * SSE stream — primary transport. Opens a long-lived fetch streaming
 * connection to /api/agent/queries/stream and parses events.
 *
 * Reconnects automatically with exponential backoff on disconnect.
 * Node 20+ has native streaming `fetch` — no `eventsource` dependency
 * needed (important for SEA packaging).
 */
async function startQuerySseStream() {
  const config = loadConfig();
  if (!config?.platformUrl || !config?.apiKey) {
    console.error('[Sidecar] SSE stream skipped — no platform URL or API key');
    return;
  }

  let backoffMs = 1000;
  const MAX_BACKOFF_MS = 30_000;

  while (queryRelayActive) {
    let aborted = false;
    const abortCtrl = new AbortController();
    try {
      console.error('[Sidecar] Opening SSE stream → /api/agent/queries/stream');
      const res = await fetch(`${config.platformUrl}/api/agent/queries/stream`, {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${config.apiKey}`,
          'Accept': 'text/event-stream',
        },
        signal: abortCtrl.signal,
        // Note: NO AbortSignal.timeout — this is a long-lived stream.
      });

      if (!res.ok || !res.body) {
        // Server responded with non-200 — probably old platform without
        // SSE endpoint. Fall back to polling-only mode until next retry.
        console.error(
          `[Sidecar] SSE stream rejected (status=${res.status}). ` +
          `Server may not support SSE yet; polling fallback will continue.`,
        );
        await new Promise(r => setTimeout(r, backoffMs));
        backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
        continue;
      }

      console.error('[Sidecar] SSE stream OPEN — backoff reset');
      backoffMs = 1000; // reset on successful open

      // Read the stream as text and accumulate into an event buffer.
      // SSE wire format: `event: <type>\ndata: <json>\n\n` (records
      // separated by blank line).
      const reader = res.body.getReader();
      const decoder = new TextDecoder('utf-8');
      let buffer = '';

      while (queryRelayActive) {
        const { done, value } = await reader.read();
        if (done) {
          console.error('[Sidecar] SSE stream closed by server — will reconnect');
          break;
        }
        buffer += decoder.decode(value, { stream: true });

        // Process complete events (terminated by \n\n)
        let sep;
        while ((sep = buffer.indexOf('\n\n')) >= 0) {
          const rawEvent = buffer.slice(0, sep);
          buffer = buffer.slice(sep + 2);
          if (!rawEvent.trim()) continue;

          // Parse `event:` + `data:` lines
          let eventType = 'message';
          let dataLines = [];
          for (const line of rawEvent.split('\n')) {
            if (line.startsWith('event:')) eventType = line.slice(6).trim();
            else if (line.startsWith('data:')) dataLines.push(line.slice(5).trim());
          }
          const dataStr = dataLines.join('\n');

          if (eventType === 'query' && dataStr) {
            try {
              const query = JSON.parse(dataStr);
              if (shouldProcessQuery(query.id)) {
                // Fire-and-forget; processRelayQuery handles its own errors
                // and POSTs the result back. We don't await here so multiple
                // queries arriving in rapid succession can run in parallel.
                processRelayQuery(query, config);
              } else {
                console.error(`[Sidecar] SSE dropped duplicate query ${query.id} (already in-flight)`);
              }
            } catch (parseErr) {
              console.error('[Sidecar] SSE event parse error:', parseErr.message);
            }
          } else if (eventType === 'connected') {
            console.error('[Sidecar] SSE connected event:', dataStr.substring(0, 100));
          }
          // heartbeat events: no-op, just keep the connection alive
        }
      }
    } catch (err) {
      if (err.name !== 'AbortError') {
        console.error('[Sidecar] SSE stream error:', err.message?.substring(0, 120));
      }
      aborted = err.name === 'AbortError';
    } finally {
      try { abortCtrl.abort(); } catch { /* ignore */ }
    }

    if (!queryRelayActive || aborted) break;

    // Reconnect after backoff
    await new Promise(r => setTimeout(r, backoffMs));
    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
  }
  console.error('[Sidecar] SSE stream loop exited');
}

/**
 * Polling loop — safety-net transport. Catches queries that landed
 * during the gap between an SSE disconnect and reconnect. Bumped from
 * 2s → 30s now that SSE handles the hot path.
 */
async function startQueryPolling() {
  const config = loadConfig();
  if (!config?.platformUrl || !config?.apiKey) {
    console.error('[Sidecar] Query polling skipped — no platform URL or API key');
    return;
  }

  console.error('[Sidecar] Query relay polling started (safety-net, 30s interval)');

  while (queryRelayActive) {
    try {
      const res = await fetch(`${config.platformUrl}/api/agent/queries`, {
        headers: { 'Authorization': `Bearer ${config.apiKey}` },
        signal: AbortSignal.timeout(10000),
      });

      if (res.ok) {
        const { queries } = await res.json();
        const queryList = (queries || []).filter(q => shouldProcessQuery(q.id));

        if (queryList.length > 0) {
          console.error(`[Sidecar] Polling found ${queryList.length} queries SSE missed — processing in parallel`);
          await Promise.allSettled(queryList.map(q => processRelayQuery(q, config)));
        }
      }
    } catch (pollErr) {
      if (!pollErr.message?.includes('abort')) {
        console.error('[Sidecar] Query poll error:', pollErr.message?.substring(0, 60));
      }
    }

    // 30s — safety-net interval. SSE delivers the hot path.
    await new Promise(resolve => setTimeout(resolve, 30_000));
  }
}

// Start both transports after query server is ready
queryRelayActive = true;
startQuerySseStream();
startQueryPolling();

// ─── Start IPC (stdio for Tauri, WebSocket for dev) ─────────────────

const isDev = process.env.NODE_ENV !== 'production';

if (isDev) {
  // Dev mode: start WebSocket server for browser communication
  const wss = new WebSocketServer({ port: 9321 });

  // Override sendEvent to broadcast via WebSocket in dev mode
  setEventSender((method, params) => {
    const notification = JSON.stringify({ jsonrpc: '2.0', method, params });
    for (const client of wss.clients) {
      if (client.readyState === 1) { // WebSocket.OPEN
        client.send(notification);
      }
    }
  });

  wss.on('connection', (ws) => {
    console.error('[Sidecar] WebSocket client connected');

    ws.on('message', async (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.jsonrpc === '2.0' && msg.method && msg.id !== undefined) {
          try {
            // Find and call the handler
            const handler = getHandler(msg.method);
            if (!handler) {
              ws.send(JSON.stringify({
                jsonrpc: '2.0',
                id: msg.id,
                error: { code: -32601, message: `Method not found: ${msg.method}` },
              }));
              return;
            }
            const result = await handler(msg.params || {});
            ws.send(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result }));
          } catch (err) {
            ws.send(JSON.stringify({
              jsonrpc: '2.0',
              id: msg.id,
              error: { code: -32000, message: err.message },
            }));
          }
        }
      } catch {
        // ignore parse errors
      }
    });

    ws.on('close', () => {
      console.error('[Sidecar] WebSocket client disconnected');
    });
  });

  console.error(`[Sidecar] WebSocket server listening on ws://localhost:9321`);
} else {
  // Production mode: use stdio IPC
  startIpcServer();
}

console.error('[Sidecar] Universal BI Agent sidecar started');
