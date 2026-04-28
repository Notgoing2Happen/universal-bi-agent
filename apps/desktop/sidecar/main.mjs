#!/usr/bin/env node

/**
 * Universal BI Desktop Agent — Node.js Sidecar
 *
 * Runs as a child process of the Tauri app, communicating via JSON-RPC 2.0.
 *
 * In development mode, also starts a WebSocket server on port 9321
 * so the Vite dev server (browser) can communicate without Tauri.
 */

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
        agentVersion: '0.1.19',
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

// ─── Query Relay Polling ─────────────────────────────────────────────
// Poll the platform for pending queries that need local file data.
// This bridges the NAT gap: Cube.js on the server can't reach our
// local machine, but we can poll the server for query requests.

let queryPollingActive = false;

async function startQueryPolling() {
  const config = loadConfig();
  if (!config.platformUrl || !config.apiKey) {
    console.error('[Sidecar] Query polling skipped — no platform URL or API key');
    return;
  }

  queryPollingActive = true;
  console.error('[Sidecar] Query relay polling started');

  while (queryPollingActive) {
    try {
      // Poll for pending queries
      const res = await fetch(`${config.platformUrl}/api/agent/queries`, {
        headers: { 'Authorization': `Bearer ${config.apiKey}` },
        signal: AbortSignal.timeout(10000),
      });

      if (res.ok) {
        const { queries } = await res.json();

        for (const query of queries || []) {
          // Process each query locally
          try {
            // Determine endpoint based on query type
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
              // Sequence region returns the full result; data queries return { data: [] }
              resultBody = isSequenceRegion ? result : { data: result.data || [] };
            } else {
              resultBody = { error: `Local query failed: ${queryRes.status}` };
            }

            // Post result back to the platform
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
            // Try to report error back
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
      }
    } catch (pollErr) {
      // Network error — server might be down, try again later
      if (!pollErr.message?.includes('abort')) {
        console.error('[Sidecar] Query poll error:', pollErr.message?.substring(0, 60));
      }
    }

    // Poll every 2 seconds
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
}

// Start polling after query server is ready
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
