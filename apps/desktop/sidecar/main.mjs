#!/usr/bin/env node

/**
 * Universal BI Desktop Agent — Node.js Sidecar
 *
 * Runs as a child process of the Tauri app, communicating via JSON-RPC 2.0.
 *
 * In development mode, also starts a WebSocket server on port 9321
 * so the Vite dev server (browser) can communicate without Tauri.
 */

import { createRequire } from 'module';
import { WebSocketServer } from 'ws';

// Use require() for CommonJS agent-core
const require = createRequire(import.meta.url);

let agentCore;
try {
  agentCore = require('@universal-bi/agent-core');
} catch {
  // Fallback to direct path in dev
  agentCore = require('../../packages/agent-core/dist/index.js');
}

const {
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
  getStateSummary,
  syncDirectory,
  uploadFile,
  getCachedConcepts,
  cacheConcepts,
} = agentCore;

// Wire up watcher events to IPC so they reach the Tauri UI
setWatcherEventCallback((event, data) => {
  sendEvent(event, data);
});

// ─── Register JSON-RPC Handlers ────────────────────────────────────

// Config handlers
registerHandler('config.get', async () => {
  const config = loadConfig();
  return config;
});

registerHandler('config.set', async (params) => {
  const config = loadConfig();
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
    lastSynced: info.lastSynced || null,
    status: info.error ? 'error' : info.lastSynced ? 'synced' : 'pending',
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
  for (const folder of config.watchFolders || []) {
    await syncDirectory(folder.path, config);
  }
  return { ok: true };
});

registerHandler('sync.importFile', async (params) => {
  const config = loadConfig();
  if (!config.platformUrl || !config.apiKey) {
    return { ok: false, error: 'Platform URL and API key must be configured first' };
  }
  const result = await uploadFile(params.path, config);
  if (result.success) {
    sendEvent('event.log', {
      level: 'info',
      message: `Imported: ${params.name || params.path}`,
      timestamp: new Date().toISOString(),
    });
  } else {
    sendEvent('event.log', {
      level: 'error',
      message: `Import failed: ${params.name || params.path} — ${result.error}`,
      timestamp: new Date().toISOString(),
    });
  }
  return result;
});

// Schema handlers
registerHandler('schema.getConcepts', async () => {
  return getCachedConcepts();
});

registerHandler('schema.cacheConcepts', async (params) => {
  cacheConcepts(params.concepts);
  return { ok: true };
});

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
