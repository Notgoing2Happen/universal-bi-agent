import { useState, useEffect, useCallback } from 'react';

/**
 * JSON-RPC 2.0 client for communicating with the Node.js sidecar process.
 *
 * In production (Tauri), communicates via Tauri invoke → Rust → stdio to sidecar.
 * In development, connects via WebSocket on port 9321.
 */

type EventHandler = (params: Record<string, unknown>) => void;

let nextId = 1;
const eventHandlers = new Map<string, Set<EventHandler>>();

// Detect if running inside Tauri
const isTauri = typeof window !== 'undefined' && '__TAURI_INTERNALS__' in window;

// ─── WebSocket transport (dev mode) ────────────────────────────────

const pendingRequests = new Map<number, {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
}>();

let ws: WebSocket | null = null;

function handleWsMessage(data: string) {
  try {
    const msg = JSON.parse(data);
    if ('id' in msg && pendingRequests.has(msg.id)) {
      const { resolve, reject } = pendingRequests.get(msg.id)!;
      pendingRequests.delete(msg.id);
      if (msg.error) {
        reject(new Error(msg.error.message));
      } else {
        resolve(msg.result);
      }
    } else if ('method' in msg && !('id' in msg)) {
      const handlers = eventHandlers.get(msg.method);
      if (handlers) {
        handlers.forEach(h => h(msg.params ?? {}));
      }
    }
  } catch {
    // ignore parse errors
  }
}

function connectWebSocket(): Promise<void> {
  return new Promise((resolve, reject) => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      resolve();
      return;
    }
    ws = new WebSocket('ws://localhost:9321');
    ws.onopen = () => {
      resolve();
    };
    ws.onmessage = (event) => handleWsMessage(event.data);
    ws.onclose = () => {
      ws = null;
    };
    ws.onerror = () => {
      reject(new Error('Failed to connect to sidecar'));
    };
  });
}

async function callViaWebSocket(method: string, params?: Record<string, unknown>): Promise<unknown> {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    await connectWebSocket();
  }

  const id = nextId++;
  const request = {
    jsonrpc: '2.0',
    id,
    method,
    ...(params && { params }),
  };

  return new Promise((resolve, reject) => {
    pendingRequests.set(id, { resolve, reject });
    ws!.send(JSON.stringify(request));

    setTimeout(() => {
      if (pendingRequests.has(id)) {
        pendingRequests.delete(id);
        reject(new Error(`Sidecar call ${method} timed out`));
      }
    }, 30_000);
  });
}

// ─── Tauri invoke transport (production) ───────────────────────────

async function callViaTauri(method: string, params?: Record<string, unknown>): Promise<unknown> {
  const { invoke } = await import('@tauri-apps/api/core');
  const id = nextId++;
  return invoke('sidecar_rpc', {
    method,
    params: params ?? {},
    id,
  });
}

// ─── Unified call function ─────────────────────────────────────────

async function callSidecar(method: string, params?: Record<string, unknown>): Promise<unknown> {
  if (isTauri) {
    return callViaTauri(method, params);
  }
  return callViaWebSocket(method, params);
}

// ─── React hook ────────────────────────────────────────────────────

export function useSidecar() {
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (isTauri) {
      // In Tauri, try a ping call to verify sidecar is running
      callViaTauri('config.get', {})
        .then(() => {
          setConnected(true);
          setError(null);
        })
        .catch((err) => {
          // Sidecar may not be running, but app should still render
          setConnected(false);
          setError(err instanceof Error ? err.message : String(err));
        });
    } else {
      // Dev mode: connect via WebSocket
      connectWebSocket()
        .then(() => {
          setConnected(true);
          setError(null);
        })
        .catch(err => {
          setConnected(false);
          setError(err.message);
        });
    }
  }, []);

  const call = useCallback(async <T = unknown>(
    method: string,
    params?: Record<string, unknown>,
  ): Promise<T> => {
    return callSidecar(method, params) as Promise<T>;
  }, []);

  const onEvent = useCallback((event: string, handler: EventHandler) => {
    if (!eventHandlers.has(event)) {
      eventHandlers.set(event, new Set());
    }
    eventHandlers.get(event)!.add(handler);
    return () => {
      eventHandlers.get(event)?.delete(handler);
    };
  }, []);

  return { connected, error, call, onEvent };
}
