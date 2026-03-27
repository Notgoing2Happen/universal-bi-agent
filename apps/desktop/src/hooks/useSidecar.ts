import { useState, useEffect, useCallback } from 'react';

/**
 * JSON-RPC 2.0 client for communicating with the Node.js sidecar process.
 *
 * In development, connects to a local sidecar via WebSocket on port 9321.
 * In production (Tauri), communicates via shell sidecar stdio.
 */

interface JsonRpcRequest {
  jsonrpc: '2.0';
  id: number;
  method: string;
  params?: Record<string, unknown>;
}

type EventHandler = (params: Record<string, unknown>) => void;

let nextId = 1;
const pendingRequests = new Map<number, {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
}>();
const eventHandlers = new Map<string, Set<EventHandler>>();

let sidecarReady = false;
let ws: WebSocket | null = null;

function handleMessage(data: string) {
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
      // Notification/event
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
      sidecarReady = true;
      resolve();
    };
    ws.onmessage = (event) => handleMessage(event.data);
    ws.onclose = () => {
      sidecarReady = false;
      ws = null;
    };
    ws.onerror = () => {
      reject(new Error('Failed to connect to sidecar'));
    };
  });
}

async function callSidecar(method: string, params?: Record<string, unknown>): Promise<unknown> {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    await connectWebSocket();
  }

  const id = nextId++;
  const request: JsonRpcRequest = {
    jsonrpc: '2.0',
    id,
    method,
    ...(params && { params }),
  };

  return new Promise((resolve, reject) => {
    pendingRequests.set(id, { resolve, reject });
    ws!.send(JSON.stringify(request));

    // Timeout after 30 seconds
    setTimeout(() => {
      if (pendingRequests.has(id)) {
        pendingRequests.delete(id);
        reject(new Error(`Sidecar call ${method} timed out`));
      }
    }, 30_000);
  });
}

export function useSidecar() {
  const [connected, setConnected] = useState(sidecarReady);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    connectWebSocket()
      .then(() => {
        setConnected(true);
        setError(null);
      })
      .catch(err => {
        setConnected(false);
        setError(err.message);
      });
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
