/**
 * IPC Server — JSON-RPC 2.0 over stdio
 *
 * Provides communication between the Tauri Rust shell and the
 * Node.js sidecar process. Messages are newline-delimited JSON (NDJSON).
 *
 * Protocol:
 *   Request:  { "jsonrpc": "2.0", "method": "sync.start", "params": {...}, "id": 1 }
 *   Response: { "jsonrpc": "2.0", "result": {...}, "id": 1 }
 *   Event:    { "jsonrpc": "2.0", "method": "event.fileChanged", "params": {...} }
 */

import * as readline from 'readline';

export interface IpcRequest {
  jsonrpc: '2.0';
  method: string;
  params?: Record<string, unknown>;
  id: number | string;
}

export interface IpcResponse {
  jsonrpc: '2.0';
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
  id: number | string;
}

export interface IpcNotification {
  jsonrpc: '2.0';
  method: string;
  params?: Record<string, unknown>;
}

export type IpcHandler = (
  params: Record<string, unknown>
) => Promise<unknown> | unknown;

const handlers = new Map<string, IpcHandler>();

/**
 * Send a JSON-RPC response to stdout.
 */
function sendResponse(response: IpcResponse): void {
  process.stdout.write(JSON.stringify(response) + '\n');
}

/**
 * Custom event sender (used in dev mode to route events through WebSocket instead of stdout).
 */
let customEventSender: ((method: string, params?: Record<string, unknown>) => void) | null = null;

/**
 * Override the default event sender (stdout).
 * Call with null to restore default behavior.
 */
export function setEventSender(sender: ((method: string, params?: Record<string, unknown>) => void) | null): void {
  customEventSender = sender;
}

/**
 * Send a JSON-RPC notification (event).
 * By default writes to stdout. Can be overridden with setEventSender().
 */
export function sendEvent(method: string, params?: Record<string, unknown>): void {
  const notification: IpcNotification = {
    jsonrpc: '2.0',
    method,
    params,
  };
  if (customEventSender) {
    customEventSender(method, params);
  } else {
    process.stdout.write(JSON.stringify(notification) + '\n');
  }
}

/**
 * Register a handler for a JSON-RPC method.
 */
export function registerHandler(method: string, handler: IpcHandler): void {
  handlers.set(method, handler);
}

/**
 * Get a registered handler by method name.
 * Used by the WebSocket dev server in the sidecar.
 */
export function getHandler(method: string): IpcHandler | undefined {
  return handlers.get(method);
}

/**
 * Process an incoming JSON-RPC request.
 */
async function processRequest(request: IpcRequest): Promise<void> {
  const handler = handlers.get(request.method);

  if (!handler) {
    sendResponse({
      jsonrpc: '2.0',
      error: { code: -32601, message: `Method not found: ${request.method}` },
      id: request.id,
    });
    return;
  }

  try {
    const result = await handler(request.params || {});
    sendResponse({
      jsonrpc: '2.0',
      result,
      id: request.id,
    });
  } catch (err) {
    sendResponse({
      jsonrpc: '2.0',
      error: {
        code: -32000,
        message: err instanceof Error ? err.message : 'Internal error',
        data: err instanceof Error ? err.stack : undefined,
      },
      id: request.id,
    });
  }
}

/**
 * Start the IPC server — reads JSON-RPC requests from stdin,
 * dispatches to registered handlers, writes responses to stdout.
 *
 * Logs go to stderr to avoid mixing with IPC protocol on stdout.
 */
export function startIpcServer(): void {
  const rl = readline.createInterface({
    input: process.stdin,
    terminal: false,
  });

  // Redirect console.log to stderr so it doesn't interfere with IPC
  const originalLog = console.log;
  console.log = (...args: unknown[]) => {
    process.stderr.write(args.map(String).join(' ') + '\n');
  };
  console.warn = console.log;

  rl.on('line', async (line: string) => {
    const trimmed = line.trim();
    if (!trimmed) return;

    try {
      const request = JSON.parse(trimmed) as IpcRequest;

      if (request.jsonrpc !== '2.0' || !request.method) {
        sendResponse({
          jsonrpc: '2.0',
          error: { code: -32600, message: 'Invalid JSON-RPC request' },
          id: request.id || 0,
        });
        return;
      }

      // Notifications (no id) don't get responses
      if (request.id === undefined || request.id === null) {
        const handler = handlers.get(request.method);
        if (handler) {
          try {
            await handler(request.params || {});
          } catch {
            // Notifications don't get error responses
          }
        }
        return;
      }

      await processRequest(request);
    } catch {
      sendResponse({
        jsonrpc: '2.0',
        error: { code: -32700, message: 'Parse error — invalid JSON' },
        id: 0,
      });
    }
  });

  rl.on('close', () => {
    process.exit(0);
  });

  // Signal readiness
  sendEvent('event.ready', { version: '0.1.0' });
}
