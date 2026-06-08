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
 *
 * Phase 3a (2026-06-08, SCOPE.md): added two safety upgrades to the
 * write path. Both are sender-side only — no protocol changes, no
 * Tauri-side reassembly required:
 *
 *   1. Response size cap (IPC_MAX_RESPONSE_BYTES, default 8MB).
 *      Oversized responses are replaced with a structured JSON-RPC error
 *      (code -32001) carrying the original byte count + a hint. The
 *      handler that produced the oversized response is logged to stderr
 *      so devs can localize the offender.
 *
 *   2. Backpressure-aware stdout writes. `process.stdout.write()` returns
 *      `false` when the OS pipe buffer is full; ignoring it means the
 *      sidecar buffers further writes in-process memory while Tauri
 *      slowly drains. Now we await the 'drain' event before sending the
 *      next message, capping in-process buffer growth at one write +
 *      pipe-buffer capacity.
 *
 * Full chunking (split a >8MB response into N <8MB frames + Tauri-side
 * reassembly) is intentionally deferred to Phase 3b — it requires
 * coordinated Rust-side work.
 */

import * as readline from 'readline';
import { getAgentVersion } from './version';

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
 * Phase 3a (2026-06-08): IPC response size cap. Default 8MB — well below
 * the typical OS pipe buffer ceiling concerns AND large enough for any
 * realistic JSON-RPC response shape. Tunable via env for power users
 * (e.g. local dev with huge schemas).
 *
 * Rationale for 8MB: Tauri's Rust BufReader handles arbitrary line
 * lengths, but a single JSON.stringify of a 50MB+ payload pins V8 heap
 * for the duration of the serialize+write. 8MB caps the heap blowup
 * while leaving headroom for any sensible response. Oversized responses
 * indicate a handler bug or scaling issue worth surfacing, not silently
 * tolerating.
 */
const IPC_MAX_RESPONSE_BYTES = Number(process.env.IPC_MAX_RESPONSE_BYTES) || 8 * 1024 * 1024;

/**
 * Phase 3a: drain-aware stdout writes. `process.stdout.write()` returns
 * `false` when the OS pipe buffer is full. Without awaiting drain, the
 * caller continues issuing writes and Node buffers them in-process
 * memory — under sustained pressure (slow consumer, big response
 * stream) this can OOM the sidecar.
 *
 * Sequential serialization: each call awaits the prior write to drain
 * before issuing the next. The handler call site stays synchronous via
 * fire-and-forget; the queue ensures ordering.
 */
let writeChain: Promise<void> = Promise.resolve();

function writeWithBackpressure(line: string): void {
  writeChain = writeChain.then(
    () =>
      new Promise<void>((resolve) => {
        const ok = process.stdout.write(line);
        if (ok) {
          resolve();
        } else {
          // Pipe buffer full — wait for drain before resolving.
          process.stdout.once('drain', () => resolve());
        }
      }),
  );
}

/**
 * Phase 3a: enforce response-size cap before writing. Returns the
 * serialized line (with trailing newline) to actually transmit. Caller
 * fires it via writeWithBackpressure().
 *
 * When the original response exceeds the cap, we substitute a
 * structured JSON-RPC error response (code -32001, "Response too large")
 * carrying the original byte count + a hint. The substitution lets the
 * caller surface the failure honestly rather than crashing the IPC
 * channel or silently dropping data.
 */
function serializeWithSizeCap(
  response: IpcResponse | IpcNotification,
  contextLabel: string,
): string {
  const json = JSON.stringify(response);
  if (json.length <= IPC_MAX_RESPONSE_BYTES) {
    return json + '\n';
  }
  // Oversized. Log to stderr so the offending handler is discoverable.
  // ipc-server.ts is the choke point — even handlers wrapped through
  // setEventSender route through here when no custom sender is set.
  process.stderr.write(
    `[ipc-server] oversized response (${json.length} bytes > ${IPC_MAX_RESPONSE_BYTES} cap) from ` +
      `${contextLabel} — substituting error response. Set IPC_MAX_RESPONSE_BYTES to raise the cap.\n`,
  );
  // Synthesize a JSON-RPC error response. For responses (have `id`), the
  // error replaces the result. For notifications (no `id`), the cap
  // applies but we still send a notification-shape error so Tauri sees
  // something rather than a dropped message.
  if ('id' in response) {
    const errorResp: IpcResponse = {
      jsonrpc: '2.0',
      error: {
        code: -32001,
        message: 'Response too large',
        data: {
          responseBytes: json.length,
          maxBytes: IPC_MAX_RESPONSE_BYTES,
          contextLabel,
          hint:
            'The handler returned a payload that exceeds the IPC channel cap. ' +
            'Narrow the query (use limit, filter, or summary mode), or raise IPC_MAX_RESPONSE_BYTES if you control the deployment.',
        },
      },
      id: (response as IpcResponse).id,
    };
    return JSON.stringify(errorResp) + '\n';
  }
  // Notification — synthesize an oversized-notification event so the
  // receiver at least knows something was dropped.
  const errorNotification: IpcNotification = {
    jsonrpc: '2.0',
    method: 'event.ipcOversized',
    params: {
      originalMethod: (response as IpcNotification).method,
      responseBytes: json.length,
      maxBytes: IPC_MAX_RESPONSE_BYTES,
      contextLabel,
    },
  };
  return JSON.stringify(errorNotification) + '\n';
}

/**
 * Send a JSON-RPC response to stdout.
 *
 * Phase 3a: routes through size-cap + drain-aware write helpers.
 */
function sendResponse(response: IpcResponse): void {
  const line = serializeWithSizeCap(response, `response id=${response.id}`);
  writeWithBackpressure(line);
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
 *
 * Phase 3a (2026-06-08): same size-cap + drain-aware path as
 * sendResponse. The custom event sender (used in dev mode via
 * WebSocket) bypasses these — it has its own framing.
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
    const line = serializeWithSizeCap(notification, `event method=${method}`);
    writeWithBackpressure(line);
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

  // Signal readiness. Phase 1 follow-up (2026-06-07): version no longer
  // hardcoded — reads from process.env.AGENT_VERSION via the canonical
  // version.ts helper. See packages/agent-core/src/version.ts.
  sendEvent('event.ready', { version: getAgentVersion() });
}
