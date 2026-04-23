/**
 * Universal BI Agent Core Package
 *
 * Shared logic for both the CLI agent and the Tauri desktop app.
 * This package has NO database (Prisma) dependency — it works
 * entirely with local files and network calls.
 *
 * Provides:
 * - Config management for ~/.universal-bi/config.json
 * - Local state tracking for file sync deduplication
 * - HTTP uploader with retry logic
 * - File watcher with chokidar
 * - IPC server for Tauri sidecar communication
 * - Schema cache for local Universal Schema concepts
 * - Data bridge for local file → API migration
 */

// Config management
export {
  loadConfig,
  saveConfig,
  initConfig,
  addWatchFolder,
  removeWatchFolder,
  getConfigDir,
  getConfigFile,
  type AgentConfig,
  type WatchFolder,
} from './config';

// State tracking
export {
  loadState,
  saveState,
  computeFileHash,
  hasFileChanged,
  recordSync,
  removeFileState,
  getFileState,
  getStateSummary,
  type FileState,
  type AgentState,
} from './state';

// Uploader
export {
  uploadFile,
  syncDirectory,
  type UploadResult,
} from './uploader';

// Watcher
export {
  startWatching,
  stopWatching,
  setWatcherEventCallback,
  type WatcherEventCallback,
} from './watcher';

// IPC Server (for Tauri sidecar)
export {
  startIpcServer,
  registerHandler,
  getHandler,
  sendEvent,
  setEventSender,
  type IpcHandler,
  type IpcRequest,
  type IpcResponse,
  type IpcNotification,
} from './ipc-server';

// Schema Cache
export {
  loadSchemaCache,
  saveSchemaCache,
  getCachedConcepts,
  cacheConcepts,
  type SchemaCacheEntry,
} from './schema-cache';

// Query Server (serves local file data to platform)
export {
  startQueryServer,
  stopQueryServer,
} from './query-server';

// Data Bridge (local transform pipeline)
export {
  resolveColumnNames,
  transformRows,
  type BridgePlanItem,
  type ImportTag,
} from './bridge';
