/**
 * Agent File Watcher
 *
 * Uses chokidar to watch directories for file changes.
 * When a supported file is added or changed, waits for the write to
 * stabilize (stabilityThreshold), then uploads to the platform.
 *
 * Supports an optional event callback for GUI integration (Tauri sidecar).
 */

import * as path from 'path';
import chokidar from 'chokidar';
import { AgentConfig, WatchFolder, loadConfig } from './config';
import { uploadFile, notifyFileDeletion } from './uploader';
import { removeFileState } from './state';

// Track pending uploads to debounce rapid changes
const pendingUploads = new Map<string, NodeJS.Timeout>();

// Track active watchers for cleanup
const activeWatchers: chokidar.FSWatcher[] = [];

/**
 * Optional callback for file change events (used by sidecar to push events to UI).
 */
export type WatcherEventCallback = (event: string, data: Record<string, unknown>) => void;

let eventCallback: WatcherEventCallback | null = null;

/**
 * Register a callback that receives file change events.
 * Used by the sidecar to forward events via IPC to the Tauri UI.
 */
export function setWatcherEventCallback(cb: WatcherEventCallback | null): void {
  eventCallback = cb;
}

function emitEvent(event: string, data: Record<string, unknown>): void {
  if (eventCallback) {
    eventCallback(event, data);
  }
}

/**
 * Start watching all configured folders.
 * If no config is provided, loads from disk.
 */
export function startWatching(config?: AgentConfig): void {
  const loaded = config || loadConfig();
  if (!loaded) {
    console.log('No config found. Run init first.');
    return;
  }
  const cfg: AgentConfig = loaded;

  if (cfg.watchFolders.length === 0) {
    console.log('No watch folders configured. Use: universal-bi-agent add <folder>');
    emitEvent('event.log', {
      level: 'warn',
      message: 'No watch folders configured',
      timestamp: new Date().toISOString(),
    });
    return;
  }

  console.log(`Starting file watcher for ${cfg.watchFolders.length} folder(s)...\n`);

  for (const folder of cfg.watchFolders) {
    watchFolder(folder, cfg);
  }

  // Heartbeat
  if (cfg.heartbeatInterval > 0) {
    const heartbeatTimer = setInterval(async () => {
      try {
        const response = await fetch(`${cfg.platformUrl}/api/health`, {
          headers: { 'Authorization': `Bearer ${cfg.apiKey}` },
        });
        if (!response.ok) {
          console.warn(`[Heartbeat] Platform returned ${response.status}`);
        }
      } catch {
        console.warn('[Heartbeat] Platform unreachable');
      }
    }, cfg.heartbeatInterval);

    heartbeatTimer.unref();
  }

  emitEvent('event.log', {
    level: 'info',
    message: `Watching ${cfg.watchFolders.length} folder(s) for changes`,
    timestamp: new Date().toISOString(),
  });

  console.log('Watching for changes... (Press Ctrl+C to stop)\n');
}

/**
 * Watch a single folder for file changes.
 */
function watchFolder(folder: WatchFolder, config: AgentConfig): void {
  const extensions = folder.extensions.map(ext =>
    ext.startsWith('.') ? ext : `.${ext}`
  );

  // Build glob pattern for chokidar
  const patterns = extensions.map(ext => {
    const glob = folder.recursive ? `**/*${ext}` : `*${ext}`;
    return path.join(folder.path, glob);
  });

  const ignored = folder.ignorePatterns.map(pattern => {
    // Convert simple patterns to regex-like
    if (pattern.startsWith('*.')) return new RegExp(`\\.${pattern.slice(2)}$`);
    if (pattern.startsWith('~')) return /^~/;
    if (pattern.startsWith('.')) return /(^|[/\\])\../; // Hidden files/dirs
    return pattern;
  });

  const watcher = chokidar.watch(patterns, {
    ignored: [
      ...ignored,
      /(^|[/\\])\../, // Always ignore dotfiles
      /node_modules/,
    ],
    persistent: true,
    ignoreInitial: true, // Don't trigger for existing files on start
    awaitWriteFinish: {
      stabilityThreshold: config.stabilityThreshold,
      pollInterval: 100,
    },
  });

  watcher.on('add', (filePath: string) => {
    emitEvent('event.fileChanged', {
      path: filePath,
      name: path.basename(filePath),
      type: 'added',
      timestamp: new Date().toISOString(),
    });
    handleFileChange(filePath, 'added', config);
  });

  watcher.on('change', (filePath: string) => {
    emitEvent('event.fileChanged', {
      path: filePath,
      name: path.basename(filePath),
      type: 'changed',
      timestamp: new Date().toISOString(),
    });
    handleFileChange(filePath, 'changed', config);
  });

  watcher.on('unlink', (filePath: string) => {
    console.log(`[Deleted] ${path.basename(filePath)}`);
    emitEvent('event.fileChanged', {
      path: filePath,
      name: path.basename(filePath),
      type: 'deleted',
      timestamp: new Date().toISOString(),
    });
    // Cancel any pending upload
    const pending = pendingUploads.get(filePath);
    if (pending) {
      clearTimeout(pending);
      pendingUploads.delete(filePath);
    }
    // Notify platform to deactivate the connection
    notifyFileDeletion(filePath, config).then(result => {
      if (result.success) {
        console.log(`  → Connection deactivated for deleted file`);
        emitEvent('event.syncProgress', {
          path: filePath,
          name: path.basename(filePath),
          stage: 'deleted',
          timestamp: new Date().toISOString(),
        });
      } else {
        console.warn(`  → Failed to notify deletion: ${result.error}`);
      }
    }).catch(() => {
      // Remove local state even if server notification fails
      removeFileState(filePath);
    });
  });

  watcher.on('error', (error: Error) => {
    console.error(`[Watcher] Error: ${error.message}`);
    emitEvent('event.log', {
      level: 'error',
      message: `Watcher error: ${error.message}`,
      timestamp: new Date().toISOString(),
    });
  });

  watcher.on('ready', () => {
    console.log(`  Watching: ${folder.path}`);
    console.log(`  Extensions: ${extensions.join(', ')}`);
    console.log(`  Recursive: ${folder.recursive}`);
    console.log('');
  });

  activeWatchers.push(watcher);
}

/**
 * Handle a file change event with debouncing.
 */
function handleFileChange(
  filePath: string,
  eventType: 'added' | 'changed',
  config: AgentConfig
): void {
  // Cancel any existing pending upload for this file
  const existing = pendingUploads.get(filePath);
  if (existing) {
    clearTimeout(existing);
  }

  // Debounce — wait for stability before uploading
  const timeout = setTimeout(async () => {
    pendingUploads.delete(filePath);

    const fileName = path.basename(filePath);
    const timestamp = new Date().toLocaleTimeString();
    console.log(`[${timestamp}] ${eventType}: ${fileName}`);

    emitEvent('event.syncProgress', {
      path: filePath,
      name: fileName,
      stage: 'uploading',
      timestamp: new Date().toISOString(),
    });

    const result = await uploadFile(filePath, config);

    if (result.success) {
      if (result.unchanged) {
        console.log(`  → Unchanged (skipped)`);
        emitEvent('event.syncProgress', {
          path: filePath,
          name: fileName,
          stage: 'unchanged',
          timestamp: new Date().toISOString(),
        });
      } else {
        console.log(
          `  → ${result.isNew ? 'Created' : 'Updated'} connection: ${result.connectionId}`
        );
        emitEvent('event.syncProgress', {
          path: filePath,
          name: fileName,
          stage: 'synced',
          connectionId: result.connectionId,
          isNew: result.isNew,
          timestamp: new Date().toISOString(),
        });
        emitEvent('event.log', {
          level: 'info',
          message: `Synced: ${fileName}${result.isNew ? ' (new)' : ''}`,
          timestamp: new Date().toISOString(),
        });
      }
    } else {
      console.error(`  → Failed: ${result.error}`);
      emitEvent('event.syncProgress', {
        path: filePath,
        name: fileName,
        stage: 'error',
        error: result.error,
        timestamp: new Date().toISOString(),
      });
      emitEvent('event.log', {
        level: 'error',
        message: `Sync failed: ${fileName} — ${result.error}`,
        timestamp: new Date().toISOString(),
      });
    }
  }, 500); // Extra 500ms debounce on top of chokidar's stabilityThreshold

  pendingUploads.set(filePath, timeout);
}

/**
 * Stop all watchers and clean up.
 */
export async function stopWatching(): Promise<void> {
  console.log('\nStopping file watchers...');

  for (const pending of pendingUploads.values()) {
    clearTimeout(pending);
  }
  pendingUploads.clear();

  for (const watcher of activeWatchers) {
    await watcher.close();
  }
  activeWatchers.length = 0;

  emitEvent('event.log', {
    level: 'info',
    message: 'File watching stopped',
    timestamp: new Date().toISOString(),
  });

  console.log('All watchers stopped.');
}
