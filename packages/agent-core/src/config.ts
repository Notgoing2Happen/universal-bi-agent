/**
 * Agent Configuration Manager
 *
 * Manages the agent configuration file at ~/.universal-bi/config.json.
 * Stores platform URL, API key, watch folders, and sync preferences.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

const CONFIG_DIR = path.join(os.homedir(), '.universal-bi');
const CONFIG_FILE = path.join(CONFIG_DIR, 'config.json');

export interface WatchFolder {
  path: string;
  extensions: string[];
  recursive: boolean;
  ignorePatterns: string[];
}

export interface AgentConfig {
  platformUrl: string;
  apiKey: string;
  watchFolders: WatchFolder[];
  /** Google AI API key for local column mapping */
  googleAiKey?: string;
  /** Folder where synced/imported documents are saved */
  saveFolder?: string;
  /** Wait this long (ms) after file write stabilizes before uploading */
  stabilityThreshold: number;
  /** Maximum file size in bytes (default 50MB) */
  maxFileSize: number;
  /** Retry failed uploads up to this many times */
  maxRetries: number;
  /** Base delay (ms) for exponential backoff between retries */
  retryBaseDelay: number;
  /** Heartbeat interval (ms) — how often agent pings the platform */
  heartbeatInterval: number;
}

const DEFAULT_CONFIG: Omit<AgentConfig, 'platformUrl' | 'apiKey'> = {
  watchFolders: [],
  stabilityThreshold: 2000,
  maxFileSize: 50 * 1024 * 1024,
  maxRetries: 3,
  retryBaseDelay: 1000,
  heartbeatInterval: 60000,
};

/**
 * Ensure the config directory exists.
 */
function ensureConfigDir(): void {
  if (!fs.existsSync(CONFIG_DIR)) {
    fs.mkdirSync(CONFIG_DIR, { recursive: true });
  }
}

/**
 * Load the agent configuration. Returns null if not configured.
 */
export function loadConfig(): AgentConfig | null {
  if (!fs.existsSync(CONFIG_FILE)) {
    return null;
  }

  try {
    const raw = fs.readFileSync(CONFIG_FILE, 'utf-8');
    const parsed = JSON.parse(raw);
    return { ...DEFAULT_CONFIG, ...parsed } as AgentConfig;
  } catch (err) {
    console.error('Failed to read config:', err);
    return null;
  }
}

/**
 * Save the agent configuration.
 */
export function saveConfig(config: AgentConfig): void {
  ensureConfigDir();
  fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 2), 'utf-8');
}

/**
 * Initialize configuration with platform URL and API key.
 */
export function initConfig(platformUrl: string, apiKey: string): AgentConfig {
  const config: AgentConfig = {
    ...DEFAULT_CONFIG,
    platformUrl: platformUrl.replace(/\/+$/, ''), // Strip trailing slash
    apiKey,
    watchFolders: [],
  };
  saveConfig(config);
  return config;
}

/**
 * Add a watch folder to the config.
 */
export function addWatchFolder(
  folderPath: string,
  options?: Partial<Pick<WatchFolder, 'extensions' | 'recursive' | 'ignorePatterns'>>
): AgentConfig | null {
  const config = loadConfig();
  if (!config) {
    console.error('Agent not configured. Run: universal-bi-agent init --url <URL> --key <KEY>');
    return null;
  }

  const resolved = path.resolve(folderPath);

  // Check if folder already watched
  if (config.watchFolders.some(f => path.resolve(f.path) === resolved)) {
    console.log(`Folder already watched: ${resolved}`);
    return config;
  }

  config.watchFolders.push({
    path: resolved,
    extensions: options?.extensions || ['.csv', '.xlsx', '.xls', '.json'],
    recursive: options?.recursive ?? true,
    ignorePatterns: options?.ignorePatterns || ['*.tmp', '~*', '.*'],
  });

  saveConfig(config);
  return config;
}

/**
 * Remove a watch folder from the config.
 */
export function removeWatchFolder(folderPath: string): AgentConfig | null {
  const config = loadConfig();
  if (!config) return null;

  const resolved = path.resolve(folderPath);
  config.watchFolders = config.watchFolders.filter(
    f => path.resolve(f.path) !== resolved
  );

  saveConfig(config);
  return config;
}

/**
 * Get the config directory path.
 */
export function getConfigDir(): string {
  return CONFIG_DIR;
}

/**
 * Get the config file path.
 */
export function getConfigFile(): string {
  return CONFIG_FILE;
}
