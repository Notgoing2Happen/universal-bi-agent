/**
 * DuckDB engine acquisition — Phase 0 of the DuckDB local-passthrough plan.
 *
 * The DuckDB CLI is an external NATIVE binary (a pure-JS SEA cannot embed one), so
 * there are two supported deployment shapes — both resolved by findDuckdbBinary():
 *
 *   (A) BUNDLED with the installer. Tauri ships the per-platform CLI as a bundle
 *       resource; the sidecar sets AGENT_ENGINES_DIR to its directory. Offline,
 *       vetted + signed at build time, zero runtime download. Pure discovery — no
 *       code in this file is needed for (A).
 *
 *   (B) LAZY-DOWNLOADED post-install. ensureDuckdbBinary() fetches the pinned CLI
 *       release into <configDir>/engines/, GATED by the allowNativeEngine policy,
 *       verified by EXECUTION (and SHA-256 when a checksum is pinned). Keeps the
 *       installer lean; the large-file minority pays a one-time download.
 *
 * Either way the binary is OPTIONAL: none present → the agent stays on the pure-JS
 * path (correct, just slower for very large files). Acquisition is best-effort and
 * NEVER throws to the caller, and NEVER blocks a query — call acquireInBackground()
 * once on startup; the v2 query branch uses whatever findDuckdbBinary() returns at
 * request time.
 */
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as crypto from 'crypto';
import { execFile } from 'child_process';
import type { AgentConfig } from './config';
import { enginesDir, findDuckdbBinary, runDuckdbJson } from './duckdb-engine';

/** Pinned DuckDB CLI release. Override the version with AGENT_DUCKDB_VERSION.
 * SHA-256 pins (recommended before production) can be supplied via
 * AGENT_DUCKDB_SHA256 or the per-asset `sha256` below — when absent we fall back
 * to verify-by-execution only (the binary must run `SELECT 42`). */
const DEFAULT_DUCKDB_VERSION = 'v1.5.3';

interface DuckdbAsset {
  url: string;
  archiveName: string;
  binaryName: string;
  sha256?: string; // optional supply-chain pin
}

/** Map the current platform/arch to its DuckDB CLI release asset, or null when
 * DuckDB publishes no CLI for it (→ the agent stays pure-JS). */
export function duckdbAssetForPlatform(): DuckdbAsset | null {
  const version = process.env.AGENT_DUCKDB_VERSION || DEFAULT_DUCKDB_VERSION;
  const base = `https://github.com/duckdb/duckdb/releases/download/${version}`;
  const sha256 = process.env.AGENT_DUCKDB_SHA256 || undefined; // single-platform override
  const mk = (archiveName: string, binaryName: string): DuckdbAsset => ({
    url: `${base}/${archiveName}`,
    archiveName,
    binaryName,
    sha256,
  });
  const { platform, arch } = process;
  if (platform === 'win32') {
    if (arch === 'x64') return mk('duckdb_cli-windows-amd64.zip', 'duckdb.exe');
    if (arch === 'arm64') return mk('duckdb_cli-windows-arm64.zip', 'duckdb.exe');
    return null;
  }
  if (platform === 'darwin') return mk('duckdb_cli-osx-universal.zip', 'duckdb'); // universal: x64 + arm64
  if (platform === 'linux') {
    if (arch === 'x64') return mk('duckdb_cli-linux-amd64.zip', 'duckdb');
    if (arch === 'arm64') return mk('duckdb_cli-linux-arm64.zip', 'duckdb');
    return null;
  }
  return null;
}

/** Is the agent permitted to acquire/run a native engine binary? Default OFF.
 * Honored via AGENT_ALLOW_NATIVE_ENGINE=true OR config.allowNativeEngine===true. */
export function nativeEngineAllowed(config?: Partial<AgentConfig> | null): boolean {
  if (process.env.AGENT_ALLOW_NATIVE_ENGINE === 'true') return true;
  return !!(config && config.allowNativeEngine === true);
}

function execFileP(cmd: string, args: string[], timeoutMs = 120000): Promise<void> {
  return new Promise((resolve, reject) => {
    execFile(cmd, args, { timeout: timeoutMs, windowsHide: true, maxBuffer: 16 * 1024 * 1024 }, (err, _o, stderr) => {
      if (err) reject(new Error(`${cmd} failed: ${(stderr || '').toString().trim() || err.message}`));
      else resolve();
    });
  });
}

/** Extract a .zip into destDir using the OS unzip (no JS zip dependency). */
async function unzipInto(archive: string, destDir: string): Promise<void> {
  if (process.platform === 'win32') {
    // Expand-Archive overwrites with -Force; -LiteralPath avoids wildcard parsing.
    const ps = `Expand-Archive -LiteralPath ${psQuote(archive)} -DestinationPath ${psQuote(destDir)} -Force`;
    await execFileP('powershell', ['-NoProfile', '-NonInteractive', '-Command', ps]);
  } else {
    await execFileP('unzip', ['-o', archive, '-d', destDir]); // macOS + most Linux ship unzip
  }
}
function psQuote(s: string): string {
  return `'${s.replace(/'/g, "''")}'`;
}

/** Confirm the binary is a working DuckDB by EXECUTING it (`SELECT 42`). This is
 * the integrity floor when no SHA-256 pin is configured — a truncated/corrupt/
 * wrong-arch download won't return 42. */
async function verifyByExecution(binary: string): Promise<boolean> {
  try {
    const rows = await runDuckdbJson(binary, 'SELECT 42 AS n', 15000);
    return Array.isArray(rows) && rows.length === 1 && Number((rows[0] as Record<string, unknown>).n) === 42;
  } catch {
    return false;
  }
}

let inflight: Promise<string | null> | null = null;

/**
 * Ensure a DuckDB CLI binary is available, downloading it if policy allows.
 * Returns the binary path, or null when unavailable / not permitted / failed
 * (caller falls back to JS). Single-flight: concurrent calls share one download.
 *
 * opts.sourceFile  — use a LOCAL archive instead of fetching (tests / air-gapped mirror)
 * opts.expectedSha256 — verify the archive bytes against this hex digest
 */
export async function ensureDuckdbBinary(
  opts: { config?: Partial<AgentConfig> | null; sourceFile?: string; expectedSha256?: string } = {},
): Promise<string | null> {
  const existing = findDuckdbBinary();
  if (existing) return existing;
  if (!nativeEngineAllowed(opts.config)) return null;
  if (inflight) return inflight;
  inflight = acquire(opts).finally(() => { inflight = null; });
  return inflight;
}

async function acquire(opts: { sourceFile?: string; expectedSha256?: string }): Promise<string | null> {
  const asset = duckdbAssetForPlatform();
  if (!asset) {
    console.warn(`[engine-acquire] no DuckDB CLI published for ${process.platform}/${process.arch} — staying pure-JS`);
    return null;
  }
  const dir = enginesDir();
  const target = path.join(dir, asset.binaryName);
  let tmpDir: string | null = null;
  try {
    fs.mkdirSync(dir, { recursive: true });
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'duckdb-dl-'));
    const archivePath = path.join(tmpDir, asset.archiveName);

    // 1) Obtain the archive bytes (local override or HTTPS download).
    let bytes: Buffer;
    if (opts.sourceFile) {
      bytes = fs.readFileSync(opts.sourceFile);
    } else {
      console.log(`[engine-acquire] downloading ${asset.url}`);
      const res = await fetch(asset.url, { redirect: 'follow' });
      if (!res.ok) throw new Error(`download HTTP ${res.status}`);
      bytes = Buffer.from(await res.arrayBuffer());
    }
    fs.writeFileSync(archivePath, bytes);

    // 2) SHA-256 supply-chain check when a pin is configured (else execution-only).
    const pin = opts.expectedSha256 || asset.sha256;
    if (pin) {
      const got = crypto.createHash('sha256').update(bytes).digest('hex');
      if (got.toLowerCase() !== pin.toLowerCase()) {
        throw new Error(`sha256 mismatch (expected ${pin}, got ${got})`);
      }
    } else {
      console.warn('[engine-acquire] no SHA-256 pin configured — relying on verify-by-execution (set AGENT_DUCKDB_SHA256 to harden)');
    }

    // 3) Extract into the engines dir (the CLI zip holds the binary at its root).
    await unzipInto(archivePath, dir);
    if (!fs.existsSync(target)) throw new Error(`archive did not contain ${asset.binaryName}`);
    if (process.platform !== 'win32') fs.chmodSync(target, 0o755);

    // 4) Verify-by-execution (catches truncation / wrong arch / corruption).
    if (!(await verifyByExecution(target))) {
      try { fs.rmSync(target, { force: true }); } catch { /* best-effort */ }
      throw new Error('verify-by-execution failed (SELECT 42 did not return 42)');
    }
    console.log(`[engine-acquire] DuckDB ready at ${target}`);
    return target;
  } catch (e) {
    console.warn('[engine-acquire] acquisition failed — agent stays pure-JS:', e instanceof Error ? e.message : String(e));
    return null;
  } finally {
    if (tmpDir) { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* best-effort */ } }
  }
}

/** Fire-and-forget acquisition for agent startup. Never throws; logs the outcome.
 * No-op (immediate) when a binary already exists or the policy is off. */
export function acquireInBackground(config?: Partial<AgentConfig> | null): void {
  if (findDuckdbBinary()) return;
  if (!nativeEngineAllowed(config)) return;
  ensureDuckdbBinary({ config })
    .then((p) => { if (p) console.log('[engine-acquire] background acquisition complete:', p); })
    .catch(() => { /* ensureDuckdbBinary already swallows + logs */ });
}
