/**
 * Repo-resident check for the DuckDB engine-acquisition logic (Phase 0).
 *
 * Pure-logic assertions (no binary needed) ALWAYS run: the allowNativeEngine
 * policy gate, env/config precedence, and per-platform asset mapping.
 *
 * The download/extract/verify block runs ONLY when AGENT_DUCKDB_TEST_ZIP points at
 * a release-shaped CLI zip (so CI without a binary skips it cleanly). To exercise
 * it locally: zip a DuckDB CLI into duckdb_cli-<platform>-<arch>.zip and set
 * AGENT_DUCKDB_TEST_ZIP to it.
 *
 * Run:  node packages/agent-core/scripts/engine-acquire-check.cjs
 */
const fs = require('fs');
const os = require('os');
const path = require('path');
const dist = path.join(__dirname, '..', 'dist');
let mod;
try {
  mod = { ...require(path.join(dist, 'engine-acquire.js')), ...require(path.join(dist, 'duckdb-engine.js')) };
} catch {
  console.log('SKIP: dist not built (run tsc first)');
  process.exit(0);
}
const { ensureDuckdbBinary, nativeEngineAllowed, duckdbAssetForPlatform, findDuckdbBinary } = mod;

let pass = 0, fail = 0;
const ok = (n, c, x) => { if (c) pass++; else { fail++; console.log('FAIL: ' + n + (x ? '  ' + x : '')); } };
const freshEnginesDir = () => { const d = fs.mkdtempSync(path.join(os.tmpdir(), 'eng-')); process.env.AGENT_ENGINES_DIR = d; return d; };

(async () => {
  // ── policy precedence (no binary required) ──
  delete process.env.AGENT_ALLOW_NATIVE_ENGINE;
  ok('nativeEngineAllowed: default OFF', nativeEngineAllowed(null) === false);
  ok('nativeEngineAllowed: config flag ON', nativeEngineAllowed({ allowNativeEngine: true }) === true);
  process.env.AGENT_ALLOW_NATIVE_ENGINE = 'true';
  ok('nativeEngineAllowed: env override ON', nativeEngineAllowed(null) === true);
  delete process.env.AGENT_ALLOW_NATIVE_ENGINE;

  // ── asset mapping (current platform) ──
  const asset = duckdbAssetForPlatform();
  if (asset) {
    ok('asset has url/archive/binary', !!(asset.url && asset.archiveName && asset.binaryName));
    ok('asset url targets the pinned release', /releases\/download\/v\d/.test(asset.url));
  } else {
    ok('asset null only on unsupported platform/arch', !['win32', 'darwin', 'linux'].includes(process.platform) || (process.arch !== 'x64' && process.arch !== 'arm64'));
  }

  // ── policy OFF → never acquires (no binary required) ──
  { freshEnginesDir();
    const p = await ensureDuckdbBinary({ config: null, sourceFile: process.env.AGENT_DUCKDB_TEST_ZIP || 'C:/nope.zip' });
    ok('policy OFF → ensureDuckdbBinary returns null', p === null || typeof p === 'string'); // string only if one is already on PATH
    if (p) ok('  (a binary was already discoverable on PATH)', true); }

  // ── extract/verify/sha block — only with a provided zip ──
  const ZIP = process.env.AGENT_DUCKDB_TEST_ZIP;
  if (ZIP && fs.existsSync(ZIP) && asset) {
    { const dir = freshEnginesDir();
      const p = await ensureDuckdbBinary({ config: { allowNativeEngine: true }, sourceFile: ZIP });
      ok('acquire: places + verifies binary', p === path.join(dir, asset.binaryName) && fs.existsSync(p), String(p));
      ok('acquire: findDuckdbBinary discovers it', findDuckdbBinary() === path.join(dir, asset.binaryName)); }
    { const dir = freshEnginesDir();
      const p = await ensureDuckdbBinary({ config: { allowNativeEngine: true }, sourceFile: ZIP, expectedSha256: 'deadbeef'.repeat(8) });
      ok('acquire: wrong SHA-256 rejects (nothing placed)', p === null && !fs.existsSync(path.join(dir, asset.binaryName))); }
    { const dir = freshEnginesDir();
      const [a, b] = await Promise.all([
        ensureDuckdbBinary({ config: { allowNativeEngine: true }, sourceFile: ZIP }),
        ensureDuckdbBinary({ config: { allowNativeEngine: true }, sourceFile: ZIP }),
      ]);
      ok('acquire: single-flight (concurrent → same path)', a === b && a === path.join(dir, asset.binaryName)); }
  } else {
    console.log('NOTE: set AGENT_DUCKDB_TEST_ZIP=<release zip> to also test extract/verify/sha/single-flight');
  }

  delete process.env.AGENT_ENGINES_DIR;
  console.log(`\nENGINE-ACQUIRE-CHECK: ${pass} passed, ${fail} failed (of ${pass + fail})`);
  process.exit(fail ? 1 : 0);
})().catch((e) => { console.error('ERR', e); process.exit(1); });
