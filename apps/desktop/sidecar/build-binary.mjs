#!/usr/bin/env node

/**
 * Build a standalone binary from the sidecar using Node.js SEA (Single Executable Application).
 *
 * Steps:
 * 1. Bundle JS to single CJS file (esbuild)
 * 2. Generate SEA blob (node --experimental-sea-config)
 * 3. Copy node binary
 * 4. Inject blob into binary (postject)
 * 5. Move to src-tauri/binaries/ with target triple suffix
 *
 * Usage:
 *   node sidecar/build-binary.mjs                    # Build for current platform
 *   node sidecar/build-binary.mjs --target x86_64-pc-windows-msvc
 */

import { execSync } from 'child_process';
import { copyFileSync, mkdirSync, existsSync, chmodSync, statSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const desktopDir = resolve(__dirname, '..');
const distDir = resolve(desktopDir, 'dist-sidecar');
const binDir = resolve(desktopDir, 'src-tauri', 'binaries');

// Detect or use provided target triple
function getTargetTriple() {
  const targetArg = process.argv.find(a => a.startsWith('--target'));
  if (targetArg) {
    const idx = process.argv.indexOf(targetArg);
    if (targetArg.includes('=')) return targetArg.split('=')[1];
    return process.argv[idx + 1];
  }

  const platform = process.platform;
  const arch = process.arch;

  if (platform === 'win32') return 'x86_64-pc-windows-msvc';
  if (platform === 'darwin' && arch === 'arm64') return 'aarch64-apple-darwin';
  if (platform === 'darwin') return 'x86_64-apple-darwin';
  if (platform === 'linux') return 'x86_64-unknown-linux-gnu';

  throw new Error(`Unsupported platform: ${platform}/${arch}`);
}

const isWindows = process.platform === 'win32';
const targetTriple = getTargetTriple();
const ext = targetTriple.includes('windows') ? '.exe' : '';

console.log(`Building sidecar binary for: ${targetTriple}`);

// Step 1: Bundle JS
console.log('[1/5] Bundling JS with esbuild...');
execSync('node sidecar/build.mjs', { cwd: desktopDir, stdio: 'inherit' });

// Step 2: Generate SEA blob
console.log('[2/5] Generating SEA blob...');
execSync('node --experimental-sea-config sidecar/sea-config.json', {
  cwd: desktopDir,
  stdio: 'inherit',
});

// Step 3: Copy node binary
console.log('[3/5] Copying Node.js binary...');
mkdirSync(binDir, { recursive: true });

const nodeBin = process.execPath;
const outputName = `universal-bi-sidecar-${targetTriple}${ext}`;
const outputPath = resolve(binDir, outputName);

copyFileSync(nodeBin, outputPath);

// On macOS, remove codesign before injecting
if (process.platform === 'darwin') {
  console.log('  Removing macOS code signature...');
  try {
    execSync(`codesign --remove-signature "${outputPath}"`, { stdio: 'inherit' });
  } catch {
    // May fail if not signed, that's ok
  }
}

// Make executable on Unix
if (!isWindows) {
  chmodSync(outputPath, 0o755);
}

// Step 4: Inject SEA blob
console.log('[4/5] Injecting SEA blob into binary...');
const blobPath = resolve(distDir, 'universal-bi-sidecar.blob');
const sentinel = 'NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2';

// postject args differ by platform
const machoFlag = process.platform === 'darwin' ? '--macho-segment-name NODE_SEA' : '';

try {
  execSync(
    `npx postject "${outputPath}" NODE_SEA_BLOB "${blobPath}" --sentinel-fuse ${sentinel} ${machoFlag}`,
    { cwd: desktopDir, stdio: 'inherit' }
  );
} catch (err) {
  console.error('postject failed. Ensure postject is installed: npx postject --help');
  process.exit(1);
}

// On macOS, re-sign with ad-hoc signature
if (process.platform === 'darwin') {
  console.log('  Re-signing binary for macOS...');
  execSync(`codesign --sign - "${outputPath}"`, { stdio: 'inherit' });
}

// Step 5: Verify
console.log('[5/5] Verifying...');
const stat = existsSync(outputPath);
if (!stat) {
  console.error(`ERROR: Binary not found at ${outputPath}`);
  process.exit(1);
}

const size = (statSync(outputPath).size / 1024 / 1024).toFixed(1);
console.log(`\nSidecar binary built successfully!`);
console.log(`  Path:   ${outputPath}`);
console.log(`  Target: ${targetTriple}`);
console.log(`  Size:   ${size} MB`);
