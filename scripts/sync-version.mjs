#!/usr/bin/env node
/**
 * Local-dev version sync.
 *
 * Reads the canonical version from apps/desktop/src-tauri/tauri.conf.json
 * and writes it back into apps/desktop/sidecar/version.mjs so the two
 * stay in lockstep on developer machines.
 *
 * Phase 1 follow-up (2026-06-07): the CI release workflow already does
 * this from the pushed git tag (.github/workflows/release-desktop.yml,
 * "Set version from tag" step). This script is the local equivalent —
 * after manually editing tauri.conf.json's version (e.g. for a 0.1.34
 * RC), run `pnpm sync-version` to mirror the bump into version.mjs.
 *
 * If the two files already match, this is a no-op. If they drift,
 * tauri.conf.json wins (it's the canonical source — CI rewrites it
 * from the git tag at release time).
 *
 * Run from the repo root:
 *   pnpm sync-version
 *   or: node scripts/sync-version.mjs
 */

import { readFileSync, writeFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, '..');

const TAURI_CONF = resolve(repoRoot, 'apps/desktop/src-tauri/tauri.conf.json');
const VERSION_MJS = resolve(repoRoot, 'apps/desktop/sidecar/version.mjs');

const tauriConf = JSON.parse(readFileSync(TAURI_CONF, 'utf8'));
const canonical = tauriConf.version;
if (typeof canonical !== 'string' || !/^\d+\.\d+\.\d+(-[\w.]+)?$/.test(canonical)) {
  console.error(`✗ tauri.conf.json version "${canonical}" is not a valid semver.`);
  process.exit(1);
}

const src = readFileSync(VERSION_MJS, 'utf8');
const match = src.match(/AGENT_VERSION\s*=\s*'([^']*)'/);
if (!match) {
  console.error(`✗ Could not find AGENT_VERSION literal in ${VERSION_MJS}`);
  process.exit(1);
}

if (match[1] === canonical) {
  console.log(`✓ Already in sync: ${canonical}`);
  process.exit(0);
}

const updated = src.replace(/AGENT_VERSION\s*=\s*'[^']*'/, `AGENT_VERSION = '${canonical}'`);
writeFileSync(VERSION_MJS, updated);
console.log(`✓ Updated sidecar/version.mjs: ${match[1]} → ${canonical}`);
