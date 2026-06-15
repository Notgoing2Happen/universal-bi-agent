#!/usr/bin/env node
/**
 * Sync (or drift-check) the vendored realigner against the platform source.
 *
 * The agent runs a byte-identical copy of the platform realigner
 * (packages/ai/src/integrity/{realignment,value-profile,normalize-string}.ts).
 * A drift would make the agent's realign verdict disagree with the platform's
 * v1.realignStreak requirement → a correctness bug. See
 * packages/agent-core/src/integrity/VENDORED.md.
 *
 *   node scripts/sync-integrity.mjs          # copy from the platform repo
 *   node scripts/sync-integrity.mjs --check  # exit 1 if drifted (CI; no copy)
 *
 * Platform repo path: AGENT_PLATFORM_REPO env, else the sibling default below.
 */
import { readFileSync, writeFileSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const AGENT_DIR = join(__dirname, '..', 'packages', 'agent-core', 'src', 'integrity');
const PLATFORM_REPO =
  process.env.AGENT_PLATFORM_REPO ||
  join(__dirname, '..', '..', 'Universal Intelligence'); // sibling checkout default
const PLATFORM_DIR = join(PLATFORM_REPO, 'packages', 'ai', 'src', 'integrity');
const FILES = ['realignment.ts', 'value-profile.ts', 'normalize-string.ts'];

const check = process.argv.includes('--check');

if (!existsSync(PLATFORM_DIR)) {
  console.error(`[sync-integrity] platform integrity dir not found: ${PLATFORM_DIR}\n` +
    `Set AGENT_PLATFORM_REPO to the Universal Intelligence checkout.`);
  process.exit(2);
}

let drift = 0;
for (const f of FILES) {
  const src = readFileSync(join(PLATFORM_DIR, f), 'utf8');
  const dstPath = join(AGENT_DIR, f);
  const dst = existsSync(dstPath) ? readFileSync(dstPath, 'utf8') : null;
  if (src === dst) {
    console.log(`[sync-integrity] ${f}: in sync`);
    continue;
  }
  drift++;
  if (check) {
    console.error(`[sync-integrity] DRIFT: ${f} differs from the platform source`);
  } else {
    writeFileSync(dstPath, src);
    console.log(`[sync-integrity] ${f}: SYNCED (${src.length} bytes)`);
  }
}

if (check && drift > 0) {
  console.error(`[sync-integrity] ${drift} vendored file(s) drifted — run \`node scripts/sync-integrity.mjs\` and re-bundle the agent.`);
  process.exit(1);
}
console.log(`[sync-integrity] done (${drift} ${check ? 'drifted' : 'synced'}).`);
