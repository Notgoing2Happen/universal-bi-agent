#!/usr/bin/env node

/**
 * Bundle the sidecar into a single JS file for distribution.
 *
 * Dev:  node sidecar/main.mjs (runs directly with node)
 * Prod: esbuild bundles to CJS → Node SEA creates standalone binary
 */

import { build } from 'esbuild';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

async function bundle() {
  console.log('Bundling sidecar...');

  // Bundle to CJS for Node SEA compatibility (Node 20 SEA requires CJS)
  await build({
    entryPoints: [resolve(__dirname, 'main.mjs')],
    bundle: true,
    platform: 'node',
    target: 'node20',
    outfile: resolve(__dirname, '../dist-sidecar/universal-bi-sidecar.cjs'),
    format: 'cjs',
    minify: true,
    external: [
      // Native modules that can't be bundled
      'fsevents',
    ],
  });

  console.log('Sidecar bundled to dist-sidecar/universal-bi-sidecar.cjs');
}

bundle().catch((err) => {
  console.error('Bundle failed:', err);
  process.exit(1);
});
