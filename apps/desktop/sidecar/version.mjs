// Universal BI Agent — canonical runtime version source.
//
// This file is the ONE place the agent's runtime version lives. Every
// runtime reader (sidecar/main.mjs, query-server's /health endpoint,
// ipc-server's event.ready notification) imports from here.
//
// At release time, the CI workflow (.github/workflows/release-desktop.yml)
// rewrites this file from the pushed git tag BEFORE building the sidecar
// SEA binary. Result: the bundled binary reports the exact tag it was
// built from — no manual bumping of multiple files.
//
// For local development (no git tag in sight), this file holds the
// version that matches apps/desktop/src-tauri/tauri.conf.json. Run
// `pnpm sync-version` from the repo root to update both files together.
//
// DO NOT add other version literals elsewhere in the codebase. Per
// CLAUDE.md's class-shape rule, an instance-shaped fix ("bump the
// number here, here, and here") regrows the bug on every release.
// Read from this constant instead.

export const AGENT_VERSION = '0.1.33';
