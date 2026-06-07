/**
 * Agent version accessor for the agent-core package.
 *
 * Phase 1 follow-up (2026-06-07): replaces the hardcoded '0.1.0' literals
 * that lived in query-server.ts (/health endpoint) and ipc-server.ts
 * (event.ready notification) since the agent's first commit. Both were
 * never bumped across 33 releases — the user reported the symptom as
 * "agent version shows v0.1.0 even after upgrading."
 *
 * The canonical source of truth is `apps/desktop/sidecar/version.mjs`,
 * which the CI workflow rewrites from the pushed git tag at release time.
 * The sidecar reads it on startup and propagates the value to all child
 * processes via the `AGENT_VERSION` environment variable BEFORE spawning
 * the IPC server / query server. This module's getAgentVersion() reads
 * back from that env var.
 *
 * Why an env var instead of importing the .mjs file directly here?
 *   1. agent-core is a CJS-compiled TS package; importing a sibling .mjs
 *      file via `import x from 'file://...'` requires top-level await
 *      and ESM compilation. The env-var hop sidesteps the module system.
 *   2. The sidecar SEA binary bundles version.mjs into its own snapshot;
 *      reaching out from agent-core to a file path is fragile because
 *      agent-core lives at packages/agent-core/dist/* and the sidecar
 *      version.mjs is at apps/desktop/sidecar/version.mjs — relative
 *      paths break in test, in dev, and in the bundled binary.
 *   3. Env-var injection lets the platform (or test harness) override
 *      the reported version without recompiling.
 *
 * Failure mode: if AGENT_VERSION is not set, we throw on first access.
 * This is intentional per CLAUDE.md's "fail loudly, don't return defaults
 * that look successful" rule — defaulting to '0.1.0' is exactly how this
 * bug rotted for 33 releases.
 *
 * Test harnesses that don't go through the sidecar should set
 * `process.env.AGENT_VERSION` before importing query-server / ipc-server.
 */

let cached: string | null = null;

export function getAgentVersion(): string {
  if (cached !== null) return cached;
  const v = process.env.AGENT_VERSION;
  if (!v) {
    throw new Error(
      'AGENT_VERSION env var is not set. The sidecar (apps/desktop/sidecar/main.mjs) ' +
        'must set process.env.AGENT_VERSION = AGENT_VERSION (from ./version.mjs) ' +
        'BEFORE startQueryServer() or startIpcServer() is called. ' +
        'See packages/agent-core/src/version.ts for the rationale.',
    );
  }
  cached = v;
  return cached;
}

/**
 * Test-only escape hatch. Resets the cached value so subsequent
 * getAgentVersion() calls re-read process.env. Don't use in prod code.
 */
export function __resetAgentVersionCacheForTests(): void {
  cached = null;
}
