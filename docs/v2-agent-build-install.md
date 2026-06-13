# Building & installing the DuckDB-capable agent (v0.1.40)

The DuckDB local-passthrough engine (v2 querySpec executor, parse cache, Phase-0
acquisition) shipped on `master` @ `1b702ba`; the version is bumped to **0.1.40**
(`5daefb4`). The agent on the file-serving machine must run this build (the old
v0.1.39 can't execute a v2 `querySpec`) for the platform's DuckDB v2 shadow gate to
validate. Pairs with the platform runbook `docs/duckdb-v2-shadow-runbook.md` in the
`Universal Intelligence` repo.

Verified from a dev box: `agent-core` compiles clean (`tsc` exit 0) and the SEA
sidecar binary builds from the v2 code (`node sidecar/build-binary.mjs` → 87 MB
`universal-bi-sidecar-x86_64-pc-windows-msvc.exe`, exit 0). The only thing not done
here is the **signed installer** (Tauri build + code-sign) — that's your build machine.

---

## Path A — CI release (recommended; signed installers + auto-update)

The release is **tag-driven**: pushing a `v*` tag triggers `release-desktop.yml`, which
rewrites `tauri.conf.json` + `sidecar/version.mjs` from the tag, builds the Tauri app +
SEA sidecar per platform, and uploads installers to a GitHub release. Users auto-update.

```bash
# from the universal-bi-agent repo, on master (already at 5daefb4):
git tag v0.1.40
git push origin v0.1.40        # ← triggers the signed-installer build + GitHub release
```

Then install on the file-serving machine: download the v0.1.40 installer from the
GitHub release (or let the running agent auto-update), and install.

> This tag push is the "signed installer" step — it builds + publishes a release users
> can auto-update to. The v2 agent is **behavior-neutral** until the platform enables v2
> (and DuckDB only activates if `allowNativeEngine` is on — see below), so shipping it
> broadly is safe.

## Path B — local build (fastest for a one-off shadow test; unsigned)

```bash
cd apps/desktop
node sidecar/build-binary.mjs            # SEA sidecar (verified working) → src-tauri/binaries/
pnpm tauri build                         # full installer (needs Rust toolchain; unsigned locally)
# installer lands in apps/desktop/src-tauri/target/release/bundle/
```
Install the produced bundle on the file-serving machine.

---

## Make it DuckDB-capable (required for the shadow to validate)

The native-engine policy is **default-OFF** (the agent stays pure-JS unless opted in).
On the file-serving machine, do ONE of:

```jsonc
// ~/.universal-bi/config.json  — add:
"allowNativeEngine": true
```
or set the env before launching the agent:
```bash
AGENT_ALLOW_NATIVE_ENGINE=true
```
On startup the agent then lazy-downloads the DuckDB CLI to `<config>/engines/` (verified
by execution + optional SHA-256). Alternatively point `AGENT_DUCKDB_PATH` at an existing
DuckDB binary, or drop one in the engines dir / set `AGENT_ENGINES_DIR`.

> **macOS caveat (Phase-0 open item):** a lazy-downloaded CLI may be Gatekeeper-quarantined
> → `verify-by-execution` fails → the agent stays pure-JS. On macOS, prefer pointing
> `AGENT_DUCKDB_PATH` at a binary you've cleared (`xattr -d com.apple.quarantine`), or
> bundle the CLI with the installer, until the acquire step strips the quarantine xattr.

---

## Verify the agent is ready

1. **Version**: the agent's `/health` (and its platform registration) reports `0.1.40`.
2. **Capability**: registration shows `supportsDuckdb: true` (and `canAcquireDuckdb: true`
   if you used the policy rather than a pre-placed binary). If `supportsDuckdb` is false,
   no DuckDB binary was found/acquired — recheck the step above.
3. **Shadow fires**: with `AGENT_DUCKDB_V2_SHADOW=true` on the platform, run a v2-pushable
   query against an agent-file cube. The cube logs should show
   `[duckdb-v2-shadow] <cube>: MATCH …`, NOT `… agent did NOT DuckDB-aggregate … skip`.
   Then run the platform's `scripts/verify-v2-shadow.cjs`.
