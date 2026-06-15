# Vendored integrity module — DO NOT hand-edit

These files are a **verbatim vendored copy** of the platform realigner from
`Universal Intelligence/packages/ai/src/integrity/`, taken at platform commit **be4e7d0**.

## Why vendored (not an npm dep)
The agent (`universal-bi-agent`) is a separate repo with no dependency on
`@universal-bi/ai`. The big-file **self-verify serve path** needs the agent to run
the **EXACT** Node realigner over rows streamed from DuckDB — the cheap SQL proxy
was proven unable to certify multi-numeric cost cubes (same-type-swap blind spot).
Running byte-identical realigner code on both sides is what lets the agent's
realign verdict satisfy the platform's `v1.realignStreak` requirement without drift.

## Files
| File | Used on agent? | Purpose |
|---|---|---|
| `realignment.ts` | **YES** | `realignRows` / `checkRowAlignment` — the per-row realigner (graded score + shift/swap search) |
| `value-profile.ts` | partial | `scoreValueAgainstProfile` + `ValueProfile` type (runtime). `buildValueProfile` + `ValueProfileSchema` (zod) are **unused** on the agent (profiles arrive pre-built from the platform). |
| `normalize-string.ts` | no (transitively) | `probeInvisibleCharRate`, used only by `buildValueProfile` |

Runtime entry: import from **`./realigner-runtime`** (the clean API surface).

## Drift = correctness bug
If these diverge from the platform's committed version, the agent's realign verdict
could disagree with the platform's `v1.realignStreak` requirement → a cube proven
on one realigner served under the other. **Re-sync after ANY platform change to
`packages/ai/src/integrity/{realignment,value-profile,normalize-string}.ts`:**

```
node scripts/sync-integrity.mjs           # copy from the platform repo + restamp the commit
node scripts/sync-integrity.mjs --check   # CI: exit 1 if drifted (no copy)
```
