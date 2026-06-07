# Agent Throughput Fix — Scope

## Summary

**Branch:** `agent-throughput-fix`

**Total Effort Estimate:**
- Phase 0 — Memory Safety Foundation: 2-3 days
- Phase 1 — Streaming Readers for Query Path: 3-4 days
- Phase 2 — Streaming AI #1 Column Sampling: 3-4 days
- Phase 3 — JSON-RPC Chunked Response Transport: 3-4 days
- **Total (Phases 0-3, required): 11-15 days**
- Phase 4 — Platform-Side AgentRelay Query Batching (optional, performance): 3-4 days
- **Total (with optional Phase 4): 14-19 days**

**High-Level Objectives:**

1. **Eliminate silent crashes** on large file uploads and queries by enforcing file-size caps, capping HTTP body sizes, constraining Node.js heap, and surfacing structured errors to the platform.
2. **Replace whole-file synchronous reads** in the query path with streaming parsers that push filter and limit predicates down into the parser pipeline, reducing peak memory from O(file_size) to O(result_size).
3. **Replace whole-file buffering** in AI #1 column sampling with true streaming reservoir sampling, enabling 10GB+ file ingestion without OOM regardless of input size.
4. **Eliminate IPC payload ceiling** by implementing chunked JSON-RPC response transport, allowing large query results to transit the Tauri sidecar boundary without single-string allocation failures.
5. **(Optional) Reduce polling overhead** by batching pending queries per poll cycle and consolidating result POSTs, complementing the agent's existing parallel query execution.

The required phases (0-3) address correctness — the agent will no longer silently crash on large files at any stage of the pipeline. Phase 4 is a throughput optimization that pairs well with the existing parallel-query execution (commit ca66dfe) but is not required for correctness.

## Architecture

### Current Data Flow

```
+----------------+                                          +----------------+
|                |                                          |                |
|   Platform     |                                          |   File         |
|   (Cube.js +   |                                          |   System       |
|    Next.js)    |                                          |                |
|                |                                          |                |
+--------+-------+                                          +--------+-------+
         |                                                           ^
         | POST /api/agent/queries                                    |
         | (single query at a time)                                   |
         v                                                            |
+----------------+        +----------------+        +----------------+|
|                |        |                |        |                ||
|  AgentRelay    | <----> |  Tauri Rust    | <----> |  Node Sidecar  ||
|  (in-memory    |  HTTP  |  (lib.rs)      | stdio  |  (main.mjs +   ||
|   promise map) |  poll  |                |        |   agent-core)  ||
|                |        |                |        |                ||
+----------------+        +-------+--------+        +--------+-------+|
                                  ^                          |       |
                                  |                          |       |
                                  | reader.read_line()       |       |
                                  | (expects single line     |       |
                                  |  per response — fails    |       |
                                  |  silently on >256MB)     |       |
                                                             |       |
                          +----------+                       |       |
                          |          |  fs.readFileSync()    |       |
                          |  Query   | <---------------------+-------+
                          |  Server  |  (whole file in memory)
                          |          |
                          | parsers: |  applyFilters POST-load
                          |   csv    |  limit via rows.slice()
                          |   json   |
                          |   xlsx   |
                          +----+-----+
                               |
                          +----v-----+
                          |          |  fs.readFileSync()
                          | Uploader | <-------------------+
                          |  AI #1   |  (whole file)       |
                          |          |                     |
                          | Fisher-  |                     |
                          | Yates    |                     |
                          | shuffle  |                     |
                          +----+-----+                     |
                               |                           |
                               | sample values             |
                               v                           |
                          +----+-----+                     |
                          | AI #10   |                     |
                          | fetch()  |  no timeout         |
                          | platform |                     |
                          +----------+                     |
                                                           |
                                                           |
PROBLEMS:                                                  |
- 50MB+ files trigger silent OOM / heap exhaustion         |
- Excel locks the entire file in memory                    |
- Query results >256MB hit V8 string ceiling on send       |
- AI #10 fetch can hang indefinitely                       |
- No structured errors back to platform                    |
- No batching: N queries = N polls + N result POSTs        |
```

### Target Data Flow

```
+----------------+                                          +----------------+
|                |                                          |                |
|   Platform     |                                          |   File         |
|   (Cube.js +   |                                          |   System       |
|    Next.js)    |                                          |                |
|                |                                          |                |
+--------+-------+                                          +--------+-------+
         |                                                           ^
         | POST /api/agent/queries                                    |
         |   (single OR batched)                                      |
         v                                                            |
+----------------+        +----------------+        +----------------+|
|                |        |                |        |                ||
|  AgentRelay    | <----> |  Tauri Rust    | <----> |  Node Sidecar  ||
|  (capability   |  HTTP  |  (lib.rs)      | stdio  |  (main.mjs +   ||
|   negotiation) |  batch |                |        |   agent-core)  ||
|                |  poll  | + ChunkAssem.  |        |  + classify    ||
+----------------+        |   reassembles  |        |    errors      ||
                          |   chunked      |        |  NODE_OPTIONS  ||
                          |   responses    |        |   --max-old-   ||
                          |                |        |    space-size= ||
                          | + heap cap     |        |    800         ||
                          |   monitor      |        |                ||
                          +-------+--------+        +--------+-------+|
                                  ^                          |       |
                                  |                          |       |
                                  | reader.read_line() loop  |       |
                                  | with __chunked detection |       |
                                  | + UUID-keyed reassembly  |       |
                                  | + 30s frame timeout      |       |
                                                             |       |
                          +----------+                       |       |
                          |  Query   |   serializeWith       |       |
                          |  Server  |   Chunking(8MB)       |       |
                          |          |                       |       |
                          | parsers: |   fs.createReadStream |       |
                          |   csv:   | <---------------------+-------+
                          |     csv- |   (streaming + filter |
                          |     parse|    push-down + limit  |
                          |     w/   |    early termination) |
                          |     filter                       |
                          |     stop |   stat-before-read,   |
                          |   json:  |   413 if > 50MB       |
                          |     fast-|                       |
                          |     path |   readBody cap        |
                          |     +    |                       |
                          |     fall-|                       |
                          |     back |                       |
                          |   xlsx:  |                       |
                          |     hard |                       |
                          |     cap  |                       |
                          |     10MB |                       |
                          +----+-----+                       |
                               |                             |
                          +----v-----+                       |
                          | Uploader |   StreamingReservoir  |
                          |  AI #1   |   Sampler (Alg. R)    |
                          |          |                       |
                          | fs.create| <---------------------+
                          | ReadStream                       |
                          |   +      |   stat-check before   |
                          | reservoir|   stream open         |
                          | per-col  |                       |
                          | (N=10)   |                       |
                          |          |                       |
                          | constant |                       |
                          | memory   |                       |
                          +----+-----+
                               |
                               | sample values
                               v
                          +----+-----+
                          | AI #10   |
                          | fetch()  |  AbortSignal.
                          | platform |  timeout(30000)
                          +----------+

IMPROVEMENTS:
- File-size caps enforced before any I/O (Phase 0)
- HTTP body capped to prevent unbounded growth (Phase 0)
- Node heap explicitly capped at 800MB (Phase 0)
- Structured error codes flow back to platform (Phase 0)
- Streaming filter + limit push-down on CSV (Phase 1)
- JSON streaming fast-path for arrays (Phase 1)
- Excel hard cap with clear user message (Phase 1)
- Reservoir sampling: O(N x cols) memory regardless of file size (Phase 2)
- AI #10 fetch bounded by 30s timeout (Phase 2)
- Large IPC responses split into 8MB frames (Phase 3)
- Rust reassembles frames into typed results (Phase 3)
- (Optional) Batched poll + result POSTs (Phase 4)
```

## Phase 0 — Memory Safety Foundation

**Objective:** Establish memory safety guardrails to prevent silent crashes on large files. This phase implements file-size validation before any I/O, caps HTTP request bodies, configures Node.js heap limits, and wraps all query handlers in structured error reporting so failures surface clearly to the platform. Success means files over `maxFileSize` (50MB default) are rejected with a descriptive error code before attempting to read, memory exhaustion is caught before it kills the sidecar, and every query failure reports back to `/api/agent/queries/:id/result` with a structured error payload that the platform can surface to users.

### File Changes

| File | Current Behavior | Change Description |
|---|---|---|
| `packages/agent-core/src/config.ts` | Lines 32-33 define `maxFileSize` in the `AgentConfig` interface (default 50MB at line 45), but `maxFileSize` is never exported as a constant and is never referenced by any query or upload handler. | Export `maxFileSize` as a public constant so all modules can reference the same limit without parsing config. This becomes the single source of truth for file-size enforcement. |
| `packages/agent-core/src/query-server.ts` | Lines 223-230 (`loadFileData`) calls `fs.readFileSync(filePath)` with no size check. Lines 82, 136, 159 call `readFileSync` on CSV, JSON, and Excel files without stat-checking first. Lines 514-520 (`readBody`) accumulates request body chunks with no size limit, creating OOM risk. | Stat-before-read pattern: `fs.statSync` before each `readFileSync`; reject with HTTP 413 if size > `maxFileSize`; cap `readBody` accumulation to prevent unbounded memory growth; enhance top-level try/catch to detect error types and return appropriate HTTP status codes. |
| `packages/agent-core/src/uploader.ts` | Line 555 (`uploadFile`) calls `fs.readFileSync(resolved)` with no size check. Line 546 has `fs.statSync(resolved)` to get file size for upload metadata, but size is never validated against `maxFileSize` before reading. | Add file-size validation before `readFileSync` using already-computed `stats.size`; return early with `{ success: false, code: 'FILE_TOO_LARGE', maxFileSize, actualSize }` matching the query-server pattern. |
| `apps/desktop/src-tauri/src/lib.rs` | Lines 168-179 spawn the Node sidecar with stdio pipes but no `--max-old-space-size` flag. Node uses default heap (~1.4GB on 64-bit) so OOM behavior is unpredictable. | Inject `NODE_OPTIONS=--max-old-space-size=800` (or pass `--max-old-space-size` as command-line argument) to constrain Node's heap to 4x `maxFileSize`. Log clear message when sidecar exits with SIGABRT (status 134). |
| `apps/desktop/sidecar/main.mjs` | Lines 390-457 (query relay polling) spawn promises for queries in parallel but error reporting is minimal: just `{ error: queryErr.message }`. No error code to distinguish file-too-large from network timeout from corrupt file. | Enhance query error handler to classify error types into error codes (`FILE_TOO_LARGE`, `FILE_NOT_FOUND`, `PARSE_ERROR`, `MEMORY_LIMIT_EXCEEDED`, etc.) before POSTing to `/api/agent/queries/:id/result`, so the platform can surface actionable messages. |
| `packages/agent-core/src/query-server.ts` (error classifier) | Lines 485-489 have a top-level try/catch returning HTTP 500 with `{ error: err.message }`. No HTTP status differentiation by error type. | Add `classifyError(err)` returning `{ status, payload: { error, code, ...details } }`. Map FILE_TOO_LARGE → 413, PARSE_ERROR → 400, FILE_NOT_FOUND → 404, else 500. Prefer typed error classes (`class FileTooLargeError extends Error { code = 'FILE_TOO_LARGE' }`) over regex on message text. |

### Test Plan

- File-size validation (uploader.ts): Create a 60MB test CSV. Call `uploadFile(path, config)` and verify it returns `{ success: false, code: 'FILE_TOO_LARGE', maxFileSize: 52428800, actualSize: 62914560 }`. Verify the file is not read.
- File-size validation (query-server.ts): POST `/query` with a 60MB local file. Verify server returns HTTP 413 with `{ error: 'file_too_large', code: 'FILE_TOO_LARGE', ... }`.
- HTTP body cap: POST `/query` with a request body > 50MB. Verify `readBody` rejects it and server returns HTTP 413 with no memory growth.
- Node.js heap limit: Start the sidecar and verify `NODE_OPTIONS=--max-old-space-size=800` is set. Simulate memory exhaustion and verify process exits with SIGABRT (status 134), not silent hang.
- Error classification: Trigger FILE_TOO_LARGE, PARSE_ERROR (corrupt JSON), FILE_NOT_FOUND, NETWORK_TIMEOUT. Verify each classified correctly in error code and HTTP status code.
- Query relay error reporting: POST `/query` with malformed request. Verify relay catches, classifies, and POSTs to `/api/agent/queries/:id/result` with `{ error: '...', code: '...' }`.
- Concurrent query resilience: POST 10 queries with 3 triggering file-too-large. Verify all 10 report back (none lost) and the 3 errors are classified correctly.
- Config constant usage: Verify `MAX_FILE_SIZE_BYTES` (or `loadConfig().maxFileSize`) is imported and used by uploader.ts, query-server.ts, and main.mjs. Verify changing `maxFileSize` in config.ts propagates to all three.
- Backward compatibility: Queries against files < 50MB still work. Query relay still processes batch queries correctly.

### Risks

| Risk | Mitigation |
|---|---|
| `NODE_OPTIONS` at Tauri startup may not affect a Node sidecar compiled as a standalone SEA — SEA binaries have baked-in Node runtime and may not honor env vars. | Test on the target deployment (Tauri-bundled SEA). If `NODE_OPTIONS` doesn't work, modify lib.rs to pass `--max-old-space-size=800` as the first argument to the sidecar command. Fallback: document warning in UI if sidecar crashes with SIGABRT. |
| Capping `readBody` at 50MB means a single HTTP request cannot exceed 50MB. If platform sends queries with embedded large sample data, they'll be rejected. | Set `readBody` cap to `2 * MAX_FILE_SIZE` (100MB) for headroom on query parameters and metadata. Monitor memory usage during parallel queries. |
| Regex matching on `error.message` for classification is fragile — messages can change, typos cause misclassification. | Define typed error classes (`class FileTooLargeError extends Error { code = 'FILE_TOO_LARGE' }`) and throw them from parsers. Catch by `instanceof` instead of regex. Safer and more testable. |
| Adding `fs.statSync` before `readFileSync` adds latency. | `stats` is already computed at line 546; the check is just an O(1) size comparison. No new I/O. |
| Hardcoded limits in query-server.ts and main.mjs may drift from config.ts. | Export `MAX_FILE_SIZE_BYTES` from config.ts. Import in query-server.ts and uploader.ts. In main.mjs, call `loadConfig().maxFileSize` at startup or use a shared constants module. |

**Effort Estimate:** 2-3 days

**Dependencies:**
- Existing repo has concurrency in place (commit ca66dfe). No dependency on Phase 1.
- Node.js >= 14.x (standard for `--max-old-space-size`).
- Tauri >= 1.0 (already in use).
- Platform backend must parse `{ code }` field from `/api/agent/queries/:id/result` payloads. Coordinate with backend team to map `FILE_TOO_LARGE` → user-facing message.

## Phase 1 — Streaming Readers for Query Path

**Objective:** Replace whole-file synchronous reads with streaming parsers in the query server to enable early termination and memory-efficient filtering on large CSV/JSON files. CSV gains filter+limit push-down into the stream pipeline; JSON uses fast-path for arrays with fallback; Excel gets a hard 10MB cap with clear user messaging. This phase delivers streaming-ready infrastructure for queries (the hot path) while deferring exceljs migration to Phase 2. Queries on large CSV files no longer load the entire file into memory; filter and limit are applied during parsing for orders-of-magnitude memory savings on selective queries.

### File Changes

| File | Current Behavior | Change Description |
|---|---|---|
| `packages/agent-core/package.json` | Dependencies include only `chokidar` and `xlsx` (0.18.5, no streaming). No `csv-parse`, `stream-json`, or `exceljs`. | Add `csv-parse: ^5.5.5` (streaming CSV parser with filter callbacks) and `stream-json: ^1.8.0` (streaming JSON parser with pick/ignore transforms). Defer `exceljs` to Phase 2; Phase 1 enforces a hard cap on xlsx instead. |
| `packages/agent-core/src/query-server.ts` (parsers) | Lines 81-110: `parseCsvFile` uses `fs.readFileSync` + `string.split`. Lines 135-144: `parseJsonFile` uses `readFileSync + JSON.parse`. Lines 157-190: `parseExcelFile` uses `XLSX.read` on full buffer. Lines 236-254: `applyFilters` runs AFTER load. Limit applied via `rows.slice` after full load. | Replace with streaming parsers: (1) CSV via `csv-parse` stream with filter+limit push-down and early termination via `stream.destroy()`. (2) JSON via `stream-json` `StreamArray` fast-path with whole-file fallback for nested objects (50MB cap). (3) Excel: hard cap at 10MB with clear user error. Make `loadFileData` async returning `Promise<Record<string, unknown>[]>`. |
| `packages/agent-core/src/query-server.ts` (/query handler) | Lines 319-364: POST `/query` calls `loadFileData`, then post-filters, then applies limit via slice. No streaming, entire file in memory before filtering. | Refactor to await async `loadFileData`. Remove post-filter step (filters now applied in stream). Keep offset/limit pagination on the already-limited result. `totalRows` semantics change to filtered row count (simpler, more accurate UX). |
| `packages/agent-core/src/query-server.ts` (/sequence-region) | Lines 366-463: `/sequence-region` POST calls `loadFileData` (line 390) and loads full file. | Line 390: change `const rows = loadFileData(filePath);` → `const rows = await loadFileData(filePath);`. Logic unchanged — sequence-region needs full dataset anyway. |
| `packages/agent-core/src/query-server.ts` (/schema) | Lines 465-481: `/schema/:connectionId` GET calls `loadFileData` (line 475) to infer columns. | Line 475: change to `await loadFileData(filePath)`. Logic unchanged. |

### Test Plan

- Unit test: `parseCsvFile` with 1M-row CSV + limit=100. Verify only ~100 rows held in memory (stream early-terminates). Measure peak heap before/after.
- Unit test: `parseJsonFile` with 100MB JSON file in array format. Verify stream-based parsing completes without OOM.
- Unit test: `parseJsonFile` with nested object (non-array). Verify fallback to whole-file with 50MB cap check; file > 50MB throws.
- Unit test: `parseExcelFile` with 15MB .xlsx file. Verify throws `Excel file exceeds 10MB limit` with user guidance.
- Integration test: POST `/query` on 500K-row CSV with filters (`status='active'`, `limit=50`). Verify response contains only 50 rows in < 500ms with no memory spike.
- Integration test: POST `/query` on same 500K-row CSV without limit (default 10K). Verify `totalRows` semantics (filtered count for now).
- Integration test: POST `/sequence-region` still works with streaming (await change).
- Integration test: GET `/schema/:connectionId` still works with streaming.
- Backward compat: POST `/query` with `offset+limit` combination (e.g., `offset=1000, limit=100` on filtered result).
- Error case: POST `/query` on malformed CSV. Verify stream parser error is caught and returned as 500 with error message.

### Risks

| Risk | Mitigation |
|---|---|
| Changing `totalRows` semantics from 'file row count' to 'filtered row count' may break downstream consumers (Cube.js, UI pagination) that expect the pre-filter total. | Phase 1 chooses 'filtered row count' (simpler, better UX). Document in changelog. If Cube.js requires pre-filter count, add `?countAll=true` query param in Phase 1.1, or add `totalRowsBeforeFilter` field. |
| `stream-json` may not support all JSON structures (deeply nested objects, streaming object properties). Fallback to whole-file may still OOM. | Test `stream-json` against real-world JSON files from agent uploads. If nested structures fail, hard-cap whole-file reads at 50MB with clear user error: 'JSON file structure not streamable and exceeds 50MB; please split or use CSV format.' |
| Filter callbacks in `csv-parse` Transform streams may have race conditions or incorrect row counting; early termination could truncate valid results. | Thorough unit tests with edge cases: empty result set (no matches), limit=1, offset > filtered row count, filters on non-existent column. Manual inspection of csv-parse Transform logic before merge. |
| Existing tests assume synchronous parsers; changing to async (Promise-returning) breaks them. | Search repo for existing tests of parseCsvFile/parseJsonFile/parseExcelFile. Update all calls to use `await`. Write new tests as part of Phase 1 if none exist. |
| Hard 10MB cap on Excel may frustrate users with legitimate large .xlsx files. Phase 2 (`exceljs`) may not ship if priorities shift. | Document cap clearly in error message with link to CSV conversion guide. Track user complaints via error logs. Prioritize Phase 2 if >10% of queries hit the Excel cap. |
| Stream-based parsing may leak file handles if `stream.destroy()` is not called correctly. | All streams wrapped in try/finally to ensure `destroy()` called. Test cleanup with `lsof` or Node.js handle counting. Add heap snapshot tests to verify no handle accumulation over 100 requests. |

**Effort Estimate:** 3-4 days

**Dependencies:**
- Phase 0 — Memory Safety Foundation (file size caps, error handling patterns, test infrastructure).
- Commit ca66dfe (concurrency already in place via `Promise.allSettled` — Phase 1 builds on this for parallel streaming).

## Phase 2 — Streaming AI #1 Column Sampling

**Objective:** Replace Phase 1's full-file buffering with true streaming reservoir sampling for CSV, JSON, and Excel parsers. Maintain a fixed-size reservoir (N=10 per column) while iterating through the file once using Node.js streams. This reduces peak memory from O(file_size) to O(N × num_columns) regardless of input file size. Also add explicit 30s timeout to AI #10 structure-decision fetch to prevent hanging on large files. After this phase, the agent can process 10GB CSV files without OOM crashes.

### File Changes

| File | Current Behavior | Change Description |
|---|---|---|
| `packages/agent-core/src/uploader.ts` (sampling) | Lines 77-88: `sampleColumnValues()` gathers ALL non-empty values into an array, shuffles via Fisher-Yates, slices to `SAMPLE_ROWS`. Caller must pre-load entire file (line 555: `fs.readFileSync`). Lines 205-250: `parseCsvSchema` loads full text and iterates column-wise. Peak memory O(file_size). | Implement true streaming reservoir sampling using Algorithm R (Vitter 1985). Create `StreamingReservoirSampler` with `onRow(rowValues)` callback; for each new value, decide with probability N/count whether to add to reservoir, replace random element if added. CSV uses `fs.createReadStream` + `readline.Interface`. JSON uses `JSONStream`/`stream-json` or full parse if <100MB. Excel KEEPS full buffer (xlsx 0.18.5 limitation) but applies reservoir sampling within the buffer. |
| `packages/agent-core/src/uploader.ts` (AI #10 fetch) | Lines 97-162: `getStructureDecisionFromAI()` has no timeout on fetch call (line 134-141). Relies on implicit Node.js timeout (~2 min). Can hang if platform is slow or file too large to analyze. | Add `signal: AbortSignal.timeout(30000)` to the fetch call. If platform doesn't respond within 30s, abort and fall back to heuristic parsing. Existing catch block at line 158 already handles `AbortError`. |
| `packages/agent-core/src/uploader.ts` (SAMPLE_ROWS doc) | Lines 48, 205, 283, 390: `SAMPLE_ROWS = 10` is hard-coded across four parser entry points. Comment doesn't explain Phase 2 semantics change. | Update JSDoc on `SAMPLE_ROWS` to clarify: "Reservoir size per column for streaming AI #1 sampling. Algorithm R (Vitter 1985) maintains a fixed-size reservoir of this many non-empty values per column while streaming the file once, regardless of total file size." |

### Test Plan

- Unit test: `StreamingReservoirSampler` with deterministic PRNG (seeded `Math.random`). For a synthetic column with 1000 unique values, verify reservoir contains ~10 distinct values and distribution is uniform (not clustered at start/end).
- Integration test: `parseCsvSchema` with `fs.createReadStream` on 10GB CSV. Measure peak memory — should be <100MB regardless of file size.
- Integration test: `parseJsonSchema` streaming a 10GB JSONL file. Verify same constant memory profile.
- Excel edge case: `parseExcelSchema` still buffers (xlsx limitation) but verify reservoir sampling correctly applied. 100MB+ Excel file should not crash (enforced by Phase 1 size cap).
- Timeout test: `getStructureDecisionFromAI` with mock fetch stalling >30s. Verify `AbortSignal` fires and function returns null (triggering fallback heuristic).
- Regression test: `parseCsvSchema` on Global_Health_Lab test fixture (sparse `location_details` column, ~100k rows). Verify sampled values are non-null and representative.
- Platform integration: Schema upload includes sampled values from streaming path. Verify AI #1 receives same quality samples as Phase 1 Fisher-Yates with no degradation in mapping confidence.
- Concurrency stress test: `syncDirectory` on watch folder with 100+ mixed-size files (some CSV 1GB, some 10MB). Verify no OOM, all files process without hanging, query relay queries still process in parallel.

### Risks

| Risk | Mitigation |
|---|---|
| Streaming JSON parser complexity — manual JSON streaming is non-trivial; library (`JSONStream`, `ndjson`) adds dependency. | Prefer full-buffer parse if file <100MB (Phase 1 size cap handles this). Only implement streaming if JSON files >100MB are a real use case. Add feature flag to toggle streaming/buffering paths for A/B testing. |
| Reservoir algorithm variance — Fisher-Yates guarantees uniform permutation; Algorithm R gives uniform distribution but preserves encounter order within the reservoir. Platform's AI #1 may be sensitive to sample order. | After reservoir fills, Fisher-Yates shuffle the final 10 samples before returning. One-time O(10) shuffle per column, negligible. Preserves Phase 1's shuffle behavior. |
| Backwards compatibility — existing local-file connections have samples from Phase 1 (Fisher-Yates). Phase 2 streaming may produce slightly different sample distributions. | Samples are metadata only. Re-syncing a file regenerates samples via Phase 2 reservoir; AI #1 re-maps with new samples. Document in release notes that users with existing connections should re-sync for better mapping. |
| Excel files still require full buffering (xlsx limitation). A 10GB Excel file will OOM or hit memory ceiling. | Phase 1 size cap enforces max file size at upload. Add explicit warning: 'Excel >500MB not supported; consider CSV or Parquet'. Plan Phase 3 (ParquetJS or arrow-js for columnar formats) if Excel streaming becomes critical. |
| Timeout value (30s) may be too aggressive for slow platforms or large CSV samples. AI #10 might genuinely need >30s. | Make timeout configurable via `AgentConfig` (default 30s). Add `console.warn` if timeout fires. Monitor production deployments — if timeout rate >5%, increase default to 60s. |

**Effort Estimate:** 3-4 days

**Dependencies:**
- Phase 1 (sampling refactor) — commit 63b5bc5.
- Phase 0 (file size caps in Tauri config) — enforces <500MB per file.
- Node.js streams API (built-in).
- Existing test fixtures (Global_Health_Lab, etc.).

## Phase 3 — JSON-RPC Chunked Response Transport

**Objective:** Implement a chunked JSON-RPC response protocol that breaks large results (>8MB) into frames before transmission, allowing the Rust side to reassemble them without loading entire responses into memory at once. This eliminates silent crashes on large files by respecting the V8 string size limit (~512MB theoretical, ~256MB practical) and Rust `String` allocation constraints. Maintains full backward compatibility: responses under 8MB remain single-frame and work with existing code.

### File Changes

| File | Current Behavior | Change Description |
|---|---|---|
| `packages/agent-core/src/ipc-server.ts` | Lines 44-46 and 74-75: `sendResponse()` and `sendEvent()` call `JSON.stringify()` once on the full response and append a newline. For large results (e.g., 10,000 rows × 50 columns of strings), this creates a single JSON string that may exceed safe transmission limits. | Wrap `sendResponse()` to detect response size and emit chunked frames when payload exceeds 8MB. Frame envelope: `{"jsonrpc":"2.0","result":{"__chunked":true,"chunkId":"uuid","totalChunks":N,"chunkIndex":i,"data":"base64data"},"id":1}`. Responses under threshold emit as-is with no `__chunked` marker. `sendEvent()` unchanged (notifications are fire-and-forget). |
| `apps/desktop/src-tauri/src/lib.rs` | Lines 71-104: `sidecar_rpc` uses `reader.read_line(&mut response_line)` loop, expecting one complete JSON-RPC response per line. For chunked responses, this reads only the first frame and fails to recognize `__chunked` marker. | Extend read loop to detect chunked frames and accumulate them. When `__chunked=true` is detected, enter reassembly mode: cache `(chunkId, totalChunks, chunkIndex)`, read subsequent lines until all chunks received, base64-decode, concatenate, parse the reassembled JSON, return to single-frame mode. Add 30s frame timeout. Single-frame responses (no `__chunked`) bypass reassembly. |

### Test Plan

- Unit test: Verify `serializeWithChunking()` correctly splits a 100MB JSON object into 8MB frames and reconstructs identical JSON.
- Unit test: `ChunkAssembler` correctly reassembles frames in order even if received out-of-order over a long time window.
- Integration test: Send 50MB result (100k rows, 50 columns) via `ipc-server` and confirm Rust reassembler receives complete data.
- Integration test: Send 5MB result (under threshold). Confirm no chunking occurs (single-frame, no `__chunked` marker).
- Integration test: Simulate missing chunk (send frames 1,2,4 of 5). Confirm 30s timeout error with helpful message.
- Integration test: Simulate out-of-order frames (4,2,3,1). Confirm reassembler sorts and reconstructs correctly.
- Regression test: Existing single-frame RPC calls (schema, health, small queries) still work without modification.
- Performance test: Measure CPU and memory during 100MB chunked response. Confirm no spike in Rust `String` allocations.
- Stress test: Send 10 concurrent 50MB queries. Confirm all reassemble correctly without deadlock or corruption.

### Risks

| Risk | Mitigation |
|---|---|
| Base64 encoding inflates JSON payload by ~33%, so 8MB threshold on raw data is actually ~10.7MB of JSON. If Rust allocates the entire base64 string before decoding, this still blows past safe limits. | Set `CHUNK_THRESHOLD` to 6MB (not 8MB) to account for JSON overhead and base64 expansion (targets ~4MB raw chunks). Alternatively, stream base64 decoding in Rust: collect base64 strings into a `Vec`, decode each in-place without materializing the full JSON string. |
| Frame reassembly adds latency — client waits for all chunks before parsing. For 100MB result in 50 chunks, stdio could block if Rust-side read buffer fills. | Chunk size ~8MB serialized JSON (~100k lines). Node.js stdout is line-buffered; set stdio to non-blocking or use custom buffering. Increase chunk size to 16-20MB if the platform can handle V8 strings of that size. |
| Out-of-order frames possible if two large responses are sent in rapid succession. Frame 1 of response A could interleave with Frame 0 of response B. Without unique `chunkId`, reassembly corrupts both responses. | `chunkId` must be globally unique and included in every frame. Use UUID v4 or combination of request id + timestamp. Reassembler keys on `(chunkId, chunkIndex)` to handle concurrent responses. |
| Chunk timeout (30s) may be too strict for slow networks or high CPU. Timeout mid-reassembly leaves Rust waiting, causing Tauri command to hang. | On timeout, clear reassembler and return detailed error: 'Chunked response stalled: received 3/5 chunks for chunkId X in 30s.' Log to stderr. Caller can retry or show helpful message. |
| Backward compatibility — if an old sidecar (no chunking) sends a 50MB single-frame response to a new Rust client expecting chunks, `read_line()` still blocks/allocates. | Phase 3 must be deployed atomically (Rust + Node together). Version the IPC protocol: include `'ipc_protocol_version': '2.0'` in `event.ready` notification. Older Node sidecars never emit `__chunked`, so new Rust code treats them as single-frame (safe, just not scalable). |

**Effort Estimate:** 3-4 days

**Dependencies:**
- Phase 0 (heap budget; not a hard blocker but should be considered for Rust side).
- Node.js process with sidecar running for integration testing.

## Phase 4 — Platform-Side AgentRelay Query Batching (Optional)

**Objective:** Enable the platform-side query relay to batch N pending queries into a single GET poll response and process N result POSTs in a single round-trip, reducing polling overhead and network latency for high-concurrency workloads. This is a performance optimization that complements the agent's existing parallel query execution (`Promise.allSettled` from commit ca66dfe) by consolidating multiple poll cycles into one. **Lower priority than Phases 0-3** because correctness (memory safety, streaming, large-file handling) is already addressed; this is an incremental throughput win for fan-out scenarios like "how many records per source" that spawn 10+ concurrent queries.

### File Changes

| File | Current Behavior | Change Description |
|---|---|---|
| `apps/web/lib/agent-query-relay.ts` | Lines 92-127: `getPendingQueries(tenantId)` returns flat array. Lines 42-88: `submitAgentQuery()` enqueues one query at a time. Lines 132-180: `resolveAgentQuery` and `rejectAgentQuery` process one result at a time. | Add optional `batchId` and `batchGeneration` to `PendingQuery`. Add per-tenant capability registry `{ supportsQueryBatching, agentVersion, lastRegistered }`. `getPendingQueries()` optionally returns `{ batchId, generation, queries: [...] }` when agent supports batching. Support batch result POST format: `{ batchId, results: [...] }`. Maintain backward compat via auto-detection. |
| `apps/web/lib/agent-registry.ts` | Lines 10-27: Simple in-memory map storing `{ url, version, lastSeen }` per tenantId. | Extend registry entry to include `{ url, version, lastSeen, supportsQueryBatching?, reportedCapabilities? }`. Modify `registerAgent` to accept optional `capabilities` object. Detect batching support via version check (>= 0.1.35) OR explicit flag. Add `getAgentCapabilities(tenantId)` export and `cleanupExpiredAgents()` utility. |
| `apps/web/app/api/agent/queries/route.ts` | Lines 24-61: GET handler returns `{ queries: [...] }` flat array. Lines 68-128: POST handler accepts a single query, calls `submitAgentQuery()`, returns `{ data }`. | Modify GET to return batched response when agent supports batching AND `pendingQueries.size >= 2`. Generate `batchId` like `aq_batch_${Date.now()}_${Math.random()}` and assign `batchGeneration` counter. Response: legacy `{ queries: [...] }` OR batched `{ batchId, batchGeneration, queries, batchingSupported: true }`. Add `?batchSize=N` param (default 10, max 100). |
| `apps/web/app/api/agent/queries/[id]/result/route.ts` | Lines 15-54: POST single-result handler. | Keep existing route as-is. Add new route at `apps/web/app/api/agent/queries/batch/result/route.ts` accepting `{ batchId, results: [{ id, data|error, index }, ...] }`. Validate batchId, call `resolveAgentQuery` or `rejectAgentQuery` per result. Return `{ batchId, successCount, failureCount }`. |
| `apps/desktop/sidecar/main.mjs` | Lines 355-469: `startQueryPolling()` polls GET `/api/agent/queries` every 2s, receives flat array, processes via `Promise.allSettled`, posts individual results one-per-query. | Detect `batchId` and `batchGeneration` in GET response. If batching, collect results into `[{ id, data|error, index }, ...]` and POST to `/api/agent/queries/batch/result`. Fall back to individual POSTs if batch POST fails. Advertise capability in registration: `{ batchingSupported: true, agentVersion: '0.1.35+' }`. |

### Test Plan

- Unit: Test `getPendingQueries()` batching logic — mock registry with batching enabled, verify `batchId` and `batchGeneration` assigned, verify batch size respects `?batchSize`.
- Unit: Test backward compatibility — mock registry with batching disabled, verify flat array without `batchId`.
- Unit: Test `resolveAgentQuery()` with batch results — verify batch result POST unmarshals `[{ id, data, index }]` and resolves each promise correctly.
- Unit: Test sidecar batch detection — mock GET response with `batchId`, verify sidecar collects results into batch array and POSTs to `/batch/result`.
- Unit: Test sidecar fallback to legacy — mock GET response without `batchId`, verify individual POSTs.
- Integration: End-to-end with agent registration — (1) agent registers with `batchingSupported=true`, (2) platform receives 10 queries, (3) GET returns batched response, (4) sidecar processes batch and POSTs batch result, (5) all 10 results resolved.
- Integration: Concurrent agents with mixed batching — one supports, one doesn't. Verify both modes handled correctly with no crosstalk.
- Load: 100 pending queries, batch size 20 — verify GET returns 20 with `batchId`, sidecar POSTs batch result in <50ms, subsequent GET returns next batch.
- Load: High-frequency polling (1000 queries/sec across 5 agents) — verify batch POSTs don't exceed Tauri JSON-RPC line length limits.
- Metrics: Run workload with 50 queries / 10 batches. Verify INFO logs show batch sizes, processing times, hit rates.
- Edge case: Empty batch — agent polls but no queries pending. Verify GET returns `{ queries: [] }` (no batch overhead).
- Edge case: Single-query batch — verify still accepted.
- Edge case: Result POST fails for batch — agent retries with same `batchId` without double-resolving.
- Rollback: Disable batching via config flag — verify platform stops returning `batchId` even if agent advertises support.

### Risks

| Risk | Mitigation |
|---|---|
| Breaking change if agent assumes flat array — older agents receive `{ batchId, queries: [...] }` and fail to parse. | Always include `queries` array; only add `batchId`/`batchGeneration` as optional fields. Legacy agents ignore new fields and use `queries`. Backward compatibility automatic — no version check in sidecar required. |
| Batch result POST not yet implemented in sidecar — if platform sends batched response but sidecar doesn't support it, queries time out. | Coordinated rollout: platform ships first (reads batching flag from registry, only uses if agent advertises). Sidecar ships second. During transition, batching is opt-in via agent registration flag. Feature flag to disable server-side. |
| Batch result array ordering — if agent POSTs results out of order or with missing IDs, relay misassigns results. | Each result includes `id` as authority, not array index. Add optional `index` for debugging. Add `validateBatchIntegrity()` to ensure all batch IDs present; log WARN if missing. |
| Network timeout on large batch POST — if batch contains 100 queries and JSON payload exceeds Tauri JSON-RPC line limits, sidecar hangs/crashes. | Enforce max batch size (default 20, configurable). Test batch size limits empirically before shipping. Pre-flight check: if serialized batch result >1MB, split into sub-batches. |
| Batch ID collision — multiple sidecar instances with same timestamp generation could cause result mismatches. | Use `{ Math.random(), Date.now(), tenantId }` for `batchId`. Namespace per `tenantId` in relay map. Use deterministic IDs in tests. |
| Agent doesn't support batch result POSTs but platform sends batched response — agent attempts single-query POSTs failing with 404. | Don't enable platform batching until sidecar ships. Use version-gating: only batch if `getAgentCapabilities(tenantId).supportsQueryBatching === true`. If flag missing, default to legacy. |

**Effort Estimate:** 3-4 days

**Dependencies:**
- Phases 0-3 should be complete or in-flight (memory safety, streaming, large-file handling). This phase is independent of correctness fixes.
- Agent sidecar code must be updated in parallel to handle batch result POST. Platform code alone does not ship without agent support.
- Backward compatibility with agents < 0.1.35 must be maintained (flat array fallback).

## Cross-Cutting Concerns

These tests must run across all phases as gating checks before merge:

- **Memory soak test (24h continuous):** Run agent under steady-state query load (50 queries/min, mixed CSV/JSON/Excel sources, 1MB-40MB file sizes) for 24 hours. Capture heap snapshot every hour. Verify (a) no heap growth trend (RSS plateaus), (b) no file-handle accumulation (check `lsof`), (c) no sidecar restarts.
- **Parallel-query stress test:** Issue 100 concurrent queries via the relay (fan-out from a single Cube.js measure-first call). Verify (a) all 100 results return without loss, (b) no deadlock, (c) wall-clock < sequential equivalent, (d) memory peaks bounded.
- **File-size sweep:** Run query and upload against synthetic CSV/JSON/Excel files at: 1MB, 10MB, 25MB, 50MB (boundary), 51MB (should reject), 100MB (should reject), 1GB (CSV only, streaming path). Verify behavior at each size: success below cap, structured FILE_TOO_LARGE error above cap, streaming reservoir maintains constant memory regardless of size.
- **Concurrent file-write race:** Simulate user dropping a file mid-sync. Verify uploader doesn't crash on partial reads, file-size check catches incomplete files.
- **IPC stress test:** Send 10 concurrent 50MB query results through the chunked IPC. Verify Rust correctly demultiplexes by `chunkId`, no frame interleaving corrupts results, no Rust panics.
- **Sidecar crash recovery:** Kill the sidecar process mid-query (SIGKILL). Verify Tauri detects exit, restarts cleanly, in-flight platform queries time out gracefully with structured error.
- **Backward-compat smoke:** Old platform (without batch endpoints) + new agent (with batch support). Verify agent falls back to single-result POSTs. New platform + old agent (no batch advertise). Verify platform returns flat array. Test both matrix corners.
- **End-to-end one-click setup:** Run the full one-click setup flow (connect → map → cube generate → query) on a real tenant after all phases land. Verify accuracy unchanged from baseline (commit 3707760 systemic-fix test log is the reference).

## Rollout Plan

### Merge Order

1. **Phase 0 first.** Memory safety must land before any streaming or chunking work because it provides the test infrastructure (error codes, heap caps, structured errors) that later phases verify against. Phase 0 is also the smallest blast radius — it adds checks without changing parsing logic. Canary: deploy to a single staging tenant for 48 hours.

2. **Phase 1 second.** Streaming readers replace the query-path parsers but keep the same async return shape. Phase 1 depends on Phase 0's file-size caps. Canary: deploy to staging, run the cross-cutting file-size sweep, monitor for regressions in `totalRows` semantics or filter behavior.

3. **Phase 2 third.** Reservoir sampling replaces Phase 1's full-file buffering in the uploader (separate from query-server). Can ship independently of Phase 3. Canary: re-sync 5 existing connections, compare AI #1 mapping decisions against pre-Phase 2 baseline. Mapping confidence should be unchanged within ±5%.

4. **Phase 3 fourth.** Chunked IPC is the most complex change and requires coordinated Rust + Node deploy. Atomic ship. Canary: deploy a staging-only build with `ipc_protocol_version: '2.0'`. Run the IPC stress test and verify backward-compat (old sidecar + new Rust).

5. **Phase 4 last (optional).** Batching is throughput-only. Defer until Phases 0-3 are stable in production for at least 2 weeks. Canary: enable batching for one tenant via capability flag, monitor poll latency and result POST throughput.

### Verification Between Phases

- **After Phase 0:** Confirm structured errors flow end-to-end. Trigger FILE_TOO_LARGE from a real file upload and verify the platform UI shows a user-friendly message (not a generic 500). Confirm sidecar's NODE_OPTIONS heap cap is honored on the bundled SEA binary.
- **After Phase 1:** Run streaming-vs-buffered comparison on a 500MB CSV: same filter+limit query through old code path and new. Verify (a) identical row count, (b) identical row contents, (c) 10x+ memory reduction. Verify Excel hard cap shows correct user message in UI.
- **After Phase 2:** Re-sync the Global_Health_Lab fixture and verify sampled values are representative (not all nulls in sparse columns). Run an existing tenant's mapping baseline (commit 3707760) and compare concept names + confidence to ensure no AI #1 regression.
- **After Phase 3:** Issue a 100MB query result and verify Rust receives complete data with zero corruption. Inspect heap of Rust process during chunk reassembly — should peak below 200MB regardless of total response size.
- **After Phase 4 (if shipped):** Compare batch vs single-result throughput in staging — should see 3-5x reduction in HTTP round-trips for fan-out queries.

### Canary Signals to Watch

- **Sidecar SIGABRT (status 134):** Any occurrence means heap cap fired. Investigate file size, query concurrency, or NODE_OPTIONS not being honored.
- **HTTP 413 rate on `/query`:** Expected for files >50MB. If rate spikes after Phase 1 deploy on files <50MB, the cap is misconfigured.
- **`AbortError` rate in `getStructureDecisionFromAI`:** If rate >5%, AI #10 is too slow or the timeout is too aggressive — consider raising default to 60s.
- **Chunked reassembly timeout rate:** Any occurrence means a chunk was lost in stdio. Investigate sidecar stability and stdio buffering.
- **Query result POST failures with 'parse_error' code:** Indicates streaming parsers are choking on real-world data. Capture the failing payload for diagnosis.
- **AI #1 mapping confidence drift:** After Phase 2, run weekly mapping baseline comparison. Confidence should stay within ±5% of pre-Phase 2.
- **Memory RSS plateau:** During 24h soak test, RSS should level off within first 2 hours and stay flat. Any upward trend = leak.

## Open Questions

These items could not be fully resolved during investigation and need product/eng decisions before or during implementation:

1. **`totalRows` semantics after Phase 1.** Current code reports file row count (pre-filter). Phase 1 changes to filtered row count for simplicity. Does Cube.js or the platform UI depend on the pre-filter total for pagination math? If yes, we need a separate `totalRowsBeforeFilter` field or a `?countAll=true` query parameter. **Decision needed before Phase 1 ships.**

2. **Excel hard cap policy.** Phase 1 caps Excel at 10MB with a "convert to CSV" message. Phase 2 keeps the cap. Phase 3 (or beyond) would adopt `exceljs` for streaming Excel support. Is the 10MB cap acceptable for the user base, or do we need to prioritize `exceljs` integration sooner? **Track Excel-rejection rate post-Phase 1 to decide.**

3. **NODE_OPTIONS on SEA binaries.** Phase 0 assumes `NODE_OPTIONS=--max-old-space-size=800` will be honored by the Tauri-bundled Node SEA sidecar. This may not work on SEA. Fallback is passing `--max-old-space-size` as a command-line argument from Rust. **Verify on target deployment before Phase 0 merges.**

4. **Chunked response threshold tuning.** Phase 3 uses 8MB threshold (or 6MB after base64 overhead consideration). Larger threshold = fewer frames but bigger Rust allocations. Smaller threshold = more frames but more stdio overhead. **Run IPC stress test with several thresholds (4MB, 8MB, 16MB) to find optimal value for production heap settings.**

5. **Batch size for Phase 4.** Default 20 queries per batch. Maximum 100. Is this right for the high-concurrency fan-out scenarios? Should it adapt based on average query result size to avoid blowing up batch POST payload? **Decide once Phase 4 starts; can be tuned via config.**

6. **Coordinating Phase 4 deploy with agent versions.** Platform code can ship before agent supports batching, but the platform must read the capability flag from the registry. Older agents (no batch advertise) keep flat-array mode. Does the platform need a kill-switch to force flat-array mode even for batching-capable agents (e.g., during an incident)? **Add server-side feature flag to Phase 4 design.**

7. **Re-sync policy after Phase 2.** Existing connections have Phase 1 (Fisher-Yates on first-N-rows) samples. Phase 2 streaming samples may differ slightly. Should the platform auto-trigger re-sync for all tenants on Phase 2 deploy, or wait for users to manually re-sync? **Product decision — auto-resync has cost but ensures consistent AI #1 quality.**

8. **Error code taxonomy.** Phase 0 introduces `FILE_TOO_LARGE`, `FILE_NOT_FOUND`, `PARSE_ERROR`, `MEMORY_LIMIT_EXCEEDED`, `UNKNOWN`. Does the platform backend already have a competing error code taxonomy (e.g., for Nango integrations or DB connection errors)? **Coordinate with backend team to align taxonomies before Phase 0 merges; otherwise platform UI may have to translate between agent error codes and its own.**

9. **Telemetry for memory pressure.** Should the sidecar emit heap usage metrics (e.g., `process.memoryUsage()` every 30s) back to the platform as health events? This would let us detect leaks proactively, but adds noise. **Defer to post-Phase 3 if cross-cutting soak test reveals leak patterns.**

10. **Tauri Rust heap budgeting.** Phase 3 reassembles base64-decoded chunks into a single `Vec<u8>` then a `String`. For a 256MB response, this is 256MB + transient base64 buffer. Should Rust apply a hard cap (e.g., 512MB) and fail clean if a response exceeds it, rather than letting the OS OOM-kill the Tauri process? **Add to Phase 3 design if Rust panic-on-allocation is a real risk on the target platforms.**
