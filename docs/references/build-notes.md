# Build Notes: Things That Surprised Us

A log of non-obvious lessons learned building EvolvSQL. Not a feature list, not a changelog. The stuff you only find out by writing the code and watching it break.

## The Bugs You Don't See Coming

### `try/after` does nothing across `:proc_lib.hibernate`

Wrapped a connection lifecycle in `try ... after release_counter() end` to guarantee cleanup. Looked bulletproof. It wasn't.

`:proc_lib.hibernate/3` discards the entire call stack. The process wakes up in a fresh stack frame inside `__wake__/2`. Your `after` clause is gone. Every exit path after the first hibernation leaks.

Since EvolvSQL hibernates connections after 30 seconds idle, virtually every real connection hibernated at least once. After 100 connections cycled through, the counter sat permanently at the limit and rejected everything. Complete DoS from a one-line cleanup pattern.

Fix: monitor from the listener with `Process.monitor/1` and release on `:DOWN`. The monitor lives outside the hibernating process, so it survives.

If you ever see `try/after` in a process that hibernates, it's wrong. Doesn't matter how careful the rest of the code is.

### `cache.clear()` on overflow is a trap

The original parse cache: `if cache.len() >= 1024 { cache.clear() }`. Looks reasonable. Bounded memory, simple code.

Failure mode: any workload with 1025+ unique queries causes the cache to nuke itself constantly. Cold start for every query right after the clear. Not a slowdown, a complete cache bypass on a workload pattern that's extremely common (analytics, reporting, ad-hoc queries).

LRU eviction kicks one entry on each insert at capacity. Same memory bound, infinitely better behavior. The `lru` crate is 200 lines, do this every time.

### Mutex vs RwLock for an LRU cache

First instinct switching from HashMap to LRU: keep the `RwLock`. LRU is read-heavy after all.

Wrong. LRU mutates on every "read" because it has to update the recency list. There's no actual read path. `RwLock<LruCache>` would force `.write()` for every lookup, giving you all the contention of a Mutex with extra overhead. Just use `Mutex`.

This caught us: the first PR held the mutex during `pg_query::parse()` on cache misses, serializing every concurrent query in the BEAM behind one parse call. Drop-before-parse is non-negotiable, even with the simpler lock type.

### Aggregate ORDER BY sorted lexicographically

`SELECT name, SUM(price) FROM t GROUP BY name ORDER BY 2` returned rows in the wrong order. SUM was producing correct numeric values, ORDER BY was correctly resolving to column 2. Both pieces fine in isolation.

The bug: `exec_select_aggregate` returned `Vec<Option<String>>` rows. The post-filter then reparsed each cell with `parse_text_to_value(s, oid)`. The OID for SUM defaulted to 25 (text). So `[100, 5, 42, 9]` came back as `["100", "5", "42", "9"]` and got compared as strings: `"100" < "42" < "5" < "9"`.

The string round-trip is the real architectural smell. We patched the OID inference (resolve column type from the FuncCall argument), but the right fix is to keep aggregate results in `ArenaValue` form throughout. Filed for future work.

### `string_agg` panicked on empty tables only

`compute_string_agg` accessed `rows[0]` to evaluate the delimiter expression. Worked fine for every test that had data. Crashed on `SELECT string_agg(name, ',') FROM empty_table` because the no-GROUP-BY path creates exactly one group with zero rows.

Every other aggregate handled empty groups correctly. `string_agg` was the only one that needed `rows[0]` for delimiter evaluation, and it forgot the guard. Lesson: when one function in a family has a different signature, audit it separately.

### `CREATE TABLE IF NOT EXISTS` leaked sequences

SERIAL columns auto-create sequences during column parsing. The `if_not_exists` check happened later, after parsing. If the table already existed and we hit the early return, the sequences we created for the parse were already in the catalog. Forever.

A workload that did `CREATE TABLE IF NOT EXISTS x (id serial, ...)` on every startup would leak one sequence per call. Took Devin to spot this. Easy fix (drop the sequences on early return), but the lesson is that `IF NOT EXISTS` paths need full cleanup, not just an early return.

### Fast equality filter ignored TypeCast

`WHERE id = '5'::int` returned 0 rows. The filter optimization extracted `Text("5")` from the AST (eval_const strips TypeCast nodes), then compared it against `Int` columns. No cross-type match arm, so no rows ever matched.

The slow path was correct. The fast path silently produced wrong results. This is the worst class of optimization bug: it doesn't crash, it doesn't error, it just lies. The fix was a one-line bail-out: if either side is a TypeCast, fall back to the slow path. Always check that your fast paths handle every input the slow path does, not just the common ones.

### `INTERSECT` and `EXCEPT` weren't deduplicating

PostgreSQL: `INTERSECT` is set semantics (dedup), `INTERSECT ALL` is multiset (keep dups). Same for EXCEPT.

We had the inverse: both always kept duplicates. UNION had the dedup logic, INTERSECT and EXCEPT didn't, even though they're in the same match block. Easy to miss when you write `union_intersect_except` as one feature but only test UNION's dedup behavior.

### `left('hello', -2)` should return `'hel'`

PostgreSQL has a weird convention: negative `n` in `left()` means "all but the last `|n|` characters". Same idea for `right()`. We did `*i as usize` which wraps -2 to `usize::MAX - 1`, then `.take(huge)` returns the entire string. Tests passed because no test used negative arguments. Devin caught it during static review.

Always cast signed-to-unsigned through an explicit branch. Never `as usize` on values that could be negative.

## The Architectural Surprises

### CTEs without changing any function signatures

Adding CTEs needed to make `WITH` results visible to every part of the query pipeline (FROM, subqueries, set operations). The natural shape is to thread a `cte_registry` parameter through every signature: `eval_expr`, `execute_from`, `exec_select_raw`, etc. Dozens of changes.

Better idea: stick the registry in `QueryArena`. The arena is already threaded everywhere because it owns interned strings and value buffers. Zero new parameters, zero broken signatures, CTEs visible everywhere by construction. Total diff for the threading was zero lines.

The catch: arena lifetimes. CTE rows initially stored as `Vec<Vec<ArenaValue>>` (offsets into the arena's byte buffers). When a sub-query cloned the arena, the offsets became dangling. Switched to `Vec<Vec<Value>>` (heap-allocated, self-contained) and the problem went away. ArenaValue is a perf optimization, not a representation choice; use `Value` whenever you need data to survive arena boundaries.

### CTE scope leakage from a global registry

First version: child SELECTs added their CTEs to the shared registry. A subquery that defined `WITH t AS (...)` would leak `t` into the parent's namespace. Devin caught this in the same PR.

Fix: snapshot the registry at every `exec_select_raw` entry, restore on exit. This puts SQL scoping rules back in place via a wrapper function: `exec_select_raw` is the snapshot/restore wrapper, `exec_select_raw_body` is the actual logic. Same pattern works for any "global state with lexical scoping" requirement.

### Atomic UPSERT via clone-and-swap

Bulk UPSERT with constraint checks needs all-or-nothing semantics: if row 50 of 100 violates a constraint, rows 1-49 should not be visible.

Most databases solve this with transactions. We don't have transactions. The pattern that worked: clone the entire row vector and indexes, mutate the clones, and only swap them in if every row succeeds. Same pattern as functional persistent data structures, applied to per-table mutation.

For 1M-row tables this is going to be expensive. For 1K-row tables it's free. The right call until we have proper MVCC.

### Hash join was already in the codebase

While planning the "implement hash join for equi-joins" PR, we discovered `try_equi_hash_join` already existed in `executor.rs`. It had been written but never wired into the general join path - only the specific RangeVar path used it. The 7,500-line monolithic file made this invisible. Splitting into 36 modules surfaced the function immediately.

Architectural lesson: code you can't see doesn't exist. Splitting executor.rs revealed ~1,400 lines of duplication that we eliminated for free during the refactor. The split wasn't a refactor, it was an accidental code review.

### Window functions slot between WHERE and projection

PostgreSQL window functions have a specific position in the SELECT pipeline:

```
FROM -> WHERE -> GROUP BY -> HAVING -> WINDOW -> SELECT (project) -> DISTINCT -> ORDER BY -> LIMIT
```

The window step needs full input rows (post-WHERE, pre-projection) because the window can reference any column, even columns not in SELECT. We get this by computing window values into temporary slots appended to each row, then mapping them through `window_positions` during projection.

What threw us: window functions also need their own ORDER BY (within the OVER clause) which is independent of the query-level ORDER BY. Two completely separate sort operations on the same data, with the window's sort happening per partition. This is one place where the SQL semantics actually do call for what looks like a wasteful pass.

### RANGE frames use peer groups, not row positions

`LAST_VALUE(x) OVER (ORDER BY ts)` should return the value from the last row that has the same `ts` as the current row, not the literal next row. PostgreSQL's default RANGE frame is `BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, where CURRENT ROW means "the last row in the current peer group" (rows with equal ORDER BY values).

We initially used row position. Tests passed because none of them had ties. Devin caught it on review by asking "what does `LAST_VALUE` return when ties are present?". The fix was a `last_peer_pos()` function that walks forward to find the end of the peer group.

## Process Lessons

### Devin reviews are worth more than tests

PR #35 had 236 passing tests. Devin found 8 correctness bugs that the tests didn't catch. Round 2 found 3 more after the first round of fixes. Round 3 confirmed all clean.

Tests verify what you thought to test. A static reviewer reads every line of code and asks "what could go wrong here". These cover different failure modes. We learned to treat "all tests pass" as necessary but nowhere near sufficient.

### Devin admits its own false positives

In round 2 of PR #35, Devin re-checked the round 1 fixes and explicitly marked 4 of its 5 original findings as "false positive, the code already does this correctly". This was after we'd already pushed the fixes - Devin wasn't sycophantic about the changes, it independently verified.

The takeaway: the AI reviewer doesn't have memory of its previous claims, so its second pass is a true second opinion, not a confirmation bias. We started intentionally pushing fixes and asking for re-reviews specifically to get this independent re-check.

### "Persistence before performance"

When we self-critiqued the scalability plan, the original ordering was: vectorize execution, hash joins, optimizer, then disk persistence in phase 3. After research into DuckDB, CockroachDB, TiDB, MariaDB scaling, this got inverted.

The insight: a fast in-memory database that loses data on restart is a cache, not a database. Nobody uses an unproven cache for production workloads, so the "fast version" never gets exercised on real data, so the fast version's bottlenecks aren't real-workload bottlenecks. WAL has to come first because it gates everything that comes after.

The corollary insight: vectorization without columnar storage is 2-5x, not 10-100x. Row-wise iteration over a vectorized engine still pays the cache miss every `sizeof(Row)` bytes. DuckDB's 100x gains come from the trifecta: columnar storage + vectorized execution + morsel-driven parallelism. Vectorizing first looks like a quick win but caps your eventual ceiling.

### The 100-line file limit is a forcing function

A user-level rule: every file under 100 lines, or explicitly justify. Felt arbitrary and annoying when applied to a 7,563-line executor. Forced us into 36 focused modules. The result was strictly better than a "natural" split into 6-8 modules of 1000 lines each:

- Easier to find any specific function (the file name tells you what's inside)
- Easier to review (small diff per file)
- Easier to test (the boundaries become natural module boundaries)
- The split itself eliminated 1,400 lines of duplication that hid in the monolith

The arbitrary rule produced better architecture than thoughtful "natural" boundaries. Worth keeping.

### The exact same rule applied to tests

`tests.rs` was 2,851 lines, 208 test functions. Splitting it into 42 files (each under 100 lines) by feature area was the cleanest review experience of the entire session: Devin's review came back with zero findings. When tests are organized by feature, the structure of the test suite mirrors the structure of the code, and bugs cluster geographically.

## BEAM-Specific Surprises

### `:atomics` + `:persistent_term` is the answer for shared counters

Looking for a connection counter, the obvious BEAM pattern is a GenServer with cast/call. That introduces message passing overhead per connection. ETS is faster but still goes through the ETS table manager.

`:atomics.new/1` creates a CPU-cache-friendly mutable integer with `atomics:add_get/3` for compare-and-swap. `:persistent_term` stores the atomic ref globally with zero per-lookup cost (it's literally a constant in the BEAM heap). Together: O(1) increment with no message passing, no ETS lookup, no GenServer round-trip.

Use this for any "BEAM-wide counter that updates frequently and reads occasionally". Connection counts, request rates, anything you'd reach for an atomic integer for in a non-BEAM language.

### `Process.monitor` survives hibernate

Building on the `try/after` failure: monitors live in the runtime, outside the hibernating process. When the connection hibernates, the listener still has a live monitor on it. When the connection terminates (via any path), the monitor fires. This is the correct primitive for "do X when this process eventually exits, no matter how".

## The Plan Self-Critique That Saved Two Months of Work

Before approving the scalability roadmap, we ran it through a "find 9 holes in this plan" pass. The original plan had:

1. Vectorization without columnar storage (would have been 5% of expected gains)
2. Persistence as a phase 3 afterthought (the database would never see real workloads)
3. Hash join listed as new work when it already existed
4. Executor split as PR 4 when it should be PR 1 (it unblocks everything else)
5. Query optimizer ranked below vectorization (wrong: optimizer is higher ROI)
6. No concurrent test infrastructure (we proposed MVCC with no way to verify it)
7. HNSW full rebuild on every mutation as a side note (it's actually the dominant cost)
8. No memory limits (one large INSERT could OOM the process)
9. `scan_with` for the general path (won't help, JOINs need rows outside the lock)

Catching these on paper saved us from building real things in the wrong order. The single most useful exercise was asking "what would I think if a different team showed me this plan and asked for review". The default mode of "approve and execute" missed all 9.

## Things We Still Don't Know

- How does our parse cache hit rate look on a real workload? We've never measured it.
- The aggregate string round-trip almost certainly costs significant CPU. Worth keeping until we measure how much.
- The clone-based UPSERT pattern is fine at 1K rows, broken at 1M. We don't know where the crossover is.
- Window function evaluation is row-at-a-time. For analytical queries this is probably the biggest single optimization opportunity. Never benchmarked.
- HNSW full rebuild on UPDATE/DELETE is a known performance cliff. We've avoided exercising it.

The honest answer to "is EvolvSQL fast" is "we don't know yet". We have a working SQL engine with good correctness coverage. Performance work starts with measurement, and we haven't measured.
