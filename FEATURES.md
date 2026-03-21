# Complete Feature Specification
## BEAM + Rust PostgreSQL-Compatible Database

This document maps EVERY PostgreSQL feature (v6.0 → v18) to our implementation plan.
Each feature is categorized by priority tier and assigned to a layer (BEAM or Rust).

---

## Implementation Tiers

- **T0 — Foundation**: Must exist for `psql` to connect and basic CRUD to work
- **T1 — Core SQL**: Features needed for most applications and ORMs to function
- **T2 — Production**: Features needed for real production workloads
- **T3 — Advanced**: Power-user features, analytics, specialized use cases
- **T4 — Ecosystem**: Extensions, FDW, procedural languages, advanced replication

---

## Architecture Assignment

- **BEAM**: Connection management, wire protocol, query routing, replication coordination, supervision
- **RUST**: Parser, analyzer, planner, executor, storage engine, MVCC, indexes, type system
- **BOTH**: Features spanning the boundary (e.g., LISTEN/NOTIFY, distributed transactions)

---

## T0 — Foundation (The "Hello, psql" Tier)

### Wire Protocol (BEAM)
- [ ] Frontend/Backend Protocol v3 (PG 7.4, 2003)
- [ ] StartupMessage / AuthenticationOk handshake
- [ ] Simple Query protocol (Query → RowDescription → DataRow → CommandComplete → ReadyForQuery)
- [ ] Extended Query protocol (Parse/Bind/Describe/Execute/Sync)
- [ ] ErrorResponse with field codes
- [ ] ParameterStatus messages (server_version, client_encoding, etc.)
- [ ] Terminate handling
- [ ] SSL/TLS negotiation
- [ ] Protocol v3.2 — 256-bit cancel request keys (PG 18)

### Core SQL (RUST)
- [ ] Parser — PostgreSQL grammar (via libpg_query FFI or native port)
- [ ] CREATE TABLE / DROP TABLE
- [ ] INSERT / SELECT / UPDATE / DELETE
- [ ] WHERE clauses (=, <>, <, >, <=, >=, AND, OR, NOT, IS NULL, IS NOT NULL)
- [ ] ORDER BY, LIMIT, OFFSET
- [ ] Basic expressions and operators (+, -, *, /, ||, LIKE, ILIKE, IN, BETWEEN)
- [ ] Column aliases (AS)
- [ ] Table aliases
- [ ] NULL handling (three-valued logic)

### Core Types (RUST)
- [ ] int2 (smallint) — OID 21
- [ ] int4 (integer) — OID 23
- [ ] int8 (bigint) — OID 20
- [ ] float4 (real) — OID 700
- [ ] float8 (double precision) — OID 701
- [ ] numeric/decimal — OID 1700
- [ ] bool — OID 16
- [ ] text — OID 25
- [ ] varchar — OID 1043
- [ ] char — OID 1042
- [ ] bytea — OID 17
- [ ] oid — OID 26
- [ ] void — OID 2278
- [ ] unknown — OID 705

### Core Storage (RUST)
- [ ] Pluggable storage trait (StorageEngine)
- [ ] Default storage backend (SQLite, redb, or custom B-tree)
- [ ] Basic ACID transactions (BEGIN/COMMIT/ROLLBACK)
- [ ] Auto-commit for single statements
- [ ] Sequential scan
- [ ] Undo-log MVCC (NOT in-heap — this is our key differentiator)
- [ ] 64-bit transaction timestamps (NOT 32-bit XIDs)
- [ ] Direct I/O (O_DIRECT) — no double buffering

### Core Catalog (RUST — virtual tables)
- [ ] pg_class (tables, indexes, sequences, views)
- [ ] pg_attribute (columns)
- [ ] pg_type (types with correct OIDs)
- [ ] pg_namespace (schemas: public, pg_catalog)
- [ ] pg_database
- [ ] information_schema.tables
- [ ] information_schema.columns

### Connection Management (BEAM)
- [ ] GenServer per connection (~2KB each)
- [ ] Supervisor tree for connection lifecycle
- [ ] Connection limit configuration
- [ ] Graceful shutdown

### Authentication (BEAM)
- [ ] Trust (no password)
- [ ] MD5 password (PG 7.2) — for backward compat
- [ ] SCRAM-SHA-256 (PG 10)

---

## T1 — Core SQL (ORM Compatibility Tier)

### DDL (RUST)
- [ ] ALTER TABLE ADD COLUMN
- [ ] ALTER TABLE DROP COLUMN (PG 7.3)
- [ ] ALTER TABLE ALTER COLUMN TYPE (PG 8.0)
- [ ] ALTER TABLE ADD/DROP CONSTRAINT
- [ ] ALTER TABLE RENAME
- [ ] CREATE/DROP SCHEMA (PG 7.3)
- [ ] CREATE/DROP INDEX
- [ ] CREATE INDEX CONCURRENTLY (PG 8.2)
- [ ] Schemas / namespaces (PG 7.3)
- [ ] IF EXISTS / IF NOT EXISTS clauses
- [ ] CASCADE / RESTRICT (PG 7.3)

### DML (RUST)
- [ ] INSERT ... RETURNING (PG 8.2)
- [ ] UPDATE ... RETURNING (PG 8.2)
- [ ] DELETE ... RETURNING (PG 8.2)
- [ ] UPSERT — INSERT ... ON CONFLICT DO UPDATE/NOTHING (PG 9.5)
- [ ] MERGE (PG 15)
- [ ] Multi-row VALUES (PG 8.2)
- [ ] COPY TO/FROM with CSV (PG 8.0)
- [ ] TRUNCATE

### Queries (RUST)
- [ ] JOINs — INNER, LEFT, RIGHT, FULL OUTER, CROSS (PG 7.0-7.1)
- [ ] Subqueries in WHERE, FROM, SELECT
- [ ] EXISTS / NOT EXISTS
- [ ] UNION / INTERSECT / EXCEPT (and ALL variants)
- [ ] GROUP BY / HAVING
- [ ] DISTINCT / DISTINCT ON
- [ ] Common Table Expressions — WITH (PG 8.4)
- [ ] WITH RECURSIVE (PG 8.4)
- [ ] Data-modifying CTEs (PG 9.1)
- [ ] CTE inlining — MATERIALIZED / NOT MATERIALIZED (PG 12)
- [ ] LATERAL joins (PG 9.3)
- [ ] Window functions — OVER, PARTITION BY, ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, etc. (PG 8.4)
- [ ] Window RANGE/GROUPS modes, frame exclusion (PG 11)
- [ ] GROUPING SETS, CUBE, ROLLUP (PG 9.5)
- [ ] FETCH FIRST ... WITH TIES (PG 13)

### Constraints (RUST)
- [ ] PRIMARY KEY
- [ ] UNIQUE
- [ ] NOT NULL
- [ ] CHECK constraints
- [ ] FOREIGN KEY with ON DELETE/UPDATE CASCADE/SET NULL/SET DEFAULT/RESTRICT (PG 7.0)
- [ ] Deferrable constraints (INITIALLY DEFERRED/IMMEDIATE)
- [ ] Exclusion constraints (PG 9.0)
- [ ] UNIQUE NULLS NOT DISTINCT (PG 15)
- [ ] NOT ENFORCED constraints (PG 18)
- [ ] Temporal constraints — WITHOUT OVERLAPS (PG 18)
- [ ] NOT NULL as first-class object in pg_constraint (PG 18)

### Indexes (RUST)
- [ ] B-tree (default, with correct operator classes)
- [ ] B-tree deduplication (PG 13)
- [ ] B-tree skip scans (PG 18)
- [ ] Covering indexes — INCLUDE clause (PG 11)
- [ ] Partial indexes (PG 7.2)
- [ ] Expression indexes
- [ ] Unique indexes
- [ ] Index-only scans (PG 9.2)
- [ ] Bitmap index scans (PG 8.1)

### Additional Types (RUST)
- [ ] date — OID 1082
- [ ] time — OID 1083
- [ ] timetz — OID 1266
- [ ] timestamp — OID 1114
- [ ] timestamptz — OID 1184
- [ ] interval — OID 1186
- [ ] uuid — OID 2950
- [ ] json — OID 114 (PG 9.2)
- [ ] jsonb — OID 3802 (PG 9.4)
- [ ] ARRAY types for all base types
- [ ] ENUM types (PG 8.3)
- [ ] Serial / BigSerial (sequence-backed)
- [ ] Identity columns (PG 10)

### Transactions (RUST)
- [ ] Savepoints — SAVEPOINT / ROLLBACK TO / RELEASE (PG 8.0)
- [ ] Transaction isolation levels (READ COMMITTED, REPEATABLE READ, SERIALIZABLE)
- [ ] Serializable Snapshot Isolation — SSI (PG 9.1)
- [ ] SELECT FOR UPDATE / FOR SHARE (row locking)
- [ ] SKIP LOCKED (PG 9.5)
- [ ] Advisory locks (PG 8.2)
- [ ] COMMIT AND CHAIN / ROLLBACK AND CHAIN (PG 12)
- [ ] transaction_timeout (PG 17)

### Functions & Expressions (RUST)
- [ ] Built-in string functions (length, substring, trim, upper, lower, concat, replace, etc.)
- [ ] Built-in math functions (abs, ceil, floor, round, power, sqrt, mod, etc.)
- [ ] Built-in date/time functions (now(), current_timestamp, date_trunc, extract, age, etc.)
- [ ] COALESCE, NULLIF, GREATEST, LEAST
- [ ] CASE WHEN / THEN / ELSE / END
- [ ] CAST / :: type casting
- [ ] Aggregate functions (COUNT, SUM, AVG, MIN, MAX, array_agg, string_agg, bool_and, bool_or)
- [ ] FILTER clause for aggregates (PG 9.4)
- [ ] Ordered-set aggregates — percentile_cont, percentile_disc, mode (PG 9.4)
- [ ] Statistical aggregates — var_pop, stddev_pop, regr_*, corr, covar_* (PG 8.2)
- [ ] gen_random_uuid() (PG 13)
- [ ] uuidv7() (PG 18)

### Utility (RUST/BEAM)
- [ ] EXPLAIN / EXPLAIN ANALYZE (PG 7.2)
- [ ] SET / SHOW / RESET for GUC parameters (PG 7.1)
- [ ] PREPARE / EXECUTE / DEALLOCATE (PG 7.3)
- [ ] DO anonymous code blocks (PG 9.0)
- [ ] LISTEN / NOTIFY with payload (PG 9.0) — BEAM native pub/sub
- [ ] ALTER SYSTEM (PG 9.4)
- [ ] COMMENT ON
- [ ] \d, \dt, \di etc. (psql catalog queries — compatibility via pg_catalog)

---

## T2 — Production (Real Workload Tier)

### Views (RUST)
- [ ] CREATE VIEW / DROP VIEW
- [ ] Auto-updatable simple views (PG 9.3)
- [ ] Security barrier views (PG 9.2)
- [ ] Materialized views (PG 9.3)
- [ ] REFRESH MATERIALIZED VIEW CONCURRENTLY (PG 9.4)

### Partitioning (RUST)
- [ ] Declarative RANGE partitioning (PG 10)
- [ ] Declarative LIST partitioning (PG 10)
- [ ] HASH partitioning (PG 11)
- [ ] Default partitions (PG 11)
- [ ] Automatic row movement between partitions (PG 11)
- [ ] Partitionwise joins and aggregates (PG 11)
- [ ] Constraint exclusion / partition pruning
- [ ] Indexes on partitioned tables (PG 11)
- [ ] FK references to partitioned tables (PG 12)

### JSON/JSONB (RUST)
- [ ] JSONB subscripting — jsonb['key'] (PG 14)
- [ ] JSONB operators (->>, ->, #>, @>, ?, ?|, ?&, ||, -)
- [ ] JSONB functions (jsonb_set, jsonb_build_object, jsonb_agg, jsonb_each, etc.)
- [ ] SQL/JSON path language — jsonpath (PG 12)
- [ ] SQL/JSON constructors — JSON_ARRAY, JSON_OBJECT, JSON_ARRAYAGG, JSON_OBJECTAGG (PG 16)
- [ ] SQL/JSON query — JSON_TABLE, JSON_EXISTS, JSON_QUERY, JSON_VALUE (PG 17)
- [ ] IS JSON predicate (PG 16)

### Additional Index Types (RUST)
- [ ] GIN — Generalized Inverted Index (PG 8.2) — for JSONB, arrays, full-text
- [ ] GiST — Generalized Search Tree (improved through many versions) — for geometric, range, full-text
- [ ] BRIN — Block Range Index (PG 9.5) — for large naturally ordered tables
- [ ] SP-GiST — Space-Partitioned GiST (PG 9.2) — for non-balanced structures
- [ ] Hash index (WAL-logged since PG 10)
- [ ] GiST KNN search (PG 9.1)
- [ ] BRIN bloom filters, multi min/max (PG 14)

### Full-Text Search (RUST)
- [ ] tsvector / tsquery types
- [ ] to_tsvector(), to_tsquery(), plainto_tsquery(), websearch_to_tsquery() (PG 11)
- [ ] GIN indexes for full-text
- [ ] Phrase search — <-> operator (PG 9.6)
- [ ] Text search configurations, dictionaries, parsers

### Range Types (RUST)
- [ ] int4range, int8range, numrange, tsrange, tstzrange, daterange (PG 9.2)
- [ ] Range operators (&&, @>, <@, <<, >>, etc.)
- [ ] Multirange types (PG 14)
- [ ] GiST/SP-GiST indexes for ranges

### Security (BEAM/RUST)
- [ ] Row-Level Security policies (PG 9.5)
- [ ] Column-level privileges (PG 8.4)
- [ ] GRANT / REVOKE (tables, schemas, functions, sequences)
- [ ] Roles system — users and groups unified (PG 8.1)
- [ ] SECURITY DEFINER functions
- [ ] SSL/TLS connections
- [ ] pg_hba.conf-style access control
- [ ] OAuth authentication (PG 18)
- [ ] Predefined roles (pg_read_all_data, pg_write_all_data, pg_monitor, etc.)

### Performance (RUST)
- [ ] Query planner — cost-based optimization
- [ ] Join order optimization
- [ ] Parallel sequential scans (PG 9.6)
- [ ] Parallel index scans (PG 10)
- [ ] Parallel joins — hash, merge, nested loop (PG 9.6-10)
- [ ] Parallel aggregates (PG 9.6)
- [ ] Parallel UNION (PG 11)
- [ ] Incremental sorting (PG 13)
- [ ] Memoize for nested-loop joins (PG 14)
- [ ] Self-join elimination (PG 18)
- [ ] Multi-column statistics (PG 10)
- [ ] Extended statistics on expressions (PG 14)

### Sequences (RUST)
- [ ] CREATE/ALTER/DROP SEQUENCE
- [ ] nextval(), currval(), setval(), lastval()
- [ ] SERIAL / BIGSERIAL / SMALLSERIAL
- [ ] GENERATED ALWAYS AS IDENTITY / BY DEFAULT AS IDENTITY (PG 10)

### TOAST (RUST)
- [ ] Automatic compression of large values
- [ ] Out-of-line storage for values > 2KB
- [ ] LZ4 compression option (PG 14)
- [ ] Configurable per-column compression

### Monitoring (BEAM/RUST)
- [ ] pg_stat_activity — current sessions and queries
- [ ] pg_stat_user_tables — table-level stats
- [ ] pg_stat_user_indexes — index-level stats
- [ ] pg_stat_io (PG 16)
- [ ] pg_stat_progress_* views
- [ ] pg_locks — lock information
- [ ] pg_stat_statements equivalent

### Replication (BEAM)
- [ ] WAL-based replication via BEAM distribution
- [ ] Embedded replicas (libSQL-inspired — local read replicas in the application)
- [ ] Synchronous replication option
- [ ] Logical replication — PUBLICATION / SUBSCRIPTION (PG 10)
- [ ] Logical replication row/column filtering (PG 15)
- [ ] Logical replication from standby (PG 16)
- [ ] Replication slots (PG 9.4)
- [ ] Cascading replication (PG 9.2)

### Backup & Recovery (BEAM/RUST)
- [ ] Point-in-Time Recovery — PITR (PG 8.0)
- [ ] pg_basebackup equivalent
- [ ] Incremental backup (PG 17)
- [ ] WAL archiving
- [ ] pg_dump / pg_restore protocol compatibility

---

## T3 — Advanced (Power User Tier)

### Stored Procedures & Functions (RUST)
- [ ] CREATE FUNCTION (SQL language)
- [ ] SQL-standard function bodies — BEGIN ATOMIC ... END (PG 14)
- [ ] CREATE PROCEDURE with transaction control (PG 11)
- [ ] PL/pgSQL language (PG 6.3)
- [ ] VARIADIC functions
- [ ] Default parameter values
- [ ] Named parameters
- [ ] RETURNS TABLE / RETURNS SETOF
- [ ] Polymorphic types (anyelement, anycompatible, etc.)

### Triggers (RUST)
- [ ] BEFORE / AFTER triggers on INSERT/UPDATE/DELETE
- [ ] INSTEAD OF triggers on views
- [ ] Per-column triggers, WHEN clause (PG 9.0)
- [ ] Transition tables in AFTER triggers (PG 10)
- [ ] Event triggers on DDL (PG 9.3)

### Generated Columns (RUST)
- [ ] Stored generated columns (PG 12)
- [ ] Virtual generated columns (PG 18)

### Domain Types (RUST)
- [ ] CREATE DOMAIN with CHECK constraints
- [ ] Composite types (row types)
- [ ] Custom base types (CREATE TYPE)

### Additional Types (RUST)
- [ ] XML type (PG 8.3)
- [ ] XMLTABLE (PG 10)
- [ ] inet / cidr / macaddr / macaddr8 network types
- [ ] point / line / lseg / box / path / polygon / circle geometric types
- [ ] BIT / BIT VARYING (PG 7.1)
- [ ] money type
- [ ] tsvector / tsquery
- [ ] Interval infinity (PG 17)

### JIT Compilation (RUST)
- [ ] LLVM-based or Cranelift-based JIT for hot query paths
- [ ] JIT for expression evaluation
- [ ] JIT for tuple deforming

### Advanced Query Features (RUST)
- [ ] TABLESAMPLE — SYSTEM and BERNOULLI methods (PG 9.5)
- [ ] RETURNING OLD/NEW (PG 18)
- [ ] WITH ORDINALITY (PG 9.4)
- [ ] LATERAL subqueries
- [ ] IS NOT DISTINCT FROM (PG 8.2)
- [ ] Non-decimal integer literals — 0x, 0o, 0b (PG 16)
- [ ] Regular expression functions — regexp_count, regexp_instr, regexp_like, regexp_substr (PG 15)
- [ ] Nondeterministic ICU collations (PG 12)
- [ ] Built-in collation provider (PG 17)
- [ ] PG_UNICODE_FAST collation (PG 18)

### Tablespaces (RUST)
- [ ] CREATE TABLESPACE
- [ ] Per-table / per-index tablespace assignment

### Unlogged Tables (RUST)
- [ ] CREATE UNLOGGED TABLE (PG 9.1)
- [ ] ALTER TABLE SET LOGGED / UNLOGGED (PG 9.5)

### Large Objects (RUST)
- [ ] Large object API (lo_create, lo_open, lo_read, lo_write, etc.)
- [ ] 4TB large object support (PG 9.3)

### Data Checksums (RUST)
- [ ] Page-level data checksums (PG 9.3)
- [ ] Checksums enabled by default (PG 18)

---

## T4 — Ecosystem (Extension & Integration Tier)

### Extension Framework (RUST)
- [ ] CREATE EXTENSION / DROP EXTENSION (PG 9.1)
- [ ] Extension versioning and upgrade paths
- [ ] Trusted extensions (PG 13)
- [ ] WASM-based extension runtime (inspired by libSQL) — safer than C shared libraries
- [ ] extension_control_path (PG 18)

### Key Extensions to Support (RUST)
- [ ] pgvector — vector similarity search
- [ ] PostGIS — geographic data
- [ ] pg_stat_statements — query statistics
- [ ] hstore — key-value pairs (PG 8.2)
- [ ] citext — case-insensitive text (PG 8.4)
- [ ] pg_trgm — trigram similarity
- [ ] uuid-ossp — UUID generation
- [ ] pgcrypto — cryptographic functions
- [ ] pg_cron — job scheduling

### Foreign Data Wrappers (RUST)
- [ ] CREATE FOREIGN TABLE (PG 9.1)
- [ ] Writeable foreign tables (PG 9.3)
- [ ] postgres_fdw (PG 9.3)
- [ ] IMPORT FOREIGN SCHEMA (PG 9.5)
- [ ] FDW pushdown — joins, sorts, DML (PG 9.6+)

### Procedural Languages (RUST)
- [ ] PL/pgSQL (built-in)
- [ ] PL/Python
- [ ] PL/Perl
- [ ] PL/V8 (JavaScript)
- [ ] WASM-based UDFs (libSQL-inspired approach)

### Advanced Replication (BEAM)
- [ ] Two-Phase Commit — PREPARE TRANSACTION (PG 8.1)
- [ ] Two-phase commit for logical replication (PG 15)
- [ ] Distributed query execution (sharding)
- [ ] Geo-partitioning (pin data to regions)
- [ ] Logical replication failover (PG 17)

### Distributed Features (BEAM)
- [ ] Automatic sharding (hash/range based)
- [ ] Distributed transactions (2PC via BEAM coordination)
- [ ] Cross-shard query execution
- [ ] Node discovery and cluster management
- [ ] Hot code upgrades (BEAM native)

### Asynchronous I/O (RUST)
- [ ] io_uring integration for Linux
- [ ] AIO subsystem (inspired by PG 18)

### NUMA Support (RUST)
- [ ] NUMA-aware memory allocation (PG 18)
- [ ] Per-NUMA-node buffer management

---

## Features We Do DIFFERENTLY (Our Advantages)

These are PostgreSQL features we implement but with fundamentally better architecture:

| PostgreSQL Approach | Our Approach | Benefit |
|---|---|---|
| Process-per-connection (fork) | BEAM lightweight processes | 1000x less memory per connection |
| In-heap MVCC + VACUUM | Undo-log MVCC | Zero bloat, no vacuum needed |
| shared_buffers + OS page cache | Direct I/O (O_DIRECT) | No double buffering, 2x effective RAM |
| 32-bit XIDs + freeze vacuum | 64-bit timestamps | No wraparound, ever |
| Full-page WAL images | Double-write buffer or row-level WAL | 2-3x less WAL volume |
| 65 MVCC catalog tables | Lightweight metadata + virtual pg_catalog | Near-instant DDL at any schema size |
| Centralized lock table | Latch-free structures + in-row locks | Linear scaling under concurrency |
| Bolt-on streaming replication | BEAM native distribution | Built-in clustering, automatic failover |
| PgBouncer required | Built-in connection pooling (BEAM) | No external pooler needed |
| C shared library extensions | WASM sandboxed extensions | Safer, portable, no server restart |
| Full restart for config changes | BEAM hot code reloading | Zero-downtime configuration |

---

## Version Mapping

Our implementation roughly maps to PostgreSQL feature coverage:

| Our Milestone | ≈ PostgreSQL Version | Key Feature Parity |
|---|---|---|
| T0 complete | ~PG 6.x | Basic SQL, CRUD, wire protocol |
| T1 complete | ~PG 9.4 | CTEs, window functions, JSONB, UPSERT, indexes |
| T2 complete | ~PG 15 | Partitioning, RLS, parallel query, full-text search, replication |
| T3 complete | ~PG 17 | Stored procedures, triggers, JIT, advanced types |
| T4 complete | ~PG 18+ | Extensions, FDW, distributed features, full ecosystem |

---

*This document is the source of truth for feature scope. Update as features are implemented.*
