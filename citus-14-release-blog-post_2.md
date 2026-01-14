# Citus 14: PostgreSQL 18 support (and what it took to get there)

Welcome to the release notes for **Citus 14**. The headline for 14 is **PostgreSQL 18 support**—so you can upgrade to Postgres 18 (released **2025-09-25**) while keeping Citus distributed SQL working end-to-end across coordinator + workers.

Like prior major releases, once Citus is compatible with the new Postgres major, many upstream improvements “just work”—but PG18 also introduces **new SQL surface area** and **behavior changes** that require Citus-specific work (parsing/deparsing, DDL propagation, and regression stability). (See prior major release notes for context.)

**Citus 14** is primarily about **PostgreSQL 18 compatibility**—so teams can adopt Postgres 18 while keeping the distributed SQL, sharding, and operational model that Citus clusters depend on.

PostgreSQL 18’s **release date is September 25, 2025**, and it’s a substantial release: asynchronous I/O (AIO), skip-scan for multicolumn B-tree indexes, `uuidv7()`, virtual generated columns by default, OAuth authentication, `RETURNING OLD/NEW`, and temporal constraints.

Because Citus is implemented as a Postgres extension, most upstream improvements “just work” once compatibility lands—but major releases also bring **SQL surface-area changes** and **planner/behavior shifts** that require Citus-specific work to keep distributed semantics correct across **coordinator + workers**, and to keep DDL/DML/utility commands working end-to-end.

---

This page dives deep into many of the changes in Citus 14, including:

* [PostgreSQL 18 support intro](#postgresql-18-support-intro): summary on how 14 adds PostgreSQL 18 support (and what you get “for free” from upstream: AIO, skip-scan, `uuidv7()`, OAuth)
* [PG18: Faster scans and maintenance via AIO](#pg18-faster-scans-and-maintenance-via-aio): benefit from Postgres 18’s new async I/O subsystem for scans and VACUUM-heavy workloads
* [PG18: Better index usage with skip-scan](#pg18-better-index-usage-with-skip-scan): leverage skip-scan on multicolumn B-tree indexes for common multi-tenant patterns
* [PG18: `uuidv7()` for time-ordered UUIDs](#pg18-uuidv7-for-time-ordered-uuids): reduce index churn with time-ordered UUID generation
* [PG18: OAuth authentication support](#pg18-oauth-authentication-support): integrate Postgres auth with modern SSO/OAuth flows
* [PG18: JSON_TABLE() COLUMNS](#pg18-json_table-columns): `JSON_TABLE()` COLUMNS clause support in distributed queries (PG18 expansion)
* [PG18: Temporal constraints](#pg18-temporal-constraints): support `WITHOUT OVERLAPS` in `UNIQUE/PRIMARY KEY` and `PERIOD` in `FOREIGN KEY`
* [PG18: CREATE FOREIGN TABLE ... LIKE](#pg18-create-foreign-table--like): `LIKE` support when creating foreign tables
* [PG18: Generated columns](#pg18-generated-columns-virtual-by-default--logical-replication-knobs): virtual-by-default generated columns + distributed DDL/metadata propagation (and `publish_generated_columns`)
* [PG18: VACUUM/ANALYZE ONLY semantics](#pg18-vacuumanalyze-semantics-only-restores-old-behavior): use `ONLY` to keep maintenance scoped to the parent table in partitioned schemas
* [PG18: Constraints: NOT ENFORCED](#pg18-constraints-not-enforced-partitioned-table-additions): propagate `CHECK/FOREIGN KEY ... NOT ENFORCED` (and related partitioned-table additions)
* [PG18: DML: RETURNING OLD/NEW](#pg18-dml-returning-oldnew): preserve new `RETURNING old/new` semantics through distributed execution and result shaping
* [PG18: COPY and SQL/JSON expansions](#pg18-copy-and-sqljson-expansions): `COPY ... REJECT_LIMIT`, `COPY ... TO` from materialized views, and SQL/JSON coverage
* [PG18: Collations and text semantics](#pg18-collations-and-text-semantics-that-affect-distributed-correctness): nondeterministic collation support for `LIKE` and text-position search functions
* [PG18: Utility/ops plumbing and observability](#pg18-utilityops-plumbing-and-observability): `file_copy_method`, new `EXPLAIN (WAL)` fields, and `PG_MODULE_MAGIC_EXT` support

---

## PostgreSQL 18 support intro

Postgres 18 includes major improvements like **AIO**, **skip-scan**, and `uuidv7()`. Once Citus is compatible with PG18, these upstream performance and UX improvements generally benefit Citus clusters automatically—especially for shard-heavy scans and maintenance.

Citus 14’s PG18-specific work focuses on the remaining pieces: new syntax and behavior that must be understood and preserved across distributed planning + coordinator/worker execution. The tracking issue is maintained publicly.

## PostgreSQL 18 highlights that matter in Citus clusters

### PG18: Faster scans and maintenance via AIO

Postgres 18 adds an **asynchronous I/O subsystem** that can improve sequential scans, bitmap heap scans, and vacuuming—workloads that show up constantly in shard-heavy distributed clusters.

### PG18: Better index usage with skip-scan

Postgres 18 expands when **multicolumn B-tree indexes** can be used via **skip scan**, helping common multi-tenant schemas where predicates don’t always constrain the leading index column.

### PG18: `uuidv7()` for time-ordered UUIDs

Time-ordered UUIDs can reduce index churn and improve locality; Postgres 18 adds `uuidv7()`.

### PG18: OAuth authentication support

Postgres 18 adds **OAuth authentication**, making it easier to plug database auth into modern SSO flows—often a practical requirement in multi-node deployments.

---

## What Citus 14 adds for PostgreSQL 18 compatibility

Citus tracks the PG18 work as a concrete checklist of **syntax, behavior, deparsing/propagation, and regression coverage** items required for distributed correctness.

### PG18: JSON_TABLE() COLUMNS

PG18 expands SQL/JSON `JSON_TABLE()` with a richer `COLUMNS` clause, making it easy to extract multiple fields from JSON documents in a single, typed table expression. Citus 14 ensures the syntax can be parsed/deparsed and executed consistently in distributed queries.

Example: extract multiple fields (`name`, `age`) from a nested JSON object:

```sql
CREATE TABLE pg18_json_test (id serial PRIMARY KEY, data JSON);

INSERT INTO pg18_json_test (data) VALUES
  ('{ "user": {"name": "Alice",   "age": 30, "city": "San Diego"} }'),
  ('{ "user": {"name": "Bob",     "age": 25, "city": "Los Angeles"} }'),
  ('{ "user": {"name": "Charlie", "age": 35, "city": "Los Angeles"} }'),
  ('{ "user": {"name": "Diana",   "age": 28, "city": "Seattle"} }'),
  ('{ "user": {"name": "Evan",    "age": 40, "city": "Portland"} }'),
  ('{ "user": {"name": "Ethan",   "age": 32, "city": "Seattle"} }'),
  ('{ "user": {"name": "Fiona",   "age": 27, "city": "Seattle"} }'),
  ('{ "user": {"name": "George",  "age": 29, "city": "San Francisco"} }'),
  ('{ "user": {"name": "Hannah",  "age": 33, "city": "Seattle"} }'),
  ('{ "user": {"name": "Ian",     "age": 26, "city": "Portland"} }'),
  ('{ "user": {"name": "Jane",    "age": 38, "city": "San Francisco"} }');

SELECT jt.name, jt.age
FROM pg18_json_test,
     JSON_TABLE(
       data,
       '$.user'
       COLUMNS (
         age  INT  PATH '$.age',
         name TEXT PATH '$.name'
       )
     ) AS jt
WHERE jt.age BETWEEN 25 AND 35
ORDER BY jt.age, jt.name;
```

### PG18: Temporal constraints

Postgres 18 adds temporal constraint syntax that Citus must propagate and preserve correctly:

* `WITHOUT OVERLAPS` for `PRIMARY KEY` / `UNIQUE`
* `PERIOD` for `FOREIGN KEY`

Example: `WITHOUT OVERLAPS` in a composite primary key (ranges must not overlap for the same key):

```sql
CREATE TABLE temporal_rng (
  -- Use an int4range so we don't depend on btree_gist for plain ints
  id int4range,
  valid_at daterange,
  CONSTRAINT temporal_rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);

SELECT create_distributed_table('temporal_rng', 'id');

-- ok: non-overlapping ranges for the same id
INSERT INTO temporal_rng (id, valid_at)
VALUES ('[1,2)', daterange('2018-01-02', '2018-02-03'));

INSERT INTO temporal_rng (id, valid_at)
VALUES ('[1,2)', daterange('2018-03-03', '2018-04-04'));

-- should fail: overlaps the existing [2018-01-02,2018-02-03)
INSERT INTO temporal_rng (id, valid_at)
VALUES ('[1,2)', daterange('2018-01-01', '2018-01-05'));
```

Example: temporal foreign key using `PERIOD` (child rows must match a parent row over the referenced time period):

```sql
-- Parent table (reference table here to allow FK from a distributed table)
CREATE EXTENSION btree_gist;

CREATE TABLE temporal_test (
  id integer,
  valid_at daterange,
  CONSTRAINT temporal_test_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);

SELECT create_reference_table('temporal_test');

-- Child table (distributed) with temporal FK
CREATE TABLE temporal_fk_rng2rng (
  id integer,
  valid_at daterange,
  parent_id integer,
  CONSTRAINT temporal_fk_rng2rng_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);

SELECT create_distributed_table('temporal_fk_rng2rng', 'id');

ALTER TABLE temporal_fk_rng2rng
  ADD CONSTRAINT temporal_fk_rng2rng_fk
  FOREIGN KEY (parent_id, PERIOD valid_at)
  REFERENCES temporal_test (id, PERIOD valid_at);

-- sample data
INSERT INTO temporal_test VALUES
  (1, '[2000-01-01,2001-01-01)'),
  (1, '[2001-01-01,2002-01-01)');

-- ok: fully covered by parent periods
INSERT INTO temporal_fk_rng2rng VALUES
  (1, '[2000-01-01,2001-01-01)', 1);

-- ok: spans two parent periods
INSERT INTO temporal_fk_rng2rng VALUES
  (2, '[2000-01-01,2002-01-01)', 1);

-- should fail: missing parent_id=3 for that period
INSERT INTO temporal_fk_rng2rng VALUES
  (3, '[2000-01-01,2001-01-01)', 3);
```


### PG18: CREATE FOREIGN TABLE ... LIKE

Postgres 18 supports `CREATE FOREIGN TABLE ... LIKE`, letting you define a foreign table by copying the column layout (and optionally defaults/constraints/indexes) from an existing table. Citus 14 includes coverage so FDW workflows remain compatible in distributed environments.

Example (from the PG18 regression coverage): create a local table with a few modern features, then create foreign tables using `LIKE ... EXCLUDING ALL` vs `LIKE ... INCLUDING ALL`.

```sql
SET citus.use_citus_managed_tables TO ON;

CREATE EXTENSION postgres_fdw;

CREATE SERVER foreign_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'localhost', port :'master_port', dbname 'regression');

CREATE USER MAPPING FOR CURRENT_USER
  SERVER foreign_server
  OPTIONS (user 'postgres');

CREATE TABLE ctl_table(
  a int PRIMARY KEY,
  b varchar COMPRESSION pglz,
  c int GENERATED ALWAYS AS (a * 2) STORED,
  d bigint GENERATED ALWAYS AS IDENTITY,
  e int DEFAULT 1
);

CREATE INDEX ctl_table_ab_key ON ctl_table(a, b);
```

`EXCLUDING ALL` example:

```sql
CREATE FOREIGN TABLE ctl_ft1 (LIKE ctl_table EXCLUDING ALL)
  SERVER foreign_server
  OPTIONS (schema_name 'pg18_nn', table_name 'ctl_table');
```

`INCLUDING ALL` example:

```sql
CREATE FOREIGN TABLE ctl_ft2 (LIKE ctl_table INCLUDING ALL)
  SERVER foreign_server
  OPTIONS (schema_name 'pg18_nn', table_name 'ctl_table');

SELECT * FROM ctl_ft2 ORDER BY a;
```
---

### PG18: Generated columns: virtual-by-default + logical replication knobs

Postgres 18 changes the default behavior for generated columns: **generated columns are virtual by default** (computed on read), which can reduce write amplification for workloads where the generated value is rarely queried. Postgres 18 also improves **logical replication support** for generated columns via the `publish_generated_columns` publication option.

Citus 14 ensures these PG18 changes work cleanly in distributed clusters by updating distributed DDL propagation and metadata handling so generated-column definitions remain consistent across coordinator and workers.

**Example: virtual generated column on a distributed table**

```sql
CREATE TABLE events (
  id bigint,
  payload jsonb,
  payload_hash text GENERATED ALWAYS AS (md5(payload::text)) VIRTUAL
);

SELECT create_distributed_table('events', 'id');

INSERT INTO events VALUES
  (1, '{"type":"signup","user":"alice"}'),
  (2, '{"type":"purchase","user":"bob"}');

-- Computed on read
SELECT id, payload_hash
FROM events
ORDER BY id;
```

**Example: explicit `STORED` generated column**

If you want the value materialized at write time (e.g., heavily filtered/indexed), use `STORED` explicitly.

```sql
CREATE TABLE events_stored (
  id bigint,
  payload jsonb,
  payload_hash text GENERATED ALWAYS AS (md5(payload::text)) STORED
);

SELECT create_distributed_table('events_stored', 'id');
```

**Example: publish generated columns in logical replication**

```sql
CREATE PUBLICATION pub_events
  FOR TABLE events
  WITH (publish_generated_columns = true);
```

---

### PG18: VACUUM/ANALYZE semantics: `ONLY` restores old behavior

Postgres 18 introduces `ONLY` for `VACUUM` and `ANALYZE` so you can explicitly **target only the parent** of a partitioned/inheritance tree without automatically processing children. This matters in distributed environments because a “parent object” can represent many underlying shard/partition relations, and predictable scoping avoids surprising work during maintenance.

Citus 14 adapts distributed utility-command behavior so `ONLY` works as intended and maintenance remains predictable across coordinator + workers.

**Example: partitioned table + parent-only maintenance**

```sql
CREATE TABLE metrics (
  tenant_id bigint,
  ts timestamptz,
  value double precision
) PARTITION BY RANGE (ts);

CREATE TABLE metrics_2025_01 PARTITION OF metrics
  FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE metrics_2025_02 PARTITION OF metrics
  FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

SELECT create_distributed_table('metrics', 'tenant_id');

-- Parent-only: do not recurse into partitions/children
VACUUM (ANALYZE) ONLY metrics;
ANALYZE ONLY metrics;
```

**Example: (contrast) without `ONLY`**

If you omit `ONLY`, Postgres may process children as well (depending on the command and table structure), which can be undesirable when you intended a light-touch operation.

```sql
VACUUM (ANALYZE) metrics;
ANALYZE metrics;
```
---

### PG18: Constraints: `NOT ENFORCED` (partitioned-table additions)

Postgres 18 expands constraint syntax and catalog semantics in ways Citus must **parse/deparse** and **propagate** correctly across coordinator + workers:

* `CHECK` / `FOREIGN KEY` constraints can be marked `NOT ENFORCED` (tracked via `pg_constraint.conenforced`)
* Partitioned-table additions exercised in Citus PG18 coverage:

  * `NOT VALID` foreign keys on partitioned tables
  * `ALTER TABLE ... DROP CONSTRAINT ONLY` on partitioned tables

**Example: `NOT ENFORCED` constraints + catalog visibility**

```sql
CREATE TABLE customers (id bigint PRIMARY KEY);
SELECT create_reference_table('customers');

CREATE TABLE orders (
  id bigint PRIMARY KEY,
  customer_id bigint NOT NULL,
  amount numeric
);
SELECT create_distributed_table('orders', 'id');

ALTER TABLE orders
  ADD CONSTRAINT orders_amount_positive CHECK (amount > 0) NOT ENFORCED;

ALTER TABLE orders
  ADD CONSTRAINT orders_customer_fk
  FOREIGN KEY (customer_id) REFERENCES customers(id)
  NOT ENFORCED;

SELECT conname, conenforced
FROM pg_constraint
WHERE conrelid = 'orders'::regclass
ORDER BY conname;
```

**Example: partitioned-table `NOT VALID` FK + `DROP CONSTRAINT ONLY`**

```sql
CREATE TABLE parent (id bigint PRIMARY KEY);
SELECT create_reference_table('parent');

CREATE TABLE child (
  tenant_id bigint,
  id bigint,
  parent_id bigint,
  PRIMARY KEY (tenant_id, id)
) PARTITION BY RANGE (id);
CREATE TABLE child_p0 PARTITION OF child FOR VALUES FROM (0) TO (1000);

SELECT create_distributed_table('child', 'tenant_id');

ALTER TABLE child
  ADD CONSTRAINT child_parent_fk
  FOREIGN KEY (parent_id) REFERENCES parent(id)
  NOT VALID;

ALTER TABLE child DROP CONSTRAINT ONLY child_parent_fk;
```

---

### PG18: DML: `RETURNING OLD/NEW`

Postgres 18 lets `RETURNING` reference both the **previous** (`old`) and **new** (`new`) row values in `INSERT/UPDATE/DELETE/MERGE`. Citus 14 preserves these semantics in distributed execution and ensures results are returned correctly from coordinator + workers.

Example:

```sql
UPDATE t
SET v = v + 1
WHERE id = 42
RETURNING old.v AS old_v, new.v AS new_v;
```

Example with `DELETE` (capture deleted values):

```sql
DELETE FROM t
WHERE id = 42
RETURNING old.v AS deleted_v;
```

---

### PG18: Utility/ops plumbing and observability

Citus 14 adapts to PG18 interface/output changes that affect tooling and extension plumbing:

* New GUC `file_copy_method` for `CREATE DATABASE ... STRATEGY=FILE_COPY` (and related file-copy operations).
* `EXPLAIN (WAL)` adds a “WAL buffers full” field; Citus propagates it through distributed EXPLAIN output.
* New extension macro `PG_MODULE_MAGIC_EXT` so extensions can report name/version metadata.

Example: distributed EXPLAIN showing the new WAL field:

```sql
EXPLAIN (ANALYZE, WAL)
SELECT count(*) FROM dist_table;
```

Example: set the new GUC (session-level):

```sql
SET file_copy_method = 'copy';
```

---