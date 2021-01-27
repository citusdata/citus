# Introduction

Citus Columnar offers a per-table option for columnar storage to
reduce IO requirements though compression and projection pushdown.

# Design Trade-Offs

Existing PostgreSQL row tables work well for OLTP:

* Support `UPDATE`/`DELETE` efficiently
* Efficient single-tuple lookups

The Citus Columnar tables work best for analytic or DW workloads:

* Compression
* Doesn't read unnecessary columns
* Efficient `VACUUM`

# Limitations

* Append-only (no ``UPDATE``/``DELETE`` support)
* No space reclamation (e.g. rolled-back transactions may still
  consume disk space)
* No index support, index scans, or bitmap index scans
* No tidscans
* No sample scans
* No TOAST support (large values supported inline)
* No support for [``ON
  CONFLICT``](https://www.postgresql.org/docs/12/sql-insert.html#SQL-ON-CONFLICT)
  statements (except ``DO NOTHING`` actions with no target specified).
* No support for tuple locks (``SELECT ... FOR SHARE``, ``SELECT
  ... FOR UPDATE``)
* No support for serializable isolation level
* Support for PostgreSQL server versions 12+ only
* No support for foreign keys, unique constraints, or exclusion
  constraints
* No support for logical decoding
* No support for intra-node parallel scans
* No support for ``AFTER ... FOR EACH ROW`` triggers
* No `UNLOGGED` columnar tables
* No `TEMPORARY` columnar tables

Future iterations will incrementally lift the limitations listed above.

# User Experience

Create a Columnar table by specifying ``USING columnar`` when creating
the table.

```sql
CREATE TABLE my_columnar_table
(
    id INT,
    i1 INT,
    i2 INT8,
    n NUMERIC,
    t TEXT
) USING columnar;
```

Insert data into the table and read from it like normal (subject to
the limitations listed above).

To see internal statistics about the table, use ``VACUUM
VERBOSE``. Note that ``VACUUM`` (without ``FULL``) is much faster on a
columnar table, because it scans only the metadata, and not the actual
data.

## Options

Set options using:

```sql
alter_columnar_table_set(
    relid REGCLASS,
    chunk_row_count INT4 DEFAULT NULL,
    stripe_row_count INT4 DEFAULT NULL,
    compression NAME DEFAULT NULL,
    compression_level INT4)
```

For example:

```sql
SELECT alter_columnar_table_set(
    'my_columnar_table',
    compression => 'none',
    stripe_row_count => 10000);
```

The following options are available:

* **compression**: `[none|pglz|zstd|lz4|lz4hc]` - set the compression type
  for _newly-inserted_ data. Existing data will not be
  recompressed/decompressed. The default value is `zstd` (if support
  has been compiled in).
* **compression_level**: ``<integer>`` - Sets compression level. Valid
  settings are from 1 through 19. If the compression method does not
  support the level chosen, the closest level will be selected
  instead.
* **stripe_row_count**: ``<integer>`` - the maximum number of rows per
  stripe for _newly-inserted_ data. Existing stripes of data will not
  be changed and may have more rows than this maximum value. The
  default value is `150000`.
* **chunk_row_count**: ``<integer>`` - the maximum number of rows per
  chunk for _newly-inserted_ data. Existing chunks of data will not be
  changed and may have more rows than this maximum value. The default
  value is `10000`.

View options for all tables with:

```sql
SELECT * FROM columnar.options;
```

You can also adjust options with a `SET` command of one of the
following GUCs:

* `columnar.compression`
* `columnar.compression_level`
* `columnar.stripe_row_count`
* `columnar.chunk_row_count`

GUCs only affect newly-created *tables*, not any newly-created
*stripes* on an existing table.

## Partitioning

Columnar tables can be used as partitions; and a partitioned table may
be made up of any combination of row and columnar partitions.

```sql
CREATE TABLE parent(ts timestamptz, i int, n numeric, s text)
  PARTITION BY RANGE (ts);

-- columnar partition
CREATE TABLE p0 PARTITION OF parent
  FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')
  USING COLUMNAR;
-- columnar partition
CREATE TABLE p1 PARTITION OF parent
  FOR VALUES FROM ('2020-02-01') TO ('2020-03-01')
  USING COLUMNAR;
-- row partition
CREATE TABLE p2 PARTITION OF parent
  FOR VALUES FROM ('2020-03-01') TO ('2020-04-01');

INSERT INTO parent VALUES ('2020-01-15', 10, 100, 'one thousand'); -- columnar
INSERT INTO parent VALUES ('2020-02-15', 20, 200, 'two thousand'); -- columnar
INSERT INTO parent VALUES ('2020-03-15', 30, 300, 'three thousand'); -- row
```

When performing operations on a partitioned table with a mix of row
and columnar partitions, take note of the following behaviors for
operations that are supported on row tables but not columnar
(e.g. ``UPDATE``, ``DELETE``, tuple locks, etc.):

* If the operation is targeted at a specific row partition
  (e.g. ``UPDATE p2 SET i = i + 1``), it will succeed; if targeted at
  a specified columnar partition (e.g. ``UPDATE p1 SET i = i + 1``),
  it will fail.
* If the operation is targeted at the partitioned table and has a
  ``WHERE`` clause that excludes all columnar partitions
  (e.g. ``UPDATE parent SET i = i + 1 WHERE ts = '2020-03-15'``), it
  will succeed.
* If the operation is targeted at the partitioned table, but does not
  exclude all columnar partitions, it will fail; even if the actual
  data to be updated only affects row tables (e.g. ``UPDATE parent SET
  i = i + 1 WHERE n = 300``).

Because columnar tables do not support indexes, it's impossible to
create indexes on the partitioned table if some partitions are
columnar. Instead, you must create indexes on the individual row
partitions. Similarly for constraints that require indexes, e.g.:

```sql
CREATE INDEX p2_ts_idx ON p2 (ts);
CREATE UNIQUE INDEX p2_i_unique ON p2 (i);
ALTER TABLE p2 ADD UNIQUE (n);
```

## Converting Between Row and Columnar

Note: ensure that you understand any advanced features that may be
used with the table before converting it (e.g. row-level security,
storage options, constraints, inheritance, etc.), and ensure that they
are reproduced in the new table or partition appropriately. ``LIKE``,
used below, is a shorthand that works only in simple cases.

```sql
CREATE TABLE my_table(i INT8 DEFAULT '7');
INSERT INTO my_table VALUES(1);
-- convert to columnar
SELECT alter_table_set_access_method('my_table', 'columnar');
-- back to row
SELECT alter_table_set_access_method('my_table', 'heap');
```

# Performance Microbenchmark

*Important*: This microbenchmark is not intended to represent any real
 workload. Compression ratios, and therefore performance, will depend
 heavily on the specific workload. This is only for the purpose of
 illustrating a "columnar friendly" contrived workload that showcases
 the benefits of columnar.

## Schema

```sql
CREATE TABLE perf_row(
    id INT8,
    ts TIMESTAMPTZ,
    customer_id INT8,
    vendor_id INT8,
    name TEXT,
    description TEXT,
    value NUMERIC,
    quantity INT4
);

CREATE TABLE perf_columnar(LIKE perf_row) USING COLUMNAR;
```

## Data

```sql
CREATE OR REPLACE FUNCTION random_words(n INT4) RETURNS TEXT LANGUAGE plpython2u AS $$
import random
t = ''
words = ['zero','one','two','three','four','five','six','seven','eight','nine','ten']
for i in xrange(0,n):
  if (i != 0):
    t += ' '
  r = random.randint(0,len(words)-1)
  t += words[r]
return t
$$;
```

```sql
INSERT INTO perf_row
   SELECT
    g, -- id
    '2020-01-01'::timestamptz + ('1 minute'::interval * g), -- ts
    (random() * 1000000)::INT4, -- customer_id
    (random() * 100)::INT4, -- vendor_id
    random_words(7), -- name
    random_words(100), -- description
    (random() * 100000)::INT4/100.0, -- value
    (random() * 100)::INT4 -- quantity
   FROM generate_series(1,75000000) g;

INSERT INTO perf_columnar SELECT * FROM perf_row;
```

## Compression Ratio

```
=> SELECT pg_total_relation_size('perf_row')::numeric/pg_total_relation_size('perf_columnar') AS compression_ratio;
 compression_ratio
--------------------
 5.4080768380134124
(1 row)
```

The overall compression ratio of columnar table, versus the same data
stored with row storage, is **5.4X**.

```
=> VACUUM VERBOSE perf_row;
INFO:  vacuuming "public.perf_row"
INFO:  "perf_row": found 0 removable, 10 nonremovable row versions in 1 out of 5769231 pages
DETAIL:  0 dead row versions cannot be removed yet, oldest xmin: 3110
There were 0 unused item identifiers.
Skipped 0 pages due to buffer pins, 5769230 frozen pages.
0 pages are entirely empty.
CPU: user: 0.10 s, system: 0.05 s, elapsed: 0.26 s.
INFO:  vacuuming "pg_toast.pg_toast_17133"
INFO:  index "pg_toast_17133_index" now contains 0 row versions in 1 pages
DETAIL:  0 index row versions were removed.
0 index pages have been deleted, 0 are currently reusable.
CPU: user: 0.00 s, system: 0.00 s, elapsed: 0.00 s.
INFO:  "pg_toast_17133": found 0 removable, 0 nonremovable row versions in 0 out of 0 pages
DETAIL:  0 dead row versions cannot be removed yet, oldest xmin: 3110
There were 0 unused item identifiers.
Skipped 0 pages due to buffer pins, 0 frozen pages.
0 pages are entirely empty.
CPU: user: 0.00 s, system: 0.00 s, elapsed: 0.00 s.

=> VACUUM VERBOSE perf_columnar;
INFO:  statistics for "perf_columnar":
storage id: 10000000020
total file size: 8741486592, total data size: 8714771176
compression rate: 4.96x
total row count: 75000000, stripe count: 501, average rows per stripe: 149700
chunk count: 60000, containing data for dropped columns: 0, zstd compressed: 60000
```

``VACUUM VERBOSE`` reports a smaller compression ratio, because it
only averages the compression ratio of the individual chunks, and does
not account for the metadata savings of the columnar format.

## System

* 16GB physical memory
* 128MB PG shared buffers

Note: because this was run on a system with enough physical memory to
hold a substantial fraction of the table, the IO benefits of columnar
won't be entirely realized by the query runtime unless the data size
is substantially increased.

## Query

```sql
-- OFFSET 1000 so that no rows are returned, and we collect only timings
SELECT vendor_id, SUM(quantity) FROM perf_row GROUP BY vendor_id OFFSET 1000;
SELECT vendor_id, SUM(quantity) FROM perf_columnar GROUP BY vendor_id OFFSET 1000;
```

Timing (median of three runs):
 * row: 201700ms
 * columnar: 14202ms
 * speedup: **14X**
