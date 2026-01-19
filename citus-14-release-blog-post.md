# Citus 14.0: Bringing PostgreSQL 18 Power to Distributed Databases

**Release Date:** [TBD 2026]

Welcome to Citus 14.0! This release marks a significant milestone as we bring full PostgreSQL 18 support to the world of distributed databases. With PostgreSQL 18's groundbreaking features now available in a distributed environment, Citus users can leverage the latest innovations in query optimization, data processing, and SQL functionality across their entire cluster.

## Table of Contents

- [PostgreSQL 18 Support: A Game Changer](#postgresql-18-support)
- [Self-Join Elimination: Smarter Queries](#self-join-elimination)
- [MIN/MAX on Complex Types: Breaking Barriers](#min-max-complex-types)
- [NOT NULL as Real Constraints](#not-null-constraints)
- [JSON_TABLE: Distributed JSON Processing](#json-table)
- [Enhanced EXPLAIN Output](#explain-enhancements)
- [Looking Ahead](#looking-ahead)

---

## PostgreSQL 18 Support: A Game Changer {#postgresql-18-support}

PostgreSQL 18 delivers substantial improvements that benefit both new workloads and mission-critical systems. The headline features include:

- **Asynchronous I/O (AIO) subsystem** for improved performance
- **pg_upgrade retains optimizer statistics** for faster migrations
- **Skip scan lookups** for multicolumn B-tree indexes
- **uuidv7()** function for timestamp-ordered UUIDs
- **Virtual generated columns** (now the default)
- **Self-join elimination** for more efficient query plans
- **Temporal constraints** for PRIMARY KEY, UNIQUE, and FOREIGN KEY

As a PostgreSQL extension, Citus 14.0 inherits all these improvements. But we've gone further—we've ensured that PostgreSQL 18's most powerful features work seamlessly in a distributed environment across your entire cluster.

---

## Self-Join Elimination: Smarter Queries {#self-join-elimination}

One of the most impactful optimizer improvements in PostgreSQL 18 is **automatic self-join elimination**. When multiple joins reference the same table with equivalent conditions, PostgreSQL 18 can now recognize and eliminate redundant joins—reducing query complexity and improving performance.

With Citus 14.0, this optimization works transparently across distributed tables.

### Example: Before and After

Consider a query that joins the same distributed table multiple times:

```sql
-- Create distributed tables
CREATE TABLE customers (
    id bigserial PRIMARY KEY,
    name text,
    created_at timestamptz DEFAULT now()
);

CREATE TABLE orders (
    id bigserial PRIMARY KEY,
    customer_id bigint,
    created_at timestamptz DEFAULT now()
);

SELECT create_distributed_table('customers', 'id');
SELECT create_distributed_table('orders', 'customer_id');

-- Insert test data
INSERT INTO customers SELECT i, 'Customer ' || i, now() 
FROM generate_series(0, 100) i;

INSERT INTO orders SELECT i, i, now() 
FROM generate_series(0, 100) i;

-- Query with multiple self-joins
SELECT count(1) 
FROM customers c
INNER JOIN orders u1 USING (id)
INNER JOIN orders u2 USING (id)
INNER JOIN orders u3 USING (id)
INNER JOIN orders u4 USING (id)
INNER JOIN orders u5 USING (id)
INNER JOIN orders u6 USING (id);
```

**In Citus 13 (PostgreSQL 17):** All six joins would be executed, even though they're redundant.

**In Citus 14 (PostgreSQL 18):** The query plan shows only **one join** because PostgreSQL 18's self-join elimination recognizes that u1 through u6 all reference the same table with the same join condition:

```sql
EXPLAIN (costs off)
SELECT count(1) FROM customers c
INNER JOIN orders u1 USING (id)
INNER JOIN orders u2 USING (id)
-- ... (u3, u4, u5)
INNER JOIN orders u6 USING (id);

                    QUERY PLAN
--------------------------------------------------------
 Aggregate
   ->  Custom Scan (Citus Adaptive)
         Task Count: 4
         Tasks Shown: One of 4
         ->  Task
               ->  Aggregate
                     ->  Hash Join
                           Hash Cond: (c.id = u6.id)
                           ->  Seq Scan on customers
                           ->  Hash
                                 ->  Seq Scan on orders u6
```

Notice: **only one Hash Join** is performed! PostgreSQL 18 eliminated the redundant self-joins, and Citus pushes this optimized plan to all workers.

### Performance Impact

For queries with multiple redundant joins, this can result in:
- **Faster query execution** (fewer join operations)
- **Reduced memory usage** (fewer hash tables)
- **Better scalability** across distributed nodes

This is especially valuable for:
- Reporting queries with complex join patterns
- ORM-generated queries that may include redundant joins
- Views that compose multiple table references

---

## MIN/MAX on Complex Types: Breaking Barriers {#min-max-complex-types}

PostgreSQL 18 made a fundamental change that unlocks new possibilities for Citus: **MIN() and MAX() aggregates now work on composite types (records) and arrays**.

### The Problem Before PostgreSQL 18

In previous versions, trying to compute MIN/MAX on arrays or composite types would fail:

```sql
-- This would fail in PostgreSQL < 18
CREATE TABLE products (id int, ratings int[]);
SELECT MIN(ratings) FROM products; -- ERROR!
```

### The Solution in Citus 14.0

PostgreSQL 18 introduces a new aggregate matching strategy called **AGG_MATCH_RECORD** that resolves aggregates for composite types. Citus 14.0 extends this to distributed queries, enabling MIN/MAX operations on complex types across your entire cluster.

#### Example: Array Aggregation

```sql
-- Create a distributed table with array columns
CREATE TABLE array_data (id int, values int[]);
SELECT create_distributed_table('array_data', 'id');

INSERT INTO array_data VALUES
  (1, ARRAY[45, 52, 38]),
  (2, ARRAY[67, 71, 58]),
  (3, ARRAY[23, 28, 15]);

-- Now this works across all shards!
SELECT 
  MIN(values) AS array_min,
  MAX(values) AS array_max
FROM array_data;

 array_min  | array_max
------------+------------
 {23,28,15} | {67,71,58}
```

#### Example: Composite Type Aggregation

```sql
-- Define a composite type
CREATE TYPE product_rating AS (
    average_score DECIMAL(3,2),
    review_count INTEGER
);

-- Create distributed table with composite type column
CREATE TABLE product_ratings (
    id int,
    rating product_rating
);
SELECT create_distributed_table('product_ratings', 'id');

INSERT INTO product_ratings VALUES
    (1, ROW(4.5, 120)::product_rating),
    (2, ROW(4.2,  89)::product_rating),
    (3, ROW(4.8, 156)::product_rating);

-- Get MIN/MAX ratings across all shards
SELECT
    MIN(rating) AS lowest_rating,
    MAX(rating) AS highest_rating
FROM product_ratings;

 lowest_rating | highest_rating
---------------+----------------
 (4.20,89)     | (4.80,156)
```

### How It Works

PostgreSQL 18 introduces a new OID resolution strategy for aggregates:

1. **AGG_MATCH_EXACT**: Direct type matching (traditional behavior)
2. **AGG_MATCH_ARRAY_POLY**: Handles ANYARRAY → specific array types
3. **AGG_MATCH_GENERAL_POLY**: Handles polymorphic types (e.g., enums)
4. **AGG_MATCH_RECORD**: ✨ **NEW** - Handles composite types

Citus 14.0 properly propagates these aggregates across shards and correctly combines results at the coordinator, respecting the comparison semantics of composite types.

### Why This Matters

This feature unlocks powerful new query patterns:

- **Time-series data**: Find MIN/MAX of complex measurements stored as composite types
- **Geospatial queries**: Aggregate over custom point/polygon types
- **Analytics**: Work with structured data without flattening to simple types
- **Multi-dimensional data**: Process arrays and nested structures efficiently

The active pull request for this feature ([#8429](https://github.com/citusdata/citus/pull/8429)) demonstrates Citus's commitment to bringing PostgreSQL 18's innovations to distributed environments.

---

## NOT NULL as Real Constraints {#not-null-constraints}

PostgreSQL 18 makes a fundamental architectural change: **NOT NULL is now a real constraint** stored in `pg_constraint`, not just a column attribute. This brings NOT NULL in line with CHECK constraints and enables new capabilities.

### What Changed

**Before PostgreSQL 18:**
- NOT NULL was just an `attnotnull` flag on columns
- No constraint tracking, no inheritance control
- Limited metadata visibility

**In PostgreSQL 18:**
- NOT NULL constraints are stored in `pg_constraint` with `contype = 'n'`
- Constraints can be named: `ALTER TABLE ... ADD CONSTRAINT name NOT NULL ...`
- NOT NULL can be `NOT VALID` (validated over time for large tables)
- Inheritance can be controlled per-constraint

### Citus 14.0 Integration

Citus fully supports these new NOT NULL constraints in distributed environments:

```sql
-- Create a distributed table with NOT NULL
CREATE TABLE users (
    id int NOT NULL,
    email text NOT NULL,
    nickname text
);

SELECT create_distributed_table('users', 'id');

-- Verify NOT NULL constraints are propagated to workers
SELECT contype, count(*) 
FROM pg_constraint
WHERE conrelid = 'users'::regclass
GROUP BY contype;

 contype | count
---------+-------
 n       |     2
```

The constraints are automatically:
- **Propagated to all shards** across worker nodes
- **Tracked in metadata** for proper catalog visibility
- **Enforced during DML operations** on all nodes
- **Updated when modified** via DDL operations

#### Advanced NOT NULL Features

```sql
-- Add a NOT NULL constraint with a name
ALTER TABLE users 
ADD CONSTRAINT users_email_not_null NOT NULL (email);

-- Add NOT VALID for large tables (validates asynchronously)
ALTER TABLE large_table 
ADD CONSTRAINT check_col NOT NULL (col) NOT VALID;

-- Later, validate the constraint
ALTER TABLE large_table VALIDATE CONSTRAINT check_col;

-- Drop NOT NULL on coordinator - automatically propagates
ALTER TABLE users ALTER COLUMN nickname DROP NOT NULL;
```

### Why This Matters

1. **Better metadata tracking**: NOT NULL constraints are now visible in standard catalog views
2. **Inheritance control**: Fine-grained control over constraint inheritance in table hierarchies  
3. **Non-blocking validation**: Use `NOT VALID` for adding constraints to large distributed tables
4. **Consistent behavior**: NOT NULL works like other constraints across your cluster

---

## JSON_TABLE: Distributed JSON Processing {#json-table}

PostgreSQL 18 introduces `JSON_TABLE()`, a powerful SQL/JSON function that converts JSON data into relational views. Citus 14.0 extends this functionality to work across distributed tables, enabling sophisticated JSON processing in distributed queries.

### Basic JSON_TABLE Usage

```sql
-- Create a distributed table with JSON data
CREATE TABLE my_favorite_books(
    book_collection_id bigserial, 
    jsonb_column jsonb
);

SELECT create_distributed_table('my_favorite_books', 'book_collection_id');

-- Insert JSON data
INSERT INTO my_favorite_books (jsonb_column) VALUES (
'{ "favorites" : [ 
   { "kind" : "mystery", 
     "books" : [ 
       { "title" : "The Count of Monte Cristo", "author" : "Alexandre Dumas"},
       { "title" : "Crime and Punishment", "author" : "Fyodor Dostoevsky" }
     ]
   },
   { "kind" : "drama", 
     "books" : [
       { "title" : "Anna Karenina", "author" : "Leo Tolstoy" }
     ]
   }
]}');

-- Query with JSON_TABLE - extracts nested JSON as relational data
SELECT json_table_output.* 
FROM my_favorite_books,
JSON_TABLE(jsonb_column, '$.favorites[*]' COLUMNS (
   key FOR ORDINALITY, 
   kind text PATH '$.kind',
   NESTED PATH '$.books[*]' COLUMNS (
     title text PATH '$.title', 
     author text PATH '$.author'
))) AS json_table_output
WHERE book_collection_id = 1
ORDER BY key, kind, title;

 key |  kind   |           title           |        author
-----+---------+---------------------------+-----------------------
   1 | mystery | Crime and Punishment      | Fyodor Dostoevsky
   1 | mystery | The Count of Monte Cristo | Alexandre Dumas
   2 | drama   | Anna Karenina            | Leo Tolstoy
```

### Distributed Query Support

Citus 14.0 supports JSON_TABLE in:

#### Router Queries (Single Shard)
```sql
-- Queries targeting a specific shard work efficiently
SELECT json_table_output.* 
FROM my_favorite_books,
JSON_TABLE(jsonb_column, '$.favorites[*]' COLUMNS (...))
WHERE book_collection_id = 1;  -- Router query
```

#### Multi-Shard Queries
```sql
-- Queries across all shards
SELECT json_table_output.kind, COUNT(*) 
FROM my_favorite_books,
JSON_TABLE(jsonb_column, '$.favorites[*]' COLUMNS (
   kind text PATH '$.kind'
)) AS json_table_output
GROUP BY kind
ORDER BY kind;
```

#### Joins with Distributed Tables
```sql
-- JSON_TABLE can participate in distributed joins
CREATE TABLE categories (id int, category_name text);
SELECT create_distributed_table('categories', 'id');

SELECT c.category_name, json_table_output.title
FROM my_favorite_books
JOIN categories c ON (c.id = my_favorite_books.book_collection_id)
CROSS JOIN LATERAL JSON_TABLE(
    jsonb_column, '$.favorites[*]' 
    COLUMNS (title text PATH '$.books[0].title')
) AS json_table_output;
```

#### Correlated Subqueries
```sql
-- JSON_TABLE in WHERE clause subqueries
SELECT COUNT(*)
FROM my_favorite_books 
WHERE (
  SELECT COUNT(*) > 0
  FROM JSON_TABLE(jsonb_column, '$.favorites[*]' 
       COLUMNS (kind text PATH '$.kind')) AS jt
  WHERE jt.kind = 'mystery'
);
```

### Limitations

While Citus 14.0 brings extensive JSON_TABLE support, some restrictions apply:

```sql
-- ❌ Cannot have JSON_TABLE alone in FROM clause with correlated subquery
SELECT *
FROM JSON_TABLE('[...]', '$[*]' COLUMNS (...)) AS foo
WHERE foo.col > (SELECT ... FROM distributed_table WHERE ...);
-- ERROR: correlated subqueries not supported with JSON_TABLE alone

-- ❌ Non-colocated joins
SELECT *
FROM my_favorite_books
JOIN JSON_TABLE(...) AS jt
JOIN other_table ON (book_collection_id != other_table.id);
-- ERROR: complex joins require colocation

-- ✅ Solutions: Include a distributed table in the query
SELECT *
FROM my_favorite_books, 
     JSON_TABLE(...) AS jt
WHERE jt.value > (SELECT ... FROM ...);  -- Now works!
```

### Why This Matters

JSON_TABLE bridges the gap between JSON documents and relational queries:

- **Extract nested structures**: Transform hierarchical JSON into flat tables
- **Join JSON with relational data**: Combine document and table data seamlessly  
- **Distributed analytics**: Process JSON across shards efficiently
- **Flexible schemas**: Query semi-structured data without predefined schemas

---

## Enhanced EXPLAIN Output {#explain-enhancements}

PostgreSQL 18 adds two powerful new EXPLAIN options: `MEMORY` and `SERIALIZE`. Citus 14.0 brings these to distributed queries, providing unprecedented insight into query performance across your cluster.

### MEMORY Option

Shows memory usage during query planning and execution:

```sql
-- Create and populate distributed table
CREATE TABLE dist_table(a int, b int); 
SELECT create_distributed_table('dist_table', 'a'); 
INSERT INTO dist_table SELECT c, c * 10000 FROM generate_series(0, 1000) c; 

-- Explain with memory details
EXPLAIN (costs off, analyze, memory) 
SELECT * FROM dist_table WHERE a BETWEEN 10 AND 20;

                                   QUERY PLAN
---------------------------------------------------------------------------------
Custom Scan (Citus Adaptive) (actual time=5.2..5.4 rows=11 loops=1)
  Task Count: 32
  Tasks Shown: One of 32
  ->  Task
        Node: host=localhost port=9702 dbname=postgres
        ->  Seq Scan (actual time=0.013..0.016 rows=11 loops=1)
              Planning:
                Memory: used=7kB  allocated=8kB        <-- Memory details!
              Planning Time: 0.024 ms
              Execution Time: 0.031 ms
Planning:
  Memory: used=359kB allocated=512kB                  <-- Coordinator memory!
Planning Time: 0.287 ms
Execution Time: 5.9 ms
```

### SERIALIZE Option

Shows the cost of converting query results to output format and sending to client:

```sql
EXPLAIN (costs off, analyze, serialize) 
SELECT * FROM dist_table;

                                   QUERY PLAN
---------------------------------------------------------------------------------
Custom Scan (Citus Adaptive) (actual time=18.4..18.5 rows=1001 loops=1)
  Task Count: 32
  Tuple data received from nodes: 8008 bytes         <-- Data transfer metrics!
  Tasks Shown: One of 32
  ->  Task
        Tuple data received from node: 272 bytes
        Node: host=localhost port=9702
        ->  Seq Scan (actual time=0.013..0.016 rows=34 loops=1)
              Planning Time: 0.024 ms
              Serialization: time=0.000ms output=0kB format=text  <-- Serialize cost!
              Execution Time: 0.031 ms
Planning Time: 0.287 ms
Serialization: time=0.097ms output=20kB format=text   <-- Total serialize cost!
Execution Time: 18.9 ms
```

### Combined Usage

Use both options together for complete visibility:

```sql
EXPLAIN (analyze, memory, serialize, buffers)
SELECT COUNT(*) FROM dist_table WHERE b > 500000;
```

### Why This Matters

These new EXPLAIN options help you:

- **Identify memory bottlenecks**: See which operations consume the most memory
- **Optimize data transfer**: Understand serialization costs in distributed queries
- **Compare worker performance**: Identify slow nodes in your cluster
- **Plan capacity**: Make informed decisions about resource allocation

For distributed systems, understanding these metrics across multiple nodes is crucial for performance tuning.

---

## Additional PostgreSQL 18 Features in Citus 14.0

### Correlated Subquery Optimization

PostgreSQL 18 can transform correlated IN subqueries into joins, and Citus 14.0 leverages this for better distributed query plans:

```sql
-- This query benefits from PG18's optimization
SELECT c.name, c.contact
FROM customers c
WHERE c.id IN (
  SELECT customer_id 
  FROM orders o 
  WHERE o.category = c.category  -- Correlation
);

-- PostgreSQL 18 converts to a join, Citus pushes it down efficiently
```

### Partitioned Table Enhancements

- **Access method inheritance**: Set access methods on partitioned tables
- **Exclusion constraints**: Now supported on distributed partitioned tables
- **Identity columns**: Full support in partitioned hierarchies

### Connection Improvements

Configure modern TLS options via `citus.node_conninfo`:

```sql
-- Use direct SSL negotiation (saves one round-trip)
ALTER SYSTEM SET citus.node_conninfo = 'sslnegotiation=direct';
```

### COPY Enhancements

```sql
-- Specify FORCE_NULL/FORCE_NOT_NULL for all columns
COPY my_table FROM STDIN WITH (
  FORMAT csv, 
  FORCE_NULL *, 
  FORCE_NOT_NULL *
);
```

### Timestamp Handling

```sql
-- AT LOCAL operator for timestamp conversions
INSERT INTO events VALUES (
  1, 
  timestamp '2026-01-15 10:00:00' AT LOCAL
);
```

---

## Looking Ahead {#looking-ahead}

Citus 14.0 represents a major step forward in bringing PostgreSQL 18's innovations to distributed databases. As we continue development, we're working on:

- **MERGE enhancements**: RETURNING clause support, updatable views
- **ALTER TABLE improvements**: Modify generation expressions, DEFAULT for access methods
- **COPY options**: ON_ERROR and LOG_VERBOSITY support
- **Additional optimizations**: More PostgreSQL 18 features in distributed contexts

### Upgrade Path

Citus 14.0 requires PostgreSQL 18. We recommend:

1. **Test in staging**: Validate your workload with Citus 14.0 + PostgreSQL 18
2. **Review query plans**: Some queries may have different (better) plans due to PG18 optimizations
3. **Update applications**: Take advantage of new features like JSON_TABLE and composite aggregates
4. **Monitor performance**: Use new EXPLAIN options to understand distributed query behavior

### Community and Support

- **Documentation**: [docs.citusdata.com](https://docs.citusdata.com/)
- **GitHub**: [github.com/citusdata/citus](https://github.com/citusdata/citus)
- **Slack**: [slack.citusdata.com](https://slack.citusdata.com/)
- **Blog**: [citusdata.com/blog](https://www.citusdata.com/blog/)

---

## Conclusion

Citus 14.0 brings the power of PostgreSQL 18 to distributed databases, enabling sophisticated queries, better performance, and new capabilities across your entire cluster. From self-join elimination to MIN/MAX on composite types, from enhanced EXPLAIN output to JSON_TABLE support—every feature is designed to work seamlessly in a distributed environment.

As PostgreSQL continues to evolve, Citus remains committed to bringing the latest innovations to your distributed database infrastructure. Welcome to Citus 14.0—distributed PostgreSQL 18 is here.

**Ready to upgrade?** Check out our [getting started guide](https://docs.citusdata.com/en/latest/get_started/what_is_citus.html) and join thousands of companies running Citus in production.

---

*Special thanks to the PostgreSQL community for PostgreSQL 18 and to all contributors who made Citus 14.0 possible. For a complete list of changes, see the [CHANGELOG](https://github.com/citusdata/citus/blob/main/CHANGELOG.md).*
