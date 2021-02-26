--
-- Testing indexes on on columnar tables.
--

CREATE SCHEMA columnar_indexes;
SET search_path tO columnar_indexes, public;

--
-- create index with the concurrent option. We should
-- error out during index creation.
-- https://github.com/citusdata/citus/issues/4599
--
create table t(a int, b int) using columnar;
create index CONCURRENTLY t_idx on t(a, b);
\d t
explain insert into t values (1, 2);
insert into t values (1, 2);
SELECT * FROM t;

-- create index without the concurrent option. We should
-- error out during index creation.
create index t_idx on t(a, b);
\d t
explain insert into t values (1, 2);
insert into t values (3, 4);
SELECT * FROM t;

SET client_min_messages TO WARNING;
DROP SCHEMA columnar_indexes CASCADE;
