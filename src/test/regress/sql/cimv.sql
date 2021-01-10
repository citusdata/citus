--
-- CIMV
--   Tests for Citus Incremental Materialized Views
--

\set VERBOSITY terse
SET citus.next_shard_id TO 400000;
CREATE SCHEMA cimv;
SET search_path TO cimv, public;

SET citus.shard_count TO 4;

CREATE TABLE events (a int, b int, c double precision, d timestamp, e bigint);

INSERT INTO events
SELECT v % 10 AS a,
       v % 100 AS b,
       v / 3.0 AS c,
       timestamp '2020-01-01 20:00:00' +
       ((v / 10000.0) * (timestamp '2020-01-01 15:00:00' -
                   timestamp '2020-01-01 10:00:00')) AS d,
       v AS e
FROM generate_series(1, 10000) v;

CREATE MATERIALIZED VIEW mv WITH (citus.cimv) AS
SELECT a,
       date_trunc('hour', d) AS d_hour,
       min(b) AS min_b,
       max(b) AS max_b,
       avg(b) AS avg_b,
       min(c) AS min_c,
       max(c) AS max_c,
       avg(c) AS avg_c,
       min(e) AS min_e,
       max(e) AS max_e,
       avg(e) AS avg_e
FROM events
WHERE b > 10
GROUP BY a, d_hour;

SELECT a,
       d_hour,
       min_b::numeric(12,2),
       max_b::numeric(12,2),
       avg_b::numeric(12,2),
       min_c::numeric(12,2),
       max_c::numeric(12,2),
       avg_c::numeric(12,2),
       min_e::numeric(12,2),
       max_e::numeric(12,2),
       avg_e::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

INSERT INTO events
SELECT v % 10 AS a,
       v % 100 AS b,
       v / 3.0 AS c,
       timestamp '2020-01-01 20:00:00' +
       ((v / 10000.0) * (timestamp '2020-01-01 15:00:00' -
                   timestamp '2020-01-01 10:00:00')) AS d,
       v AS e
FROM generate_series(10000, 11000) v;

SELECT a,
       d_hour,
       min_b::numeric(12,2),
       max_b::numeric(12,2),
       avg_b::numeric(12,2),
       min_c::numeric(12,2),
       max_c::numeric(12,2),
       avg_c::numeric(12,2),
       min_e::numeric(12,2),
       max_e::numeric(12,2),
       avg_e::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

DELETE FROM events WHERE b < 100;

DROP VIEW mv;

DROP MATERIALIZED VIEW mv;

CREATE MATERIALIZED VIEW mv WITH (citus.cimv) AS
SELECT a,
       date_trunc('hour', d) AS d_hour,
       avg(b) AS avg_b
FROM events
WHERE b > 10
GROUP BY a, d_hour;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

DELETE FROM events WHERE b < 20;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

UPDATE events SET b = b + b;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

DROP MATERIALIZED VIEW mv;

CREATE MATERIALIZED VIEW mv WITH (citus.cimv, citus.insertonlycapture) AS
SELECT a,
       date_trunc('hour', d) AS d_hour,
       avg(b) AS avg_b
FROM events
WHERE b > 10
GROUP BY a, d_hour;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;


INSERT INTO events
SELECT v % 10 AS a,
       v % 100 AS b,
       v / 3.0 AS c,
       timestamp '2020-01-01 20:00:00' +
       ((v / 10000.0) * (timestamp '2020-01-01 15:00:00' -
                   timestamp '2020-01-01 10:00:00')) AS d,
       v AS e
FROM generate_series(11000, 12000) v;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

DELETE FROM events WHERE b < 100;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

UPDATE events SET b = b + b;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

REFRESH MATERIALIZED VIEW mv WITH NO DATA;

SELECT * FROM mv;

REFRESH MATERIALIZED VIEW mv;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;


DROP MATERIALIZED VIEW mv;

CREATE MATERIALIZED VIEW mv WITH (citus.cimv, citus.insertonlycapture) AS
SELECT a,
       date_trunc('hour', d) AS d_hour,
       avg(b) AS avg_b
FROM events
WHERE b > 10
GROUP BY a, d_hour WITH NO DATA;

SELECT * FROM mv;

DROP MATERIALIZED VIEW mv;

SELECT create_distributed_table('events', 'a');

CREATE MATERIALIZED VIEW mv WITH (citus.cimv) AS
SELECT a,
       date_trunc('hour', d) AS d_hour,
       min(b) AS min_b,
       max(b) AS max_b,
       avg(b) AS avg_b,
       min(c) AS min_c,
       max(c) AS max_c,
       avg(c) AS avg_c,
       min(e) AS min_e,
       max(e) AS max_e,
       avg(e) AS avg_e
FROM events
WHERE b > 10
GROUP BY a, d_hour;

SELECT a,
       d_hour,
       min_b::numeric(12,2),
       max_b::numeric(12,2),
       avg_b::numeric(12,2),
       min_c::numeric(12,2),
       max_c::numeric(12,2),
       avg_c::numeric(12,2),
       min_e::numeric(12,2),
       max_e::numeric(12,2),
       avg_e::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

INSERT INTO events
SELECT v % 10 AS a,
       v % 100 AS b,
       v / 3.0 AS c,
       timestamp '2020-01-01 20:00:00' +
       ((v / 10000.0) * (timestamp '2020-01-01 15:00:00' -
                   timestamp '2020-01-01 10:00:00')) AS d,
       v AS e
FROM generate_series(12000, 13000) v;

SELECT a,
       d_hour,
       min_b::numeric(12,2),
       max_b::numeric(12,2),
       avg_b::numeric(12,2),
       min_c::numeric(12,2),
       max_c::numeric(12,2),
       avg_c::numeric(12,2),
       min_e::numeric(12,2),
       max_e::numeric(12,2),
       avg_e::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

DELETE FROM events WHERE b < 100;

DROP VIEW mv;

DROP MATERIALIZED VIEW mv;

CREATE MATERIALIZED VIEW mv WITH (citus.cimv) AS
SELECT a,
       date_trunc('hour', d) AS d_hour,
       avg(b) AS avg_b
FROM events
WHERE b > 10
GROUP BY a, d_hour;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

DELETE FROM events WHERE b < 20;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;


UPDATE events SET b = b + b;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;


DROP MATERIALIZED VIEW mv;

CREATE MATERIALIZED VIEW mv WITH (citus.cimv, citus.insertonlycapture) AS
SELECT a,
       date_trunc('hour', d) AS d_hour,
       avg(b) AS avg_b
FROM events
WHERE b > 10
GROUP BY a, d_hour;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

INSERT INTO events
SELECT v % 10 AS a,
       v % 100 AS b,
       v / 3.0 AS c,
       timestamp '2020-01-01 20:00:00' +
       ((v / 10000.0) * (timestamp '2020-01-01 15:00:00' -
                   timestamp '2020-01-01 10:00:00')) AS d,
       v AS e
FROM generate_series(13000, 14000) v;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

DELETE FROM events WHERE b < 100;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

UPDATE events SET b = b + b;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

REFRESH MATERIALIZED VIEW mv WITH NO DATA;

SELECT * FROM mv;

REFRESH MATERIALIZED VIEW mv;

SELECT a,
       d_hour,
       avg_b::numeric(12,2)
FROM mv
ORDER BY a, d_hour;

DROP MATERIALIZED VIEW mv;

CREATE MATERIALIZED VIEW mv WITH (citus.cimv, citus.insertonlycapture) AS
SELECT a,
       date_trunc('hour', d) AS d_hour,
       avg(b) AS avg_b
FROM events
WHERE b > 10
GROUP BY a, d_hour WITH NO DATA;

SELECT * FROM mv;

-- make sure same mv can be created in different schemas without overlap
CREATE SCHEMA another_schema;
SET search_path to another_schema;
SET citus.shard_count TO 4;

CREATE TABLE events (a int, b int, c double precision, d timestamp, e bigint);

CREATE MATERIALIZED VIEW mv WITH (citus.cimv) AS
SELECT a,
       date_trunc('hour', d) AS d_hour,
       min(b) AS min_b,
       max(b) AS max_b,
       avg(b) AS avg_b,
       min(c) AS min_c,
       max(c) AS max_c,
       avg(c) AS avg_c,
       min(e) AS min_e,
       max(e) AS max_e,
       avg(e) AS avg_e
FROM events
WHERE b > 10
GROUP BY a, d_hour;

DROP MATERIALIZED VIEW mv;

-- test that another user can create CIMV as well
CREATE USER new_user;
CREATE SCHEMA another_schema2;
SET search_path to another_schema2;

CREATE TABLE events (a int, b int, c double precision, d timestamp, e bigint);

CREATE MATERIALIZED VIEW mv WITH (citus.cimv) AS
SELECT a,
       date_trunc('hour', d) AS d_hour,
       min(b) AS min_b,
       max(b) AS max_b,
       avg(b) AS avg_b,
       min(c) AS min_c,
       max(c) AS max_c,
       avg(c) AS avg_c,
       min(e) AS min_e,
       max(e) AS max_e,
       avg(e) AS avg_e
FROM events
WHERE b > 10
GROUP BY a, d_hour;

REFRESH MATERIALIZED VIEW mv;
DROP MATERIALIZED VIEW mv CASCADE;

SET client_min_messages TO WARNING; -- suppress cascade messages
DROP SCHEMA cimv CASCADE;
