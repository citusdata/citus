-------------------------------
-- Regression Test Script for Issue 7784
-- This script tests INSERT ... SELECT with a CTE for:
--  1. Schema based sharding.
--  2. A distributed table.
-------------------------------

-- Enable schema-based sharding
SET citus.shard_replication_factor TO 1;
SET citus.enable_schema_based_sharding TO ON;

--------------------------------------------------
-- Case 1: Schema based sharding
--------------------------------------------------
CREATE SCHEMA issue_7784_schema_based;
SET search_path = issue_7784_schema_based, public;

-- Create a table for schema based sharding
CREATE TABLE version_local (
    id bigserial NOT NULL,
    description varchar(255),
    PRIMARY KEY (id)
);

-- Insert an initial row.
INSERT INTO version_local (description) VALUES ('Version 1');

-- Duplicate the row using a CTE and INSERT ... SELECT.
WITH v AS (
    SELECT * FROM version_local WHERE description = 'Version 1'
)
INSERT INTO version_local (description)
SELECT description FROM v;

-- Expected output:
--   id | description
--  ----+-------------
--    1 | Version 1
--    2 | Version 1

-- Query the table and order by id for consistency.
SELECT * FROM version_local ORDER BY id;

--------------------------------------------------
-- Case 2: Distributed Table Scenario
--------------------------------------------------
SET citus.enable_schema_based_sharding TO OFF;
CREATE SCHEMA issue_7784_distributed;
SET search_path = issue_7784_distributed, public;

-- Create a table for the distributed test.
CREATE TABLE version_dist (
    id bigserial NOT NULL,
    description varchar(255),
    PRIMARY KEY (id)
);

-- Register the table as distributed using the 'id' column as the distribution key.
SELECT create_distributed_table('version_dist', 'id');

-- Insert an initial row.
INSERT INTO version_dist (description) VALUES ('Version 1');

-- Duplicate the row using a CTE and INSERT ... SELECT.
WITH v AS (
    SELECT * FROM version_dist WHERE description = 'Version 1'
)
INSERT INTO version_dist (description)
SELECT description FROM v;

-- Expected output:
--   id | description
--  ----+-------------
--    1 | Version 1
--    2 | Version 1

-- Query the table and order by id for consistency.
SELECT * FROM version_dist ORDER BY id;

DROP SCHEMA issue_7784_schema_based CASCADE;
DROP SCHEMA issue_7784_distributed CASCADE;