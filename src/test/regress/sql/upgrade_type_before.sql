CREATE SCHEMA upgrade_type;
SET search_path TO upgrade_type, public;
CREATE TYPE type1 AS (a int, b int);
CREATE TABLE tt (a int PRIMARY KEY, b type1);
SELECT create_distributed_table('tt','a');
INSERT INTO tt VALUES (2, (3,4)::type1);
