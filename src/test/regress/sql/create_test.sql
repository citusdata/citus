CREATE SCHEMA "te;'st";
SET search_path to "te;'st", public;

CREATE TABLE dist(a int, b int);
SELECT create_distributed_table('dist', 'a');

CREATE TABLE dist2(a int, b int);
SELECT create_distributed_table('dist2', 'a');

CREATE table ref(a int, b int);
SELECT create_reference_table('ref');
