-- Tests to check propagation of all view commands
CREATE SCHEMA view_prop_schema;
SET search_path to view_prop_schema;

-- Check creating views depending on different types of tables
-- and from multiple schemas

-- Try to create view depending local table, then try to recreate it after distributing the table
CREATE TABLE view_table_1(id int, val_1 text);
CREATE VIEW prop_view_1 AS
    SELECT * FROM view_table_1;

SELECT create_distributed_table('view_table_1', 'id');
CREATE OR REPLACE VIEW prop_view_1 AS
    SELECT * FROM view_table_1;

-- Try to create view depending local table, then try to recreate it after making the table reference table
CREATE TABLE view_table_2(id int PRIMARY KEY, val_1 text);
CREATE VIEW prop_view_2 AS
    SELECT view_table_1.id, view_table_2.val_1 FROM view_table_1 INNER JOIN view_table_2
    ON view_table_1.id = view_table_2.id;

SELECT create_reference_table('view_table_2');
CREATE OR REPLACE VIEW prop_view_2 AS
    SELECT view_table_1.id, view_table_2.val_1 FROM view_table_1 INNER JOIN view_table_2
    ON view_table_1.id = view_table_2.id;

-- Try to create view depending local table, then try to recreate it after making the table citus local table
CREATE TABLE view_table_3(id int, val_1 text);
CREATE VIEW prop_view_3 AS
    SELECT * FROM view_table_1 WHERE id IN
    (SELECT view_table_2.id FROM view_table_2 INNER JOIN view_table_3 ON view_table_2.id = view_table_3.id);

SELECT 1 FROM citus_add_node('localhost', :master_port, groupid=>0);

ALTER TABLE view_table_3
ADD CONSTRAINT f_key_for_local_table
FOREIGN KEY(id)
REFERENCES view_table_2(id);

CREATE OR REPLACE VIEW prop_view_3 AS
    SELECT * FROM view_table_1 WHERE id IN
    (SELECT view_table_2.id FROM view_table_2 INNER JOIN view_table_3 ON view_table_2.id = view_table_3.id);

-- Try to create view depending on PG metadata table
CREATE VIEW prop_view_4 AS
    SELECT * FROM pg_stat_activity;

-- Try to create view depending on Citus metadata table
CREATE VIEW prop_view_5 AS
    SELECT * FROM citus_dist_stat_activity;

-- Try to create table depending on a local table from another schema, then try to create it again after distributing the table
CREATE SCHEMA view_prop_schema_inner;
SET search_path TO view_prop_schema_inner;

-- Create local table for tests below
CREATE TABLE view_table_4(id int, val_1 text);

-- Create a distributed table and view to test drop view below
CREATE TABLE inner_view_table(id int);
SELECT create_distributed_table('inner_view_table','id');
CREATE VIEW inner_view_prop AS SELECT * FROM inner_view_table;

SET search_path to view_prop_schema;

CREATE VIEW prop_view_6 AS
    SELECT vt1.id, vt4.val_1 FROM view_table_1 AS vt1
    INNER JOIN view_prop_schema_inner.view_table_4 AS vt4 ON vt1.id = vt4.id;

SELECT create_distributed_table('view_prop_schema_inner.view_table_4','id');
CREATE OR REPLACE VIEW prop_view_6 AS
    SELECT vt1.id, vt4.val_1 FROM view_table_1 AS vt1
    INNER JOIN view_prop_schema_inner.view_table_4 AS vt4 ON vt1.id = vt4.id;

-- Show that all views are propagated as distributed object
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_%';

-- Check creating views depending various kind of objects
-- Tests will also check propagating dependent objects

-- Depending on function
SET citus.enable_ddl_propagation TO OFF;
CREATE OR REPLACE FUNCTION func_1_for_view(param_1 int)
RETURNS int
LANGUAGE plpgsql AS
$$
BEGIN
    return param_1;
END;
$$;
RESET citus.enable_ddl_propagation;

-- Show that function will be propagated together with the view
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%func_1_for_view%';

CREATE VIEW prop_view_7 AS SELECT func_1_for_view(id) FROM view_table_1;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%func_1_for_view%';
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_7%';

-- Depending on type
SET citus.enable_ddl_propagation TO OFF;
CREATE TYPE type_for_view_prop AS ENUM ('a','b','c');
RESET citus.enable_ddl_propagation;

-- Show that type will be propagated together with the view
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%type_for_view_prop%';

CREATE VIEW prop_view_8 AS SELECT val_1::type_for_view_prop FROM view_table_1;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%type_for_view_prop%';
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_8%';

-- Depending on another view
CREATE TABLE view_table_5(id int);
CREATE VIEW prop_view_9 AS SELECT * FROM view_table_5;
CREATE VIEW prop_view_10 AS SELECT * FROM prop_view_9;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_9%';
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_10%';

SELECT create_distributed_table('view_table_5', 'id');
CREATE OR REPLACE VIEW prop_view_10 AS SELECT * FROM prop_view_9;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_9%';
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%prop_view_10%';

-- Create view with different options

CREATE TABLE view_table_6(id int, val_1 text);
SELECT create_distributed_table('view_table_6','id');

-- TEMP VIEW is not supported. View will be created locally.
CREATE TEMP VIEW temp_prop_view AS SELECT * FROM view_table_6;

-- Recursive views are supported
CREATE RECURSIVE VIEW nums_1_100_prop_view (n) AS
    VALUES (1)
UNION ALL
    SELECT n+1 FROM nums_1_100_prop_view WHERE n < 100;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%nums_1_100_prop_view%';

-- Aliases are supported
CREATE VIEW aliased_opt_prop_view(alias_1, alias_2) AS SELECT * FROM view_table_6;

-- View options are supported
CREATE VIEW opt_prop_view
    WITH(check_option=CASCADED, security_barrier=true)
    AS SELECT * FROM view_table_6;

CREATE VIEW sep_opt_prop_view
    AS SELECT * FROM view_table_6
    WITH LOCAL CHECK OPTION;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%opt_prop_view%';

-- Check definitions of views are correct on workers
\c - - - :worker_1_port
SET search_path to view_prop_schema;
\d+ aliased_opt_prop_view
\d+ opt_prop_view
\d+ sep_opt_prop_view

-- Drop views and check metadata afterwards
\c - - - :master_port
SET search_path to view_prop_schema;

DROP VIEW prop_view_9 CASCADE;
DROP VIEW opt_prop_view, aliased_opt_prop_view, view_prop_schema_inner.inner_view_prop, sep_opt_prop_view;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%inner_view_prop%';
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%opt_prop_view%';

DROP SCHEMA view_prop_schema_inner CASCADE;
DROP SCHEMA view_prop_schema CASCADE;
