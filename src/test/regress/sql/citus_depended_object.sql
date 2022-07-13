-- create the udf is_citus_depended_object that is needed for the tests
CREATE OR REPLACE FUNCTION
    pg_catalog.is_citus_depended_object(oid,oid)
    RETURNS bool
    LANGUAGE C
    AS 'citus', $$is_citus_depended_object$$;

-- execute tests in a separate namespace
CREATE SCHEMA citus_dependend_object;
SET search_path TO citus_dependend_object;

-- PG_CLASS VISIBILITY
-- check if we correctly determine whether a relation is citus dependent or not.
CREATE TABLE no_hide_pg_class(relname text);
CREATE TABLE hide_pg_class(relname text);

-- create a relation that depends on noderole type which is a citus object
CREATE TABLE citus_depended_class(nrole noderole);

-- create a relation that depends on columnar access method which is a citus object
CREATE TABLE citus_depended_class2(id int);
SELECT alter_table_set_access_method('citus_depended_class2', 'columnar');

-- create a relation that does not depend on citus
CREATE TABLE citus_independed_class(id int);

-- store all relations
SET citus.hide_citus_dependent_objects TO false;
INSERT INTO no_hide_pg_class SELECT relname FROM pg_class;

-- store all relations except citus relations
SET citus.hide_citus_dependent_objects TO true;
INSERT INTO hide_pg_class SELECT relname FROM pg_class;

-- prove that some relations are hidden or not
SELECT relname,
       CASE
           WHEN relname IN
           (
               SELECT relname FROM no_hide_pg_class
               EXCEPT
               SELECT relname FROM hide_pg_class
           ) THEN true
           ELSE false
       END AS is_hidden
FROM (VALUES ('pg_dist_shard'), ('pg_dist_placement'), ('pg_type'), ('pg_proc'),
('citus_depended_class'), ('citus_depended_class2'), ('citus_independed_class')) rels(relname);

-- PG_TYPE VISIBILITY
-- check if we correctly determine whether a type is citus dependent or not.
CREATE TABLE no_hide_pg_type(typname text);
CREATE TABLE hide_pg_type(typname text);

-- create a type that depends on noderole type which is a citus object
CREATE TYPE citus_depended_type AS (nrole noderole);

-- create a relation that does not depend on citus
CREATE TYPE citus_independed_type AS (id int);

-- store all types
SET citus.hide_citus_dependent_objects TO false;
INSERT INTO no_hide_pg_type SELECT typname FROM pg_type;

-- store all types except citus types
SET citus.hide_citus_dependent_objects TO true;
INSERT INTO hide_pg_type SELECT typname FROM pg_type;

-- prove that some types are hidden or not
SELECT typname,
       CASE
           WHEN typname IN
           (
               SELECT typname FROM no_hide_pg_type
               EXCEPT
               SELECT typname FROM hide_pg_type
           ) THEN true
           ELSE false
       END AS is_hidden
FROM (VALUES ('noderole'), ('_noderole'), ('int'), ('_int'),
('citus_depended_type'), ('citus_independed_type')) types(typname);

-- PG_AM VISIBILITY
-- check if we correctly determine whether an access method is citus dependent or not.
CREATE TABLE no_hide_pg_am(amname text);
CREATE TABLE hide_pg_am(amname text);

-- store all access methods
SET citus.hide_citus_dependent_objects TO false;
INSERT INTO no_hide_pg_am SELECT amname FROM pg_am;

-- store all access methods except citus access methods
SET citus.hide_citus_dependent_objects TO true;
INSERT INTO hide_pg_am SELECT amname FROM pg_am;

-- show all hidden access methods
SELECT amname AS hidden_am FROM no_hide_pg_am
EXCEPT
SELECT amname AS hidden_am FROM hide_pg_am
ORDER BY 1;

-- show all unhidden access methods
SELECT amname AS unhidden_am FROM no_hide_pg_am
EXCEPT
(
    SELECT amname FROM no_hide_pg_am
    EXCEPT
    SELECT amname FROM hide_pg_am
)
ORDER BY 1;

-- PG_PROC VISIBILITY
-- check if we correctly determine whether a procedure is citus dependent or not.
CREATE TABLE no_hide_pg_proc(proname text);
CREATE TABLE hide_pg_proc(proname text);

-- create a procedure that depends on noderole type which is a citus object
CREATE OR REPLACE PROCEDURE citus_depended_proc(nrole noderole)
LANGUAGE SQL
AS $$
$$;

-- create a procedure that does not depend on citus
CREATE OR REPLACE PROCEDURE citus_independed_proc(id int)
LANGUAGE SQL
AS $$
$$;

-- store all access procedures
SET citus.hide_citus_dependent_objects TO false;
INSERT INTO no_hide_pg_proc SELECT proname FROM pg_proc;

-- store all access procedures except citus procedures
SET citus.hide_citus_dependent_objects TO true;
INSERT INTO hide_pg_proc SELECT proname FROM pg_proc;

-- prove that some procedures are hidden or not
SELECT proname,
       CASE
           WHEN proname IN
           (
               SELECT proname FROM no_hide_pg_proc
               EXCEPT
               SELECT proname FROM hide_pg_proc
           ) THEN true
           ELSE false
       END AS is_hidden
FROM (VALUES ('master_add_node'), ('format'),
('citus_depended_proc'), ('citus_independed_proc')) procs(proname);

-- drop the namespace with all its objects
DROP SCHEMA citus_dependend_object CASCADE;
