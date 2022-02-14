CREATE SCHEMA "object prop";
SET search_path TO "object prop";

CREATE OR REPLACE FUNCTION citus_get_all_dependencies_for_object(classid oid, objid oid, objsubid int)
    RETURNS SETOF RECORD
    LANGUAGE C STRICT
    AS 'citus', $$citus_get_all_dependencies_for_object$$;
COMMENT ON FUNCTION citus_get_all_dependencies_for_object(classid oid, objid oid, objsubid int)
IS 'emulate what Citus would qualify as dependency when adding a new node';

CREATE OR REPLACE FUNCTION citus_get_dependencies_for_object(classid oid, objid oid, objsubid int)
    RETURNS SETOF RECORD
    LANGUAGE C STRICT
    AS 'citus', $$citus_get_dependencies_for_object$$;
COMMENT ON FUNCTION citus_get_dependencies_for_object(classid oid, objid oid, objsubid int)
IS 'emulate what Citus would qualify as dependency when creating the object';

create type t1 as (a int);
create table test(a int, b t1);
SELECT create_distributed_table('test', 'a');
CREATE VIEW v1 AS SELECT * FROM test;

-- find all the dependencies of table test
SELECT
	pg_identify_object(t.classid, t.objid, t.objsubid)
FROM
	(SELECT * FROM pg_get_object_address('table', '{test}', '{}')) as addr
JOIN LATERAL
	citus_get_all_dependencies_for_object(addr.classid, addr.objid, addr.objsubid) as t(classid oid, objid oid, objsubid int)
ON TRUE
	ORDER BY 1;

-- find all the dependencies of view v1
SELECT
	pg_identify_object(t.classid, t.objid, t.objsubid)
FROM
	(SELECT * FROM pg_get_object_address('view', '{v1}', '{}')) as addr
JOIN LATERAL
	citus_get_all_dependencies_for_object(addr.classid, addr.objid, addr.objsubid) as t(classid oid, objid oid, objsubid int)
ON TRUE
	ORDER BY 1;

-- find all the dependencies of type t1
SELECT
	pg_identify_object(t.classid, t.objid, t.objsubid)
FROM
	(SELECT * FROM pg_get_object_address('type', '{t1}', '{}')) as addr
JOIN LATERAL
	citus_get_all_dependencies_for_object(addr.classid, addr.objid, addr.objsubid) as t(classid oid, objid oid, objsubid int)
ON TRUE
	ORDER BY 1;

-- find non-distributed dependencies of table test
SELECT
	pg_identify_object(t.classid, t.objid, t.objsubid)
FROM
	(SELECT * FROM pg_get_object_address('table', '{test}', '{}')) as addr
JOIN LATERAL
	citus_get_dependencies_for_object(addr.classid, addr.objid, addr.objsubid) as t(classid oid, objid oid, objsubid int)
ON TRUE
	ORDER BY 1;

SET client_min_messages TO ERROR;
DROP SCHEMA  "object prop" CASCADE;
