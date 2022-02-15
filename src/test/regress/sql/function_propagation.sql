CREATE SCHEMA function_propagation_schema;
SET search_path TO 'function_propagation_schema';

-- Check whether supported dependencies can be distributed while propagating functions

-- Check types
SET citus.enable_metadata_sync TO OFF;
    CREATE TYPE function_prop_type AS (a int, b int);
RESET citus.enable_metadata_sync;

CREATE OR REPLACE FUNCTION func_1(param_1 function_prop_type)
RETURNS int
LANGUAGE plpgsql AS
$$
BEGIN
    return 1;
END;
$$;

-- Check all dependent objects and function depends on all nodes
SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema'::regnamespace::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.function_prop_type'::regtype::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_1'::regproc::oid;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema'::regnamespace::oid;$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.function_prop_type'::regtype::oid;$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_1'::regproc::oid;$$) ORDER BY 1,2;

SET citus.enable_metadata_sync TO OFF;
    CREATE TYPE function_prop_type_2 AS (a int, b int);
RESET citus.enable_metadata_sync;

CREATE OR REPLACE FUNCTION func_2(param_1 int)
RETURNS function_prop_type_2
LANGUAGE plpgsql AS
$$
BEGIN
    return 1;
END;
$$;

SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.function_prop_type_2'::regtype::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_2'::regproc::oid;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.function_prop_type_2'::regtype::oid;$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_2'::regproc::oid;$$) ORDER BY 1,2;

-- Have a separate check for type created in transaction
BEGIN;
    CREATE TYPE function_prop_type_3 AS (a int, b int);
COMMIT;

-- Objects in the body part is not found as dependency
CREATE OR REPLACE FUNCTION func_3(param_1 int)
RETURNS int
LANGUAGE plpgsql AS
$$
DECLARE
    internal_param1 function_prop_type_3;
BEGIN
    return 1;
END;
$$;

SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.function_prop_type_3'::regtype::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_3'::regproc::oid;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_3'::regproc::oid;$$) ORDER BY 1,2;

-- Check sequences
-- Note that after pg 14 creating sequence doesn't create type
-- it is expected for versions > pg14 to fail sequence tests below
CREATE SEQUENCE function_prop_seq;

-- Show that sequence is not distributed yet
SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.function_prop_seq'::regclass::oid;

CREATE OR REPLACE FUNCTION func_4(param_1 function_prop_seq)
RETURNS int
LANGUAGE plpgsql AS
$$
BEGIN
    return 1;
END;
$$;

SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.function_prop_seq'::regclass::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_4'::regproc::oid;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.function_prop_seq'::regclass::oid;$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_4'::regproc::oid;$$) ORDER BY 1,2;

CREATE SEQUENCE function_prop_seq_2;

-- Show that sequence is not distributed yet
SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.function_prop_seq_2'::regclass::oid;

CREATE OR REPLACE FUNCTION func_5(param_1 int)
RETURNS function_prop_seq_2
LANGUAGE plpgsql AS
$$
BEGIN
    return 1;
END;
$$;

SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.function_prop_seq_2'::regclass::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_5'::regproc::oid;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.function_prop_seq_2'::regclass::oid;$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_5'::regproc::oid;$$) ORDER BY 1,2;

-- Check table
CREATE TABLE function_prop_table(a int, b int);

-- Non-distributed table is not distributed as dependency
CREATE OR REPLACE FUNCTION func_6(param_1 function_prop_table)
RETURNS int
LANGUAGE plpgsql AS
$$
BEGIN
    return 1;
END;
$$;

CREATE OR REPLACE FUNCTION func_7(param_1 int)
RETURNS function_prop_table
LANGUAGE plpgsql AS
$$
BEGIN
    return 1;
END;
$$;

-- Functions can be created with distributed table dependency
SELECT create_distributed_table('function_prop_table', 'a');
CREATE OR REPLACE FUNCTION func_8(param_1 function_prop_table)
RETURNS int
LANGUAGE plpgsql AS
$$
BEGIN
    return 1;
END;
$$;

SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_8'::regproc::oid;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_8'::regproc::oid;$$) ORDER BY 1,2;

-- Views are not supported
CREATE VIEW function_prop_view AS SELECT * FROM function_prop_table;
CREATE OR REPLACE FUNCTION func_9(param_1 function_prop_view)
RETURNS int
LANGUAGE plpgsql AS
$$
BEGIN
    return 1;
END;
$$;

CREATE OR REPLACE FUNCTION func_10(param_1 int)
RETURNS function_prop_view
LANGUAGE plpgsql AS
$$
BEGIN
    return 1;
END;
$$;

-- Check within transaction
BEGIN;
    CREATE TYPE type_in_transaction AS (a int, b int);
    CREATE OR REPLACE FUNCTION func_in_transaction(param_1 type_in_transaction)
    RETURNS int
    LANGUAGE plpgsql AS
    $$
    BEGIN
        return 1;
    END;
    $$;

    -- Within transaction functions are not distributed
    SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.type_in_transaction'::regtype::oid;
    SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_in_transaction'::regproc::oid;
COMMIT;

-- Show that recreating it outside transaction distributes the function and dependencies
CREATE OR REPLACE FUNCTION func_in_transaction(param_1 type_in_transaction)
RETURNS int
LANGUAGE plpgsql AS
$$
BEGIN
    return 1;
END;
$$;

SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.type_in_transaction'::regtype::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_in_transaction'::regproc::oid;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.type_in_transaction'::regtype::oid;$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.func_in_transaction'::regproc::oid;$$) ORDER BY 1,2;

-- Test for SQL function with unsupported object in function body
CREATE TABLE table_in_sql_body(id int);

CREATE FUNCTION max_of_table()
RETURNS int
LANGUAGE SQL AS
$$
    SELECT max(id) table_in_sql_body
$$;

-- Show that only function has propagated, since the table is not resolved as dependency
SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.type_in_transaction'::regclass::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.max_of_table'::regproc::oid;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from citus.pg_dist_object where objid = 'function_propagation_schema.max_of_table'::regproc::oid;$$) ORDER BY 1,2;

RESET search_path;
