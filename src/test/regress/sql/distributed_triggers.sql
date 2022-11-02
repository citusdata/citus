SET citus.log_remote_commands TO OFF;
DROP SCHEMA IF EXISTS distributed_triggers CASCADE;
CREATE SCHEMA distributed_triggers;
SET search_path TO 'distributed_triggers';
SET citus.shard_replication_factor = 1;
SET citus.next_shard_id TO 800000;

-- idempotently add node to allow this test to run without add_coordinator
SET client_min_messages TO WARNING;
SELECT 1 FROM master_add_node('localhost', :master_port, groupid => 0);
RESET client_min_messages;

--
-- Test citus.enable_unsafe_triggers
-- Enables arbitrary triggers on distributed tables
--
CREATE TABLE data (
    shard_key_value text not null,
    object_id text not null,
    value jsonb not null
);
ALTER TABLE data
ADD CONSTRAINT data_pk
PRIMARY KEY (shard_key_value, object_id);

/* table of changes */
CREATE TABLE data_changes (
    shard_key_value text not null,
    object_id text not null,
    change_id bigint not null,
    change_time timestamptz default now(),
    operation_type text not null,
    new_value jsonb
);
ALTER TABLE data_changes
ADD CONSTRAINT data_changes_pk
PRIMARY KEY (shard_key_value, object_id, change_id);

SELECT create_distributed_table('data', 'shard_key_value');
SELECT create_distributed_table('data_changes', 'shard_key_value', colocate_with := 'data');

SET citus.enable_unsafe_triggers TO true;
SELECT run_command_on_workers('ALTER SYSTEM SET citus.enable_unsafe_triggers TO true;');
SELECT run_command_on_workers('SELECT pg_reload_conf();');

/* trigger function that is called after any change */
CREATE OR REPLACE FUNCTION record_change()
RETURNS trigger
AS $$
DECLARE
    last_change_id bigint;
BEGIN
    IF (TG_OP = 'DELETE') THEN
      /* get the last change ID for object key in OLD via index(-only) scan */
      SELECT change_id INTO last_change_id
      FROM distributed_triggers.data_changes
      WHERE shard_key_value = OLD.shard_key_value AND object_id = OLD.object_id
      ORDER BY change_id DESC LIMIT 1;

      /* insert a change record for the delete */
      INSERT INTO distributed_triggers.data_changes (shard_key_value, object_id, change_id, operation_type)
      VALUES (OLD.shard_key_value, OLD.object_id, COALESCE(last_change_id + 1, 1), TG_OP);
    ELSE
      /* get the last change ID for object key in NEW via index(-only) scan */
      SELECT change_id INTO last_change_id
      FROM distributed_triggers.data_changes
      WHERE shard_key_value = NEW.shard_key_value AND object_id = NEW.object_id
      ORDER BY change_id DESC LIMIT 1;

      /* insert a change record for the insert/update */
      INSERT INTO distributed_triggers.data_changes (shard_key_value, object_id, change_id, operation_type, new_value)
      VALUES (NEW.shard_key_value, NEW.object_id, COALESCE(last_change_id + 1, 1), TG_OP, NEW.value);
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

SELECT proname from pg_proc WHERE oid='distributed_triggers.record_change'::regproc;
SELECT run_command_on_workers($$SELECT count(*) FROM pg_proc WHERE oid='distributed_triggers.record_change'::regproc$$);

CREATE TRIGGER record_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON data
FOR EACH ROW EXECUTE FUNCTION distributed_triggers.record_change();

-- Trigger function should appear on workers
SELECT proname from pg_proc WHERE oid='distributed_triggers.record_change'::regproc;
SELECT run_command_on_workers($$SELECT count(*) FROM pg_proc WHERE oid='distributed_triggers.record_change'::regproc$$);

INSERT INTO data VALUES ('hello','world','{"hello":"world"}');
INSERT INTO data VALUES ('hello2','world2','{"hello2":"world2"}');
INSERT INTO data VALUES ('hello3','world3','{"hello3":"world3"}');
DELETE FROM data where shard_key_value = 'hello';
BEGIN;
UPDATE data SET value = '{}' where shard_key_value = 'hello3';
END;
DELETE FROM data where shard_key_value = 'hello3';

SELECT * FROM data
ORDER BY shard_key_value, object_id;
SELECT shard_key_value, object_id, change_id, operation_type, new_value
FROM data_changes
ORDER BY shard_key_value, object_id, change_id;

CREATE FUNCTION insert_delete_document(key text, id text)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	INSERT INTO distributed_triggers.data VALUES (key, id, '{"id1":"id2"}');
	DELETE FROM distributed_triggers.data where shard_key_value = key;
END;
$fn$;

SELECT create_distributed_function(
  'insert_delete_document(text, text)', 'key',
  colocate_with := 'data',
  force_delegation := true
);

SELECT insert_delete_document('hello4', 'world4');
BEGIN;
SELECT insert_delete_document('hello4', 'world4');
COMMIT;

SELECT * FROM data
ORDER BY shard_key_value, object_id;
SELECT shard_key_value, object_id, change_id, operation_type, new_value
FROM data_changes
ORDER BY shard_key_value, object_id, change_id;

SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'record_change_trigger%' ORDER BY 1,2;
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'record_change_trigger%';$$);

ALTER TRIGGER "record_change_trigger" ON "distributed_triggers"."data" RENAME TO "new_record_change_trigger";

SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'record_change_trigger%' ORDER BY 1,2;
SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'new_record_change_trigger%' ORDER BY 1,2;
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'record_change_trigger%';$$);
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'new_record_change_trigger%';$$);

--This should fail
DROP TRIGGER record_change_trigger ON data;
DROP TRIGGER new_record_change_trigger ON data;
--Trigger should go away
SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'new_record_change_trigger%' ORDER BY 1,2;

--
-- Run bad triggers
--
CREATE OR REPLACE FUNCTION bad_shardkey_record_change()
RETURNS trigger
AS $$
DECLARE
    last_change_id bigint;
BEGIN
    INSERT INTO distributed_triggers.data_changes (shard_key_value, object_id, change_id, operation_type, new_value)
    VALUES ('BAD', NEW.object_id, COALESCE(last_change_id + 1, 1), TG_OP, NEW.value);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bad_shardkey_record_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON data
FOR EACH ROW EXECUTE FUNCTION distributed_triggers.bad_shardkey_record_change();

-- Bad trigger fired from an individual SQL
-- Query-on-distributed table exception should catch this
INSERT INTO data VALUES ('hello6','world6','{"hello6":"world6"}');

-- Bad trigger fired from SQL inside a force-delegated function
-- Incorrect distribution key exception should catch this
SELECT insert_delete_document('hello6', 'world6');

SELECT * FROM data
ORDER BY shard_key_value, object_id;
SELECT shard_key_value, object_id, change_id, operation_type, new_value
FROM data_changes
ORDER BY shard_key_value, object_id, change_id;
DROP TRIGGER bad_shardkey_record_change_trigger ON data;

CREATE OR REPLACE FUNCTION remote_shardkey_record_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    last_change_id bigint;
BEGIN
    UPDATE distributed_triggers.data_changes SET operation_type = TG_OP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER remote_shardkey_record_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON data
FOR EACH ROW EXECUTE FUNCTION distributed_triggers.remote_shardkey_record_change();

CREATE FUNCTION insert_document(key text, id text)
RETURNS void
LANGUAGE plpgsql
AS $fn$
BEGIN
	INSERT INTO distributed_triggers.data VALUES (key, id, '{"id1":"id2"}');
	DELETE FROM distributed_triggers.data where shard_key_value = key;
END;
$fn$;

SELECT create_distributed_function(
  'insert_document(text, text)', 'key',
  colocate_with := 'data',
  force_delegation := false
);

BEGIN;
SELECT insert_document('hello7', 'world7');
END;

SELECT insert_document('hello7', 'world7');

SELECT * FROM data
ORDER BY shard_key_value, object_id;
SELECT shard_key_value, object_id, change_id, operation_type, new_value
FROM data_changes
ORDER BY shard_key_value, object_id, change_id;

--
-- Triggers (tables) which are not colocated
--
CREATE TABLE emptest (
    empname           text NOT NULL PRIMARY KEY,
    salary            integer
);

CREATE TABLE emptest_audit(
    operation         char(1)   NOT NULL,
    stamp             timestamp NOT NULL,
    userid            text      NOT NULL,
    empname           text      NOT NULL,
    salary            integer,
    PRIMARY KEY (empname, userid, stamp, operation, salary)
);

SELECT create_distributed_table('emptest','empname',colocate_with :='none');
SELECT create_distributed_table('emptest_audit','empname',colocate_with :='none');

CREATE OR REPLACE FUNCTION process_emp_audit() RETURNS TRIGGER AS $emp_audit$
    BEGIN
        --
        -- Create a row in emp_audit to reflect the operation performed on emp,
        -- making use of the special variable TG_OP to work out the operation.
        --
        IF (TG_OP = 'DELETE') THEN
            INSERT INTO distributed_triggers.emptest_audit SELECT 'D', now(), user, OLD.*;
        ELSIF (TG_OP = 'UPDATE') THEN
            INSERT INTO distributed_triggers.emptest_audit SELECT 'U', now(), user, NEW.*;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO distributed_triggers.emptest_audit SELECT 'I', now(), user, NEW.*;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$emp_audit$ LANGUAGE plpgsql;

CREATE TRIGGER emptest_audit
AFTER INSERT OR UPDATE OR DELETE ON emptest
    FOR EACH ROW EXECUTE FUNCTION distributed_triggers.process_emp_audit();

INSERT INTO emptest VALUES ('test1', 1);
INSERT INTO emptest VALUES ('test2', 1);
INSERT INTO emptest VALUES ('test3', 1);
INSERT INTO emptest VALUES ('test4', 1);

SELECT operation, userid, empname, salary
FROM emptest_audit
ORDER BY 3,1;

DELETE from emptest;

SELECT operation, userid, empname, salary
FROM emptest_audit
ORDER BY 3,1;

CREATE VIEW emp_triggers AS
    SELECT tgname, tgrelid::regclass, tgenabled
    FROM pg_trigger
    WHERE tgrelid::regclass::text like 'emptest%'
    ORDER BY 1, 2;
SELECT * FROM emp_triggers ORDER BY 1,2;

-- Triggers "FOR EACH STATEMENT"
CREATE TABLE record_op (
    empname        text NOT NULL,
    operation_type text not null,
    stamp          timestamp NOT NULL
);
ALTER TABLE record_op REPLICA IDENTITY FULL;

SELECT create_distributed_table('record_op', 'empname', colocate_with := 'emptest');
CREATE OR REPLACE FUNCTION record_emp() RETURNS TRIGGER AS $rec_audit$
    BEGIN
        INSERT INTO distributed_triggers.record_op SELECT 'dummy', TG_OP, now();
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$rec_audit$ LANGUAGE plpgsql;

CREATE TRIGGER record_emp_trig
AFTER INSERT OR UPDATE OR DELETE ON emptest
    FOR EACH STATEMENT EXECUTE FUNCTION distributed_triggers.record_emp();

INSERT INTO emptest VALUES ('test6', 1);
DELETE FROM emptest;
SELECT * FROM emptest;
SELECT operation_type FROM record_op;

--
-- Triggers on reference tables
--
CREATE TABLE data_ref_table (
    shard_key_value text not null,
    object_id text not null,
    value jsonb not null
);
ALTER TABLE data_ref_table
ADD CONSTRAINT data_ref_pk
PRIMARY KEY (shard_key_value, object_id);
SELECT create_reference_table('data_ref_table');

-- Trigger function record_change operates on data_changes which is *not* colocated with the reference table
CREATE TRIGGER record_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON data_ref_table
FOR EACH ROW EXECUTE FUNCTION distributed_triggers.record_change();

TRUNCATE TABLE data_changes;
INSERT INTO data_ref_table VALUES ('hello','world','{"ref":"table"}');
INSERT INTO data_ref_table VALUES ('hello2','world2','{"ref":"table"}');
DELETE FROM data_ref_table where shard_key_value = 'hello';
BEGIN;
UPDATE data_ref_table SET value = '{}' where shard_key_value = 'hello2';
END;
TABLE data_changes ORDER BY shard_key_value, object_id, change_id;
TABLE data_ref_table ORDER BY shard_key_value, object_id;

-- Colocate data_changes table with reference table
SELECT undistribute_table('data_changes');
SELECT create_reference_table('data_changes');

INSERT INTO data_ref_table VALUES ('hello','world','{"ref":"table"}');
TABLE data_changes ORDER BY shard_key_value, object_id, change_id;
TABLE data_ref_table ORDER BY shard_key_value, object_id;

-- Create data_changes table locally with reference table
DROP TABLE data_changes;

/* table of changes local to each placement of the reference table */
CREATE TABLE data_changes (
    shard_key_value text not null,
    object_id text not null,
    change_id bigint not null,
    change_time timestamptz default now(),
    operation_type text not null,
    new_value jsonb
);
SELECT run_command_on_workers($$CREATE TABLE distributed_triggers.data_changes(
    shard_key_value text not null,
    object_id text not null,
    change_id bigint not null,
    change_time timestamptz default now(),
    operation_type text not null,
    new_value jsonb);
$$);

SELECT run_command_on_workers('SELECT count(*) FROM distributed_triggers.data_changes;');

INSERT INTO data_ref_table VALUES ('hello','world','{"ref":"table"}');
INSERT INTO data_ref_table VALUES ('hello2','world2','{"ref":"table"}');
BEGIN;
UPDATE data_ref_table SET value = '{}';
END;

SELECT run_command_on_workers('SELECT count(*) FROM distributed_triggers.data_changes;');
TABLE data_ref_table ORDER BY shard_key_value, object_id;

--
--Triggers on partitioned tables
--
CREATE TABLE sale(sale_date date not null, state_code text, product_sku text, units integer)
PARTITION BY list (state_code);
ALTER TABLE sale ADD CONSTRAINT sale_pk PRIMARY KEY (state_code, sale_date);
CREATE TABLE sale_newyork PARTITION OF sale FOR VALUES IN ('NY');
CREATE TABLE sale_california PARTITION OF sale FOR VALUES IN ('CA');

CREATE TABLE record_sale(operation_type text not null, product_sku text, state_code text, units integer, PRIMARY KEY(state_code, product_sku, operation_type, units));

SELECT create_distributed_table('sale', 'state_code');
SELECT create_distributed_table('record_sale', 'state_code', colocate_with := 'sale');

CREATE OR REPLACE FUNCTION record_sale()
RETURNS trigger
AS $$
BEGIN
    INSERT INTO distributed_triggers.record_sale(operation_type, product_sku, state_code, units)
    VALUES (TG_OP, NEW.product_sku, NEW.state_code, NEW.units);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER record_sale_trigger
AFTER INSERT OR UPDATE OR DELETE ON sale
FOR EACH ROW EXECUTE FUNCTION distributed_triggers.record_sale();

INSERT INTO sale(sale_date,state_code,product_sku,units) VALUES
('2019-01-01', 'CA', 'AZ-000A1',   85),
('2019-01-02', 'CA', 'AZ-000A1',   6),
('2019-01-03', 'NY', 'AZ-000A2',   83),
('2019-02-01', 'CA', 'AZ-000A2',   59),
('2019-02-02', 'CA', 'AZ-000A1',   9),
('2019-02-03', 'NY', 'AZ-000A1',   47);

TABLE sale ORDER BY state_code, sale_date;
SELECT operation_type, product_sku, state_code FROM record_sale ORDER BY 1,2,3;

--
--Test ALTER TRIGGER
--
-- Pre PG15, renaming the trigger on the parent table didn't rename the same trigger on
-- the children as well. Hence, let's not print the trigger names of the children
-- In PG15, rename is consistent for all partitions of the parent
-- This is tested in pg15.sql file.

CREATE VIEW sale_triggers AS
    SELECT tgname, tgrelid::regclass, tgenabled
    FROM pg_trigger
    WHERE tgrelid::regclass::text = 'sale'
    ORDER BY 1, 2;

SELECT * FROM sale_triggers ORDER BY 1,2;
ALTER TRIGGER "record_sale_trigger" ON "distributed_triggers"."sale" RENAME TO "new_record_sale_trigger";
SELECT * FROM sale_triggers ORDER BY 1,2;

CREATE EXTENSION seg;
ALTER TRIGGER "emptest_audit" ON "emptest" DEPENDS ON EXTENSION seg;

DROP TABLE data_ref_table;
--
--Triggers with add/remove node
--
SELECT * FROM master_drain_node('localhost', :worker_2_port);
SELECT 1 from master_remove_node('localhost', :worker_2_port);

CREATE TABLE distributed_table(value int);
CREATE TABLE distributed_table_change(value int);

SELECT create_distributed_table('distributed_table', 'value', colocate_with => 'none');
SELECT create_distributed_table('distributed_table_change', 'value', colocate_with => 'distributed_table');

CREATE FUNCTION insert_99() RETURNS trigger AS $insert_99$
BEGIN
    INSERT INTO distributed_triggers.distributed_table_change VALUES (99);
    RETURN NEW;
END;
$insert_99$ LANGUAGE plpgsql;

CREATE TRIGGER insert_99_trigger
AFTER DELETE ON distributed_table
FOR EACH ROW EXECUTE FUNCTION distributed_triggers.insert_99();

SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'insert_99_trigger%' ORDER BY 1,2;
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'insert_99_trigger%'$$);

INSERT INTO distributed_table VALUES (99);
DELETE FROM distributed_table;
SELECT * FROM distributed_table_change;

-- add the node back
SELECT 1 from master_add_node('localhost', :worker_2_port);
INSERT INTO distributed_table VALUES (99);
DELETE FROM distributed_table;
SELECT * FROM distributed_table_change;

SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'insert_99_trigger%' ORDER BY 1,2;
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'insert_99_trigger%'$$);

CREATE TABLE "dist_\'table"(a int);

CREATE FUNCTION trigger_func()
  RETURNS trigger
  LANGUAGE plpgsql
AS $function$
BEGIN
	RETURN NULL;
END;
$function$;

CREATE TRIGGER default_mode_trigger
AFTER UPDATE OR DELETE ON "dist_\'table"
FOR STATEMENT EXECUTE FUNCTION trigger_func();

CREATE TRIGGER "disabled_trigger\'"
AFTER UPDATE OR DELETE ON "dist_\'table"
FOR STATEMENT EXECUTE FUNCTION trigger_func();

ALTER TABLE "dist_\'table" DISABLE trigger "disabled_trigger\'";

CREATE TRIGGER replica_trigger
AFTER UPDATE OR DELETE ON "dist_\'table"
FOR STATEMENT EXECUTE FUNCTION trigger_func();

ALTER TABLE "dist_\'table" ENABLE REPLICA trigger replica_trigger;

CREATE TRIGGER always_enabled_trigger
AFTER UPDATE OR DELETE ON "dist_\'table"
FOR STATEMENT EXECUTE FUNCTION trigger_func();

ALTER TABLE "dist_\'table" ENABLE ALWAYS trigger always_enabled_trigger;

CREATE TRIGGER noop_enabled_trigger
AFTER UPDATE OR DELETE ON "dist_\'table"
FOR STATEMENT EXECUTE FUNCTION trigger_func();

ALTER TABLE "dist_\'table" ENABLE trigger noop_enabled_trigger;

SELECT create_distributed_table('dist_\''table', 'a');

SELECT bool_and(tgenabled = 'O') FROM pg_trigger WHERE tgname LIKE 'default_mode_trigger%';
SELECT run_command_on_workers($$SELECT bool_and(tgenabled = 'O') FROM pg_trigger WHERE tgname LIKE 'default_mode_trigger%'$$);

SELECT bool_and(tgenabled = 'D') FROM pg_trigger WHERE tgname LIKE 'disabled_trigger%';
SELECT run_command_on_workers($$SELECT bool_and(tgenabled = 'D') FROM pg_trigger WHERE tgname LIKE 'disabled_trigger%'$$);

SELECT bool_and(tgenabled = 'R') FROM pg_trigger WHERE tgname LIKE 'replica_trigger%';
SELECT run_command_on_workers($$SELECT bool_and(tgenabled = 'R') FROM pg_trigger WHERE tgname LIKE 'replica_trigger%'$$);

SELECT bool_and(tgenabled = 'A') FROM pg_trigger WHERE tgname LIKE 'always_enabled_trigger%';
SELECT run_command_on_workers($$SELECT bool_and(tgenabled = 'A') FROM pg_trigger WHERE tgname LIKE 'always_enabled_trigger%'$$);

SELECT bool_and(tgenabled = 'O') FROM pg_trigger WHERE tgname LIKE 'noop_enabled_trigger%';
SELECT run_command_on_workers($$SELECT bool_and(tgenabled = 'O') FROM pg_trigger WHERE tgname LIKE 'noop_enabled_trigger%'$$);

CREATE TABLE citus_local(a int);

CREATE FUNCTION citus_local_trig_func()
  RETURNS trigger
  LANGUAGE plpgsql
AS $function$
BEGIN
	RETURN NULL;
END;
$function$;

CREATE TRIGGER citus_local_trig
AFTER UPDATE OR DELETE ON citus_local
FOR STATEMENT EXECUTE FUNCTION citus_local_trig_func();

-- make sure that trigger is initially not disabled
SELECT tgenabled = 'D' FROM pg_trigger WHERE tgname LIKE 'citus_local_trig%';

ALTER TABLE citus_local DISABLE trigger citus_local_trig;

SELECT citus_add_local_table_to_metadata('citus_local');

SELECT bool_and(tgenabled = 'D') FROM pg_trigger WHERE tgname LIKE 'citus_local_trig%';
SELECT run_command_on_workers($$SELECT bool_and(tgenabled = 'D') FROM pg_trigger WHERE tgname LIKE 'citus_local_trig%'$$);

CREATE TABLE dist_trigger_depends_on_test(a int);

CREATE FUNCTION dist_trigger_depends_on_test_func()
  RETURNS trigger
  LANGUAGE plpgsql
AS $function$
BEGIN
	RETURN NULL;
END;
$function$;

CREATE TRIGGER dist_trigger_depends_on_test_trig
AFTER UPDATE OR DELETE ON dist_trigger_depends_on_test
FOR STATEMENT EXECUTE FUNCTION dist_trigger_depends_on_test_func();

ALTER trigger dist_trigger_depends_on_test_trig ON dist_trigger_depends_on_test DEPENDS ON EXTENSION seg;

SELECT create_distributed_table('dist_trigger_depends_on_test', 'a');
SELECT create_reference_table('dist_trigger_depends_on_test');
SELECT citus_add_local_table_to_metadata('dist_trigger_depends_on_test');

SET client_min_messages TO ERROR;
RESET citus.enable_unsafe_triggers;
SELECT run_command_on_workers('ALTER SYSTEM RESET citus.enable_unsafe_triggers;');
SELECT run_command_on_workers('SELECT pg_reload_conf();');
SET citus.log_remote_commands TO off;

DROP SCHEMA distributed_triggers CASCADE;
