\set VERBOSITY terse

SET citus.next_shard_id TO 1507000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;
SET citus.log_local_commands TO ON;

CREATE SCHEMA citus_local_table_triggers;
SET search_path TO citus_local_table_triggers;

-- ensure that coordinator is added to pg_dist_node
SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

CREATE TABLE dummy_reference_table(a int unique, b int);
INSERT INTO dummy_reference_table SELECT i FROM generate_series(-1, 5) i;
INSERT INTO dummy_reference_table VALUES (99),(100),(599),(600);
SELECT create_reference_table('dummy_reference_table');

CREATE TABLE citus_local_table (value int);
ALTER TABLE citus_local_table ADD CONSTRAINT fkey_to_dummy_1 FOREIGN KEY (value) REFERENCES dummy_reference_table(a);

--------------------
-- DELETE trigger --
--------------------

BEGIN;
    CREATE TABLE distributed_table(value int);
    CREATE FUNCTION insert_42() RETURNS trigger AS $insert_42$
    BEGIN
        INSERT INTO distributed_table VALUES (42);
        RETURN NEW;
    END;
    $insert_42$ LANGUAGE plpgsql;

    SELECT create_distributed_table('distributed_table', 'value');

    CREATE TRIGGER insert_42_trigger
    AFTER DELETE ON citus_local_table
    FOR EACH ROW EXECUTE FUNCTION insert_42();

    -- select should print two rows with "42" as delete from citus_local_table will
    -- insert 42 per deleted row
    DELETE FROM citus_local_table;
    SELECT * FROM distributed_table;
ROLLBACK;

----------------------
-- TRUNCATE trigger --
----------------------

BEGIN;
    CREATE TABLE reference_table(value int);
    SELECT create_reference_table('reference_table');
    CREATE FUNCTION insert_100() RETURNS trigger AS $insert_100$
    BEGIN
        INSERT INTO reference_table VALUES (100);
        RETURN NEW;
    END;
    $insert_100$ LANGUAGE plpgsql;

    CREATE TRIGGER insert_100_trigger
    AFTER TRUNCATE ON citus_local_table
    FOR EACH STATEMENT EXECUTE FUNCTION insert_100();

    -- As TRUNCATE triggers are executed by utility hook, it's critical to see that they
    -- are executed only for once.
    -- select should print a row with "100" as truncate from citus_local_table will insert 100
    TRUNCATE citus_local_table;
    SELECT * FROM reference_table;
ROLLBACK;

--------------------
-- INSERT trigger --
--------------------

BEGIN;
    CREATE TABLE local_table(value int);
    CREATE FUNCTION increment_value() RETURNS trigger AS $increment_value$
    BEGIN
        UPDATE local_table SET value=value+1;
        RETURN NEW;
    END;
    $increment_value$ LANGUAGE plpgsql;

    CREATE TRIGGER increment_value_trigger
    AFTER INSERT ON citus_local_table
    FOR EACH ROW EXECUTE FUNCTION increment_value();

    -- insert initial data to the table that increment_value_trigger will execute for
    INSERT INTO local_table VALUES (0);

    -- select should print a row with "2" as insert into citus_local_table will
    -- increment all rows per inserted row
    INSERT INTO citus_local_table VALUES(0), (1);
    SELECT * FROM local_table;
ROLLBACK;

--------------------
-- UPDATE trigger --
--------------------

BEGIN;
    CREATE FUNCTION error_for_5() RETURNS trigger AS $error_for_5$
    BEGIN
        IF OLD.value = 5 THEN
            RAISE EXCEPTION 'cannot update update for value=5';
        END IF;
    END;
    $error_for_5$ LANGUAGE plpgsql;

    CREATE TRIGGER error_for_5_trigger
    BEFORE UPDATE OF value ON citus_local_table
    FOR EACH ROW EXECUTE FUNCTION error_for_5();

    -- below update will error out as trigger raises exception
    INSERT INTO citus_local_table VALUES (5);
    UPDATE citus_local_table SET value=value*2 WHERE value=5;
ROLLBACK;

------------------------------------------------------
-- Test other trigger commands + weird object names --
------------------------------------------------------

CREATE SCHEMA "interesting!schema";

-- below view is a helper to print triggers on both shell relation and
-- shard relation for "citus_local_table"
CREATE VIEW citus_local_table_triggers AS
    SELECT tgname, tgrelid::regclass, tgenabled
    FROM pg_trigger
    WHERE tgrelid::regclass::text like '"interesting!schema"."citus_local!_table%"'
    ORDER BY 1, 2;

CREATE FUNCTION dummy_function() RETURNS trigger AS $dummy_function$
BEGIN
    NEW.value := value+1;
    RETURN NEW;
END;
$dummy_function$ LANGUAGE plpgsql;

BEGIN;
    CREATE TABLE "interesting!schema"."citus_local!_table"(value int);

    CREATE TRIGGER initial_truncate_trigger
    AFTER TRUNCATE ON "interesting!schema"."citus_local!_table"
    FOR EACH STATEMENT EXECUTE FUNCTION dummy_function();

    ALTER TABLE "interesting!schema"."citus_local!_table" ADD CONSTRAINT fkey_to_dummy_2 FOREIGN KEY (value) REFERENCES dummy_reference_table(a);

    -- we shouldn't see truncate trigger on shard relation as we drop it
    SELECT * FROM citus_local_table_triggers
    WHERE tgname NOT LIKE 'RI_ConstraintTrigger%';
ROLLBACK;

CREATE TABLE "interesting!schema"."citus_local!_table"(value int);
ALTER TABLE "interesting!schema"."citus_local!_table" ADD CONSTRAINT fkey_to_dummy_2 FOREIGN KEY (value) REFERENCES dummy_reference_table(a);

CREATE TRIGGER "trigger\'name"
BEFORE INSERT ON "interesting!schema"."citus_local!_table"
FOR EACH STATEMENT EXECUTE FUNCTION dummy_function();

CREATE EXTENSION seg;
-- ALTER TRIGGER DEPENDS ON
ALTER TRIGGER "trigger\'name" ON "interesting!schema"."citus_local!_table" DEPENDS ON EXTENSION seg;

BEGIN;
    -- show that triggers on both shell relation and shard relation are not depending on seg
    SELECT tgname FROM pg_depend, pg_trigger, pg_extension
    WHERE deptype = 'x' and classid='pg_trigger'::regclass and
        pg_trigger.oid=pg_depend.objid and extname='seg'
    ORDER BY 1;

    DROP EXTENSION seg;

    -- show that dropping extension doesn't drop the triggers automatically
    SELECT * FROM citus_local_table_triggers
    WHERE tgname NOT LIKE 'RI_ConstraintTrigger%';
ROLLBACK;

-- ALTER TRIGGER RENAME
ALTER TRIGGER "trigger\'name" ON "interesting!schema"."citus_local!_table" RENAME TO "trigger\'name22";
-- show that triggers on both shell relation and shard relation are renamed
SELECT * FROM citus_local_table_triggers
    WHERE tgname NOT LIKE 'RI_ConstraintTrigger%';

-- ALTER TABLE ENABLE REPLICA trigger
ALTER TABLE "interesting!schema"."citus_local!_table" ENABLE REPLICA TRIGGER "trigger\'name22";
SELECT * FROM citus_local_table_triggers
    WHERE tgname NOT LIKE 'RI_ConstraintTrigger%';

-- ALTER TABLE ENABLE ALWAYS trigger
ALTER TABLE "interesting!schema"."citus_local!_table" ENABLE ALWAYS TRIGGER "trigger\'name22";
SELECT * FROM citus_local_table_triggers
    WHERE tgname NOT LIKE 'RI_ConstraintTrigger%';

-- ALTER TABLE DISABLE trigger
ALTER TABLE "interesting!schema"."citus_local!_table" DISABLE TRIGGER "trigger\'name22";
SELECT * FROM citus_local_table_triggers
    WHERE tgname NOT LIKE 'RI_ConstraintTrigger%';

-- ALTER TABLE ENABLE trigger
ALTER TABLE "interesting!schema"."citus_local!_table" ENABLE TRIGGER "trigger\'name22";
SELECT * FROM citus_local_table_triggers
    WHERE tgname NOT LIKE 'RI_ConstraintTrigger%';

CREATE TRIGGER another_trigger
AFTER DELETE ON "interesting!schema"."citus_local!_table"
FOR EACH STATEMENT EXECUTE FUNCTION dummy_function();

ALTER TABLE "interesting!schema"."citus_local!_table" DISABLE TRIGGER USER;
-- show that all triggers except the internal ones are disabled
SELECT * FROM citus_local_table_triggers
    WHERE tgname NOT LIKE 'RI_ConstraintTrigger%';

ALTER TABLE "interesting!schema"."citus_local!_table" ENABLE TRIGGER USER;
-- show that all triggers except the internal ones are enabled again
SELECT * FROM citus_local_table_triggers
    WHERE tgname NOT LIKE 'RI_ConstraintTrigger%';

ALTER TABLE "interesting!schema"."citus_local!_table" DISABLE TRIGGER ALL;
-- show that all triggers including internal triggers are disabled
SELECT * FROM citus_local_table_triggers
    WHERE tgname NOT LIKE 'RI_ConstraintTrigger%';

ALTER TABLE "interesting!schema"."citus_local!_table" ENABLE TRIGGER ALL;
-- show that all triggers including internal triggers are enabled again
SELECT * FROM citus_local_table_triggers
    WHERE tgname NOT LIKE 'RI_ConstraintTrigger%';

DROP TRIGGER another_trigger ON "interesting!schema"."citus_local!_table";
DROP TRIGGER "trigger\'name22" ON "interesting!schema"."citus_local!_table";
-- show that drop trigger works as expected
SELECT * FROM citus_local_table_triggers
    WHERE tgname NOT LIKE 'RI_ConstraintTrigger%';

BEGIN;
    CREATE TRIGGER "another_trigger\'name"
    AFTER TRUNCATE ON "interesting!schema"."citus_local!_table"
    FOR EACH STATEMENT EXECUTE FUNCTION dummy_function();

    ALTER TABLE "interesting!schema"."citus_local!_table" DISABLE TRIGGER "another_trigger\'name";
    -- show that our truncate trigger is disabled ..
    SELECT * FROM citus_local_table_triggers
    WHERE tgname NOT LIKE 'RI_ConstraintTrigger%';

    ALTER TABLE "interesting!schema"."citus_local!_table" ENABLE TRIGGER ALL;
    -- .. and now it is enabled back
    SELECT * FROM citus_local_table_triggers
    WHERE tgname NOT LIKE 'RI_ConstraintTrigger%';
ROLLBACK;

-- as we create ddl jobs for DROP TRIGGER before standard process utility,
-- it's important to see that we properly handle non-existing triggers
-- and relations
DROP TRIGGER no_such_trigger ON "interesting!schema"."citus_local!_table";
DROP TRIGGER no_such_trigger ON no_such_relation;

---------------------------------------
-- a complex test case with triggers --
---------------------------------------

-- create test tables and some foreign key relationships between them to see
-- that triggers are properly handled when ddl cascades to referencing table
CREATE TABLE another_citus_local_table (value int unique);
SELECT citus_add_local_table_to_metadata('another_citus_local_table');

ALTER TABLE another_citus_local_table ADD CONSTRAINT fkey_self FOREIGN KEY(value) REFERENCES another_citus_local_table(value);
ALTER TABLE citus_local_table ADD CONSTRAINT fkey_c_to_c FOREIGN KEY(value) REFERENCES another_citus_local_table(value) ON UPDATE CASCADE;

CREATE TABLE reference_table(value int);
SELECT create_reference_table('reference_table');

CREATE FUNCTION insert_100() RETURNS trigger AS $insert_100$
BEGIN
    INSERT INTO reference_table VALUES (100);
    RETURN NEW;
END;
$insert_100$ LANGUAGE plpgsql;

CREATE TABLE local_table (value int);

CREATE FUNCTION insert_100_local() RETURNS trigger AS $insert_100$
BEGIN
    INSERT INTO local_table VALUES (100);
    RETURN NEW;
END;
$insert_100$ LANGUAGE plpgsql;

BEGIN;
    CREATE TRIGGER insert_100_trigger
    AFTER TRUNCATE ON another_citus_local_table
    FOR EACH STATEMENT EXECUTE FUNCTION insert_100();

    CREATE TRIGGER insert_100_trigger
    AFTER TRUNCATE ON citus_local_table
    FOR EACH STATEMENT EXECUTE FUNCTION insert_100();

    TRUNCATE another_citus_local_table CASCADE;
    -- we should see two rows with "100"
    SELECT * FROM reference_table;
ROLLBACK;

-- can perform remote execution from a trigger on a Citus local table
BEGIN;
    -- update should actually update something to test ON UPDATE CASCADE logic
    INSERT INTO another_citus_local_table VALUES (600);
    INSERT INTO citus_local_table VALUES (600);

    CREATE TRIGGER insert_100_trigger
    AFTER UPDATE ON another_citus_local_table
    FOR EACH STATEMENT EXECUTE FUNCTION insert_100();

    CREATE TRIGGER insert_100_trigger
    AFTER UPDATE ON citus_local_table
    FOR EACH STATEMENT EXECUTE FUNCTION insert_100();

    UPDATE another_citus_local_table SET value=value-1;;
ROLLBACK;

-- can perform regular execution from a trigger on a Citus local table
BEGIN;
    -- update should actually update something to test ON UPDATE CASCADE logic
    INSERT INTO another_citus_local_table VALUES (600);
    INSERT INTO citus_local_table VALUES (600);

    CREATE TRIGGER insert_100_trigger
    AFTER UPDATE ON another_citus_local_table
    FOR EACH STATEMENT EXECUTE FUNCTION insert_100_local();

    CREATE TRIGGER insert_100_trigger
    AFTER UPDATE ON citus_local_table
    FOR EACH STATEMENT EXECUTE FUNCTION insert_100_local();

    UPDATE another_citus_local_table SET value=value-1;;
    -- we should see two rows with "100"
    SELECT * FROM local_table;
ROLLBACK;

-- can perform local execution from a trigger on a Citus local table
BEGIN;
	SELECT citus_add_local_table_to_metadata('local_table');

    -- update should actually update something to test ON UPDATE CASCADE logic
    INSERT INTO another_citus_local_table VALUES (600);
    INSERT INTO citus_local_table VALUES (600);

    CREATE TRIGGER insert_100_trigger
    AFTER UPDATE ON another_citus_local_table
    FOR EACH STATEMENT EXECUTE FUNCTION insert_100_local();

    CREATE TRIGGER insert_100_trigger
    AFTER UPDATE ON citus_local_table
    FOR EACH STATEMENT EXECUTE FUNCTION insert_100_local();

    UPDATE another_citus_local_table SET value=value-1;;
    -- we should see two rows with "100"
    SELECT * FROM local_table;
ROLLBACK;

-- test on partitioned citus local tables
CREATE TABLE par_citus_local_table (val int) PARTITION BY RANGE(val);
CREATE TABLE par_citus_local_table_1 PARTITION OF par_citus_local_table FOR VALUES FROM (1) TO (10000);
CREATE TABLE par_another_citus_local_table (val int unique) PARTITION BY RANGE(val);
CREATE TABLE par_another_citus_local_table_1 PARTITION OF par_another_citus_local_table FOR VALUES FROM (1) TO (10000);

ALTER TABLE par_another_citus_local_table ADD CONSTRAINT fkey_self FOREIGN KEY(val) REFERENCES par_another_citus_local_table(val);
ALTER TABLE par_citus_local_table ADD CONSTRAINT fkey_c_to_c FOREIGN KEY(val) REFERENCES par_another_citus_local_table(val) ON UPDATE CASCADE;

SELECT citus_add_local_table_to_metadata('par_another_citus_local_table', cascade_via_foreign_keys=>true);

CREATE TABLE par_reference_table(val int);
SELECT create_reference_table('par_reference_table');

CREATE FUNCTION par_insert_100() RETURNS trigger AS $par_insert_100$
BEGIN
    INSERT INTO par_reference_table VALUES (100);
    RETURN NEW;
END;
$par_insert_100$ LANGUAGE plpgsql;

BEGIN;
    CREATE TRIGGER par_insert_100_trigger
    AFTER TRUNCATE ON par_another_citus_local_table
    FOR EACH STATEMENT EXECUTE FUNCTION par_insert_100();

    CREATE TRIGGER insert_100_trigger
    AFTER TRUNCATE ON par_citus_local_table
    FOR EACH STATEMENT EXECUTE FUNCTION par_insert_100();

    TRUNCATE par_another_citus_local_table CASCADE;
    -- we should see two rows with "100"
    SELECT * FROM par_reference_table;
ROLLBACK;

-- cleanup at exit
SET client_min_messages TO ERROR;
DROP SCHEMA citus_local_table_triggers, "interesting!schema" CASCADE;
