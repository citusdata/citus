--
-- UPGRADE_DISTRIBUTED_TRIGGERS_AFTER
--
-- In PG15, Renaming triggers on partitioned tables
-- recurses to renaming the triggers on the partitions as well.
-- Relevant PG commit:
-- 80ba4bb383538a2ee846fece6a7b8da9518b6866
--
-- this test is relevant only for pg14-15 upgrade
--

SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int = 15 AND EXISTS (SELECT * FROM pg_namespace WHERE nspname = 'upgrade_distributed_triggers') AS is_14_15_pg_upgrade
\gset
\if :is_14_15_pg_upgrade
\else
\q
\endif

SET search_path TO upgrade_distributed_triggers, public;
SET citus.shard_count TO 4;

SET citus.enable_unsafe_triggers TO true;
SELECT run_command_on_workers('ALTER SYSTEM SET citus.enable_unsafe_triggers TO true;');
SELECT run_command_on_workers('SELECT pg_reload_conf();');

-- after PG15 upgrade, all child triggers have the same name with the parent triggers
-- check that the workers are also updated
SELECT * FROM sale_triggers ORDER BY 1, 2;

SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'record_sale_trigger%' ORDER BY 1,2;
SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'renamed_record_sale_trigger%' ORDER BY 1,2;
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'record_sale_trigger%';$$);
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'renamed_record_sale_trigger%';$$);

-- create another partition to verify that all is safe and sound
CREATE TABLE sale_alabama PARTITION OF sale FOR VALUES IN ('AL');

SELECT * FROM sale_triggers ORDER BY 1, 2;

SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'record_sale_trigger%' ORDER BY 1,2;
SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'renamed_record_sale_trigger%' ORDER BY 1,2;
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'record_sale_trigger%';$$);
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'renamed_record_sale_trigger%';$$);

-- drop a trigger to verify that all is safe and sound
DROP TRIGGER another_renamed_trigger ON sale;

SELECT * FROM sale_triggers ORDER BY 1, 2;

-- rename a trigger - note that it also renames the triggers on the partitions
ALTER TRIGGER "renamed_record_sale_trigger" ON "sale" RENAME TO "final_renamed_record_sale_trigger";

SELECT * FROM sale_triggers ORDER BY 1, 2;

SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'renamed_record_sale_trigger%' ORDER BY 1,2;
SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'final_renamed_record_sale_trigger%' ORDER BY 1,2;
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'renamed_record_sale_trigger%';$$);
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'final_renamed_record_sale_trigger%';$$);

DROP TRIGGER final_renamed_record_sale_trigger ON sale;

-- create another trigger and rename it
CREATE TRIGGER yet_another_trigger
AFTER INSERT OR UPDATE OR DELETE ON sale
FOR EACH ROW EXECUTE FUNCTION upgrade_distributed_triggers.record_sale();

ALTER TRIGGER "yet_another_trigger" ON "sale" RENAME TO "renamed_yet_another_trigger";

SELECT * FROM sale_triggers ORDER BY 1, 2;

-- after upgrade to PG15, test that we can't rename a distributed clone trigger
ALTER TRIGGER "renamed_yet_another_trigger" ON "sale_alabama" RENAME TO "another_trigger_name";
SELECT count(*) FROM pg_trigger WHERE tgname like 'another_trigger_name%';
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'another_trigger_name%';$$);

DROP SCHEMA upgrade_distributed_triggers CASCADE;
