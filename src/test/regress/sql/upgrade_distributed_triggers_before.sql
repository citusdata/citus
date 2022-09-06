--
-- UPGRADE_DISTRIBUTED_TRIGGERS_BEFORE
--
-- PRE PG15, Renaming the parent triggers on partitioned tables doesn't
-- recurse to renaming the child triggers on the partitions as well.
--
-- this test is relevant only for pg14-15 upgrade
--

SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int = 14 AS server_version_eq_14
\gset
\if :server_version_eq_14
\else
\q
\endif

CREATE SCHEMA upgrade_distributed_triggers;
SET search_path TO upgrade_distributed_triggers, public;
SET citus.shard_count TO 4;

SET citus.enable_unsafe_triggers TO true;
SELECT run_command_on_workers('ALTER SYSTEM SET citus.enable_unsafe_triggers TO true;');
SELECT run_command_on_workers('SELECT pg_reload_conf();');

CREATE TABLE sale(
    sale_date date not null,
    state_code text,
    product_sku text,
    units integer)
    PARTITION BY list (state_code);

ALTER TABLE sale ADD CONSTRAINT sale_pk PRIMARY KEY (state_code, sale_date);

CREATE TABLE sale_newyork PARTITION OF sale FOR VALUES IN ('NY');

CREATE TABLE record_sale(
    operation_type text not null,
    product_sku text,
    state_code text,
    units integer,
    PRIMARY KEY(state_code, product_sku, operation_type, units));

SELECT create_distributed_table('sale', 'state_code');
CREATE TABLE sale_california PARTITION OF sale FOR VALUES IN ('CA');
SELECT create_distributed_table('record_sale', 'state_code', colocate_with := 'sale');

CREATE OR REPLACE FUNCTION record_sale()
RETURNS trigger
AS $$
BEGIN
    INSERT INTO upgrade_distributed_triggers.record_sale(operation_type, product_sku, state_code, units)
    VALUES (TG_OP, NEW.product_sku, NEW.state_code, NEW.units);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- will rename this trigger
CREATE TRIGGER record_sale_trigger
AFTER INSERT OR UPDATE OR DELETE ON sale
FOR EACH ROW EXECUTE FUNCTION upgrade_distributed_triggers.record_sale();

-- will rename this trigger
CREATE TRIGGER another_trigger
AFTER INSERT OR UPDATE OR DELETE ON sale
FOR EACH ROW EXECUTE FUNCTION upgrade_distributed_triggers.record_sale();

-- won't rename this trigger
CREATE TRIGGER not_renamed_trigger
AFTER INSERT OR UPDATE OR DELETE ON sale
FOR EACH ROW EXECUTE FUNCTION upgrade_distributed_triggers.record_sale();

-- Trigger function should appear on workers
SELECT proname from pg_proc WHERE oid='upgrade_distributed_triggers.record_sale'::regproc;
SELECT run_command_on_workers($$SELECT count(*) FROM pg_proc WHERE oid='upgrade_distributed_triggers.record_sale'::regproc$$);

-- Trigger should appear on workers
SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'record_sale_trigger%' ORDER BY 1,2;
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'record_sale_trigger%';$$);

CREATE VIEW sale_triggers AS
    SELECT tgname, tgrelid::regclass, tgenabled
    FROM pg_trigger
    WHERE tgrelid::regclass::text like 'sale%'
    ORDER BY 1, 2;

SELECT * FROM sale_triggers ORDER BY 1, 2;

-- rename the triggers - note that it doesn't rename the
-- triggers on the partitions
ALTER TRIGGER record_sale_trigger ON sale RENAME TO renamed_record_sale_trigger;

SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'record_sale_trigger%' ORDER BY 1,2;
SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'renamed_record_sale_trigger%' ORDER BY 1,2;
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'record_sale_trigger%';$$);
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'renamed_record_sale_trigger%';$$);

ALTER TRIGGER another_trigger ON sale RENAME TO another_renamed_trigger;
SELECT * FROM sale_triggers ORDER BY 1, 2;

-- although the child triggers haven't been renamed to
-- another_renamed_trigger, they are dropped when the parent is dropped
DROP TRIGGER another_renamed_trigger ON sale;
SELECT * FROM sale_triggers ORDER BY 1, 2;

SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'another_renamed_trigger%' ORDER BY 1,2;
SELECT tgrelid::regclass::text, tgname FROM pg_trigger WHERE tgname like 'another_trigger%' ORDER BY 1,2;
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'another_renamed_trigger%';$$);
SELECT run_command_on_workers($$SELECT count(*) FROM pg_trigger WHERE tgname like 'another_trigger%';$$);

CREATE TRIGGER another_trigger
AFTER INSERT OR UPDATE OR DELETE ON sale
FOR EACH ROW EXECUTE FUNCTION upgrade_distributed_triggers.record_sale();

ALTER TRIGGER another_trigger ON sale RENAME TO another_renamed_trigger;
SELECT * FROM sale_triggers ORDER BY 1, 2;

-- check that we can't rename child triggers on partitions of distributed tables
ALTER TRIGGER another_trigger ON sale_newyork RENAME TO another_renamed_trigger;
