--
-- PG15
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 14 AS server_version_above_fourteen
\gset
\if :server_version_above_fourteen
\else
\q
\endif

CREATE SCHEMA pg15;
SET search_path TO pg15;
SET citus.next_shard_id TO 960000;
SET citus.shard_count TO 4;

--
-- In PG15, there is an added option to use ICU as global locale provider.
-- pg_collation has three locale-related fields: collcollate and collctype,
-- which are libc-related fields, and a new one colliculocale, which is the
-- ICU-related field. Only the libc-related fields or the ICU-related field
-- is set, never both.
-- Relevant PG commits:
-- f2553d43060edb210b36c63187d52a632448e1d2
-- 54637508f87bd5f07fb9406bac6b08240283be3b
--

-- fail, needs "locale"
CREATE COLLATION german_phonebook_test (provider = icu, lc_collate = 'de-u-co-phonebk');

-- fail, needs "locale"
CREATE COLLATION german_phonebook_test (provider = icu, lc_collate = 'de-u-co-phonebk', lc_ctype = 'de-u-co-phonebk');

-- works
CREATE COLLATION german_phonebook_test (provider = icu, locale = 'de-u-co-phonebk');

-- with icu provider, colliculocale will be set, collcollate and collctype will be null
SELECT result FROM run_command_on_all_nodes('
    SELECT collcollate FROM pg_collation WHERE collname = ''german_phonebook_test'';
');
SELECT result FROM run_command_on_all_nodes('
    SELECT collctype FROM pg_collation WHERE collname = ''german_phonebook_test'';
');
SELECT result FROM run_command_on_all_nodes('
    SELECT colliculocale FROM pg_collation WHERE collname = ''german_phonebook_test'';
');

-- with non-icu provider, colliculocale will be null, collcollate and collctype will be set
CREATE COLLATION default_provider (provider = libc, lc_collate = "POSIX", lc_ctype = "POSIX");

SELECT result FROM run_command_on_all_nodes('
    SELECT collcollate FROM pg_collation WHERE collname = ''default_provider'';
');
SELECT result FROM run_command_on_all_nodes('
    SELECT collctype FROM pg_collation WHERE collname = ''default_provider'';
');
SELECT result FROM run_command_on_all_nodes('
    SELECT colliculocale FROM pg_collation WHERE collname = ''default_provider'';
');

--
-- In PG15, Renaming triggers on partitioned tables had two problems
-- recurses to renaming the triggers on the partitions as well.
-- Here we test that distributed triggers behave the same way.
-- Relevant PG commit:
-- 80ba4bb383538a2ee846fece6a7b8da9518b6866
--

SET citus.enable_unsafe_triggers TO true;

CREATE TABLE sale(
    sale_date date not null,
    state_code text,
    product_sku text,
    units integer)
    PARTITION BY list (state_code);

ALTER TABLE sale ADD CONSTRAINT sale_pk PRIMARY KEY (state_code, sale_date);

CREATE TABLE sale_newyork PARTITION OF sale FOR VALUES IN ('NY');
CREATE TABLE sale_california PARTITION OF sale FOR VALUES IN ('CA');

CREATE TABLE record_sale(
    operation_type text not null,
    product_sku text,
    state_code text,
    units integer,
    PRIMARY KEY(state_code, product_sku, operation_type, units));

SELECT create_distributed_table('sale', 'state_code');
SELECT create_distributed_table('record_sale', 'state_code', colocate_with := 'sale');

CREATE OR REPLACE FUNCTION record_sale()
RETURNS trigger
AS $$
BEGIN
    INSERT INTO pg15.record_sale(operation_type, product_sku, state_code, units)
    VALUES (TG_OP, NEW.product_sku, NEW.state_code, NEW.units);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER record_sale_trigger
AFTER INSERT OR UPDATE OR DELETE ON sale
FOR EACH ROW EXECUTE FUNCTION pg15.record_sale();

CREATE VIEW sale_triggers AS
    SELECT tgname, tgrelid::regclass, tgenabled
    FROM pg_trigger
    WHERE tgrelid::regclass::text like 'sale%'
    ORDER BY 1, 2;

SELECT * FROM sale_triggers ORDER BY 1, 2;
ALTER TRIGGER "record_sale_trigger" ON "pg15"."sale" RENAME TO "new_record_sale_trigger";
SELECT * FROM sale_triggers ORDER BY 1, 2;

-- Clean up
DROP SCHEMA pg15 CASCADE;
