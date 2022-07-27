--
-- PG15
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 15 AS server_version_ge_15
\gset
\if :server_version_ge_15
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

-- Clean up
DROP SCHEMA pg15 CASCADE;
