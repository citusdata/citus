--
-- PG16
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 16 AS server_version_ge_16
\gset
\if :server_version_ge_16
\else
\q
\endif

-- create/drop database for pg >= 16

set citus.enable_create_database_propagation=on;

-- test icu_rules
--
-- practically we don't support it but better to test

CREATE DATABASE citus_icu_rules_test WITH icu_rules='de_DE@collation=phonebook';
CREATE DATABASE citus_icu_rules_test WITH icu_rules='de_DE@collation=phonebook' locale_provider='icu';
CREATE DATABASE citus_icu_rules_test WITH icu_rules='de_DE@collation=phonebook' locale_provider='icu' icu_locale = 'de_DE';
