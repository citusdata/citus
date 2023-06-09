SET citus.enable_schema_based_sharding TO ON;

-- Create tenant tables with schema names that need escaping
-- to verify that citus_prepare_pg_upgrade() correctly saves
-- them into public schema.

-- empty tenant
CREATE SCHEMA "tenant\'_1";

-- non-empty tenant
CREATE SCHEMA "tenant\'_2";
CREATE TABLE "tenant\'_2".test_table(a int, b text);

RESET citus.enable_schema_based_sharding;
