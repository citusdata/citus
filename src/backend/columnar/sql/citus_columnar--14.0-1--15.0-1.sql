-- citus_columnar--14.0-1--15.0-1
-- bump version to 15.0-1

CREATE OR REPLACE VIEW columnar.storage WITH (security_barrier) AS
  SELECT c.oid::regclass AS relation,
         columnar.get_storage_id(c.oid) AS storage_id
    FROM pg_class c, pg_am am
    WHERE c.relam = am.oid AND am.amname = 'columnar'
      AND (c.relpersistence <> 't' OR c.relnamespace = pg_catalog.pg_my_temp_schema())
      AND pg_has_role(c.relowner, 'USAGE');
