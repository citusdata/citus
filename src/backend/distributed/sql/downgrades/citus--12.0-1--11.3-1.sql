-- citus--12.0-1--11.3-1

DO $$
BEGIN
    -- Throw an error if user has created any tenant schemas.
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_dist_tenant_schema)
    THEN
        RAISE EXCEPTION 'cannot downgrade Citus because there are '
                        'tenant schemas created.'
        USING HINT = 'To downgrade Citus to an older version, you should '
                     'first issue SELECT citus.schema_tenant_unset("%s") '
                     'for each tenant schema.';
    END IF;

    -- Throw an error if user has any distributed tables without a shard key.
    IF EXISTS (
        SELECT 1 FROM pg_dist_partition
        WHERE repmodel != 't' AND partmethod = 'n' AND colocationid != 0)
    THEN
        RAISE EXCEPTION 'cannot downgrade Citus because there are '
                        'distributed tables without a shard key.'
        USING HINT = 'You can find the distributed tables without a shard '
                     'key in the cluster by using the following query: '
                     '"SELECT * FROM citus_tables WHERE distribution_column '
                     '= ''<none>'' AND colocation_id > 0".',
        DETAIL = 'To downgrade Citus to an older version, you should '
                 'first convert those tables to Postgres tables by '
                 'executing SELECT undistribute_table("%s").';
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS pg_catalog.citus_schema_distribute(regnamespace);
DROP FUNCTION IF EXISTS pg_catalog.citus_schema_undistribute(regnamespace);

DROP FUNCTION pg_catalog.citus_internal_add_tenant_schema(Oid, int);

#include "../udfs/citus_prepare_pg_upgrade/11.2-1.sql"
#include "../udfs/citus_finish_pg_upgrade/11.2-1.sql"

DROP FUNCTION pg_catalog.citus_internal_delete_tenant_schema(Oid);
DROP FUNCTION pg_catalog.citus_internal_unregister_tenant_schema_globally(Oid, text);

#include "../udfs/citus_drop_trigger/10.2-1.sql"

#include "../udfs/citus_tables/11.1-1.sql"
#include "../udfs/citus_shards/11.1-1.sql"

DROP TABLE pg_catalog.pg_dist_tenant_schema;
