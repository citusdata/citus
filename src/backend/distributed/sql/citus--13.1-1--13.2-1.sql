-- citus--13.1-1--13.2-1

-- bump version to 13.2-1
#include "udfs/worker_last_saved_explain_analyze/13.2-1.sql"

#include "cat_upgrades/add_clone_info_to_pg_dist_node.sql"
#include "udfs/citus_add_clone_node/13.2-1.sql"
#include "udfs/citus_remove_clone_node/13.2-1.sql"
#include "udfs/citus_promote_clone_and_rebalance/13.2-1.sql"
#include "udfs/get_snapshot_based_node_split_plan/13.2-1.sql"

#include "udfs/citus_rebalance_start/13.2-1.sql"
#include "udfs/citus_internal_copy_single_shard_placement/13.2-1.sql"

#include "udfs/citus_finish_pg_upgrade/13.2-1.sql"

#include "udfs/citus_stats/13.2-1.sql"

DO $drop_leftover_old_columnar_objects$
BEGIN
  -- If old columnar exists, i.e., the columnar access method that we had before Citus 11.1,
  -- and we don't have any relations using the old columnar, then we want to drop the columnar
  -- objects. This is because, we don't want to automatically create the "citus_columnar"
  -- extension together with the "citus" extension anymore. And for the cases where we don't
  -- want to automatically create the "citus_columnar" extension, there is no point of keeping
  -- the columnar objects that we had before Citus 11.1 around.
  IF (
    SELECT EXISTS (
      SELECT 1 FROM pg_am
      WHERE
        -- looking for an access method whose name is "columnar" ..
        pg_am.amname = 'columnar' AND
        -- .. and there should *NOT* be such a dependency edge in pg_depend, where ..
        NOT EXISTS (
          SELECT 1 FROM pg_depend
          WHERE
            -- .. the depender is columnar access method (2601 = access method class) ..
            pg_depend.classid = 2601 AND pg_depend.objid = pg_am.oid AND pg_depend.objsubid = 0 AND
            -- .. and the dependee is an extension (3079 = extension class)
            pg_depend.refclassid = 3079 AND pg_depend.refobjsubid = 0
          LIMIT 1
        ) AND
        -- .. and there should *NOT* be any relations using it
        NOT EXISTS (
          SELECT 1
          FROM pg_class
          WHERE pg_class.relam = pg_am.oid
          LIMIT 1
        )
    )
  )
  THEN
    -- Below we drop the columnar objects in such an order that the objects that depend on
    -- other objects are dropped first.

    DROP VIEW IF EXISTS columnar.options;
    DROP VIEW IF EXISTS columnar.stripe;
    DROP VIEW IF EXISTS columnar.chunk_group;
    DROP VIEW IF EXISTS columnar.chunk;
    DROP VIEW IF EXISTS columnar.storage;

    DROP ACCESS METHOD IF EXISTS columnar;

    DROP SEQUENCE IF EXISTS columnar_internal.storageid_seq;

    DROP TABLE IF EXISTS columnar_internal.options;
    DROP TABLE IF EXISTS columnar_internal.stripe;
    DROP TABLE IF EXISTS columnar_internal.chunk_group;
    DROP TABLE IF EXISTS columnar_internal.chunk;

    DROP FUNCTION IF EXISTS columnar_internal.columnar_handler;

    DROP FUNCTION IF EXISTS pg_catalog.alter_columnar_table_set;
    DROP FUNCTION IF EXISTS pg_catalog.alter_columnar_table_reset;
    DROP FUNCTION IF EXISTS columnar.get_storage_id;

    DROP FUNCTION IF EXISTS citus_internal.upgrade_columnar_storage;
    DROP FUNCTION IF EXISTS citus_internal.downgrade_columnar_storage;
    DROP FUNCTION IF EXISTS citus_internal.columnar_ensure_am_depends_catalog;

    DROP SCHEMA IF EXISTS columnar;
    DROP SCHEMA IF EXISTS columnar_internal;
  END IF;
END $drop_leftover_old_columnar_objects$;
