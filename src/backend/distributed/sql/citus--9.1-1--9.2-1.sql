#include "udfs/read_intermediate_results/9.2-1.sql"
#include "udfs/fetch_intermediate_results/9.2-1.sql"
#include "udfs/worker_partition_query_result/9.2-1.sql"

ALTER TABLE pg_catalog.pg_dist_colocation ADD distributioncolumncollation oid;
UPDATE pg_catalog.pg_dist_colocation dc SET distributioncolumncollation = t.typcollation
	FROM pg_catalog.pg_type t WHERE t.oid = dc.distributioncolumntype;
UPDATE pg_catalog.pg_dist_colocation dc SET distributioncolumncollation = 0 WHERE distributioncolumncollation IS NULL;
ALTER TABLE pg_catalog.pg_dist_colocation ALTER COLUMN distributioncolumncollation SET NOT NULL;

DROP INDEX pg_dist_colocation_configuration_index;
-- distributioncolumntype should be listed first so that this index can be used for looking up reference tables' colocation id
CREATE INDEX pg_dist_colocation_configuration_index
ON pg_dist_colocation USING btree(distributioncolumntype, shardcount, replicationfactor, distributioncolumncollation);

CREATE TABLE citus.pg_dist_rebalance_strategy(
    name name NOT NULL,
    default_strategy boolean NOT NULL DEFAULT false,
    shard_cost_function regproc NOT NULL,
    node_capacity_function regproc NOT NULL,
    shard_allowed_on_node_function regproc NOT NULL,
    default_threshold float4 NOT NULL,
    minimum_threshold float4 NOT NULL DEFAULT 0,
    UNIQUE(name)
);
ALTER TABLE citus.pg_dist_rebalance_strategy SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_rebalance_strategy TO public;

#include "udfs/citus_validate_rebalance_strategy_functions/9.2-1.sql"
#include "udfs/pg_dist_rebalance_strategy_trigger_func/9.2-1.sql"
CREATE TRIGGER pg_dist_rebalance_strategy_validation_trigger
  BEFORE INSERT OR UPDATE ON pg_dist_rebalance_strategy
  FOR EACH ROW EXECUTE PROCEDURE citus_internal.pg_dist_rebalance_strategy_trigger_func();

#include "udfs/citus_add_rebalance_strategy/9.2-1.sql"
#include "udfs/citus_set_default_rebalance_strategy/9.2-1.sql"

#include "udfs/citus_shard_cost_1/9.2-1.sql"
#include "udfs/citus_shard_cost_by_disk_size/9.2-1.sql"
#include "udfs/citus_node_capacity_1/9.2-1.sql"
#include "udfs/citus_shard_allowed_on_node_true/9.2-1.sql"

INSERT INTO
    pg_catalog.pg_dist_rebalance_strategy(
        name,
        default_strategy,
        shard_cost_function,
        node_capacity_function,
        shard_allowed_on_node_function,
        default_threshold,
        minimum_threshold
    ) VALUES (
        'by_shard_count',
        true,
        'citus_shard_cost_1',
        'citus_node_capacity_1',
        'citus_shard_allowed_on_node_true',
        0,
        0
    ), (
        'by_disk_size',
        false,
        'citus_shard_cost_by_disk_size',
        'citus_node_capacity_1',
        'citus_shard_allowed_on_node_true',
        0.1,
        0.01
    );


CREATE FUNCTION citus_internal.pg_dist_rebalance_strategy_enterprise_check()
  RETURNS TRIGGER
  LANGUAGE C
  AS 'MODULE_PATHNAME';
CREATE TRIGGER pg_dist_rebalance_strategy_enterprise_check_trigger
  BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON pg_dist_rebalance_strategy
  FOR EACH STATEMENT EXECUTE FUNCTION citus_internal.pg_dist_rebalance_strategy_enterprise_check();


#include "udfs/master_drain_node/9.2-1.sql"
#include "udfs/rebalance_table_shards/9.2-1.sql"
#include "udfs/get_rebalance_table_shards_plan/9.2-1.sql"

#include "udfs/citus_prepare_pg_upgrade/9.2-1.sql"
#include "udfs/citus_finish_pg_upgrade/9.2-1.sql"

-- changing the return type of the function requires we drop the function
DROP FUNCTION citus_extradata_container(INTERNAL);
CREATE OR REPLACE FUNCTION citus_extradata_container(INTERNAL)
    RETURNS SETOF record
    LANGUAGE C
AS 'MODULE_PATHNAME', $$citus_extradata_container$$;
COMMENT ON FUNCTION pg_catalog.citus_extradata_container(INTERNAL)
    IS 'placeholder function to store additional data in postgres node trees';

