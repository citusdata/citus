/* citus--9.2-4--9.3-2 */

/* bump version to 9.3-2 */

#include "udfs/citus_extradata_container/9.3-2.sql"
#include "udfs/update_distributed_table_colocation/9.3-2.sql"
#include "udfs/replicate_reference_tables/9.3-2.sql"
#include "udfs/citus_remote_connection_stats/9.3-2.sql"
#include "udfs/worker_create_or_alter_role/9.3-2.sql"
#include "udfs/truncate_local_data_after_distributing_table/9.3-2.sql"
-- add citus extension owner as a distributed object, if not already in there
INSERT INTO citus.pg_dist_object SELECT
  (SELECT oid FROM pg_class WHERE relname = 'pg_authid') AS oid,
  (SELECT oid FROM pg_authid WHERE rolname = current_user) as objid,
  0 as objsubid
ON CONFLICT DO NOTHING;

--
-- Add prefer_logical to citus.shard_transfer_mode
--
DROP FUNCTION IF EXISTS pg_catalog.master_copy_shard_placement(bigint,text,integer,text,integer,boolean,citus.shard_transfer_mode);
DROP FUNCTION IF EXISTS pg_catalog.master_move_shard_placement(bigint,text,integer,text,integer,citus.shard_transfer_mode);
DROP FUNCTION IF EXISTS pg_catalog.replicate_table_shards(regclass,int,int,bigint[], citus.shard_transfer_mode);
DROP FUNCTION IF EXISTS pg_catalog.master_drain_node(text,int,citus.shard_transfer_mode,name);
DROP FUNCTION IF EXISTS pg_catalog.rebalance_table_shards(regclass,float4,int,bigint[],citus.shard_transfer_mode,boolean,name);

DROP TYPE citus.shard_transfer_mode;
CREATE TYPE citus.shard_transfer_mode AS ENUM (
   'auto',
   'force_logical',
   'block_writes',
   'prefer_logical'
);

#include "udfs/master_copy_shard_placement/9.3-2.sql"
#include "udfs/master_move_shard_placement/9.3-2.sql"
#include "udfs/replicate_table_shards/9.3-2.sql"
#include "udfs/master_drain_node/9.3-2.sql"
#include "udfs/rebalance_table_shards/9.3-2.sql"
