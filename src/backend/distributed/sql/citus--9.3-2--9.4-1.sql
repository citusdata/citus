-- citus--9.3-2--9.4-1

-- bump version to 9.4-1
#include "udfs/worker_last_saved_explain_analyze/9.4-1.sql"
#include "udfs/worker_save_query_explain_analyze/9.4-1.sql"


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

#include "udfs/master_copy_shard_placement/9.4-1.sql"
#include "udfs/master_move_shard_placement/9.4-1.sql"
#include "udfs/replicate_table_shards/9.4-1.sql"
#include "udfs/master_drain_node/9.4-1.sql"
#include "udfs/rebalance_table_shards/9.4-1.sql"
