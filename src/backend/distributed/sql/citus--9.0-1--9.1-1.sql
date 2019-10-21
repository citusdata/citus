ALTER TABLE pg_catalog.pg_dist_node ADD shouldhaveshards bool NOT NULL DEFAULT true;
COMMENT ON COLUMN pg_catalog.pg_dist_node.shouldhaveshards IS
    'indicates whether the node is eligible to contain data from distributed tables';

#include "udfs/master_set_node_property/9.1-1.sql"
#include "udfs/master_drain_node/9.1-1.sql"
