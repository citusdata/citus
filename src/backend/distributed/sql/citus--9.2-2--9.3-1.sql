/* citus--9.2-2--9.3-1 */

/* bump version to 9.3-1 */

#include "udfs/citus_extradata_container/9.3-1.sql"
#include "udfs/master_add_inactive_node/9.3-1.sql"

ALTER TABLE pg_catalog.pg_dist_node ADD activateonrebalance bool NOT NULL DEFAULT false;
