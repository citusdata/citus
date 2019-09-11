/* citus--8.0-8--8.0-9 */
SET search_path = 'pg_catalog';

#include "udfs/master_activate_node/8.0-9.sql"
#include "udfs/master_add_inactive_node/8.0-9.sql"
#include "udfs/master_add_node/8.0-9.sql"
#include "udfs/master_add_secondary_node/8.0-9.sql"
REVOKE ALL ON FUNCTION master_disable_node(text,int) FROM PUBLIC;
REVOKE ALL ON FUNCTION master_initialize_node_metadata() FROM PUBLIC;
REVOKE ALL ON FUNCTION master_remove_node(text,int) FROM PUBLIC;
REVOKE ALL ON FUNCTION master_update_node(int,text,int) FROM PUBLIC;

RESET search_path;
