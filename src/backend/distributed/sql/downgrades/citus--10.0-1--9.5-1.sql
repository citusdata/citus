-- citus--10.0-1--9.5-1
-- this is an empty downgrade path since citus--9.5-1--10.0-1.sql is empty for now

#include "../udfs/citus_finish_pg_upgrade/9.5-1.sql"

#include "../../../columnar/sql/downgrades/columnar--10.0-1--9.5-1.sql"

DROP VIEW public.citus_tables;
DROP FUNCTION pg_catalog.citus_total_relation_size(regclass,boolean);
DROP FUNCTION pg_catalog.undistribute_table(regclass,boolean);
DROP FUNCTION pg_catalog.create_citus_local_table(regclass,boolean);
DROP FUNCTION pg_catalog.citus_add_node(text, integer, integer, noderole, name);
DROP FUNCTION pg_catalog.citus_activate_node(text, integer);
DROP FUNCTION pg_catalog.citus_add_inactive_node(text, integer, integer, noderole, name);

DROP VIEW pg_catalog.time_partitions;
DROP FUNCTION pg_catalog.time_partition_range(regclass);

DROP FUNCTION pg_catalog.citus_set_coordinator_host(text,int,noderole,name);

#include "../udfs/citus_total_relation_size/7.0-1.sql"
#include "../udfs/upgrade_to_reference_table/8.0-1.sql"
#include "../udfs/undistribute_table/9.5-1.sql"
#include "../udfs/create_citus_local_table/9.5-1.sql"
