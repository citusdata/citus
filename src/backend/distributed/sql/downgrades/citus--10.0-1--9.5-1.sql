-- citus--10.0-1--9.5-1
-- this is an empty downgrade path since citus--9.5-1--10.0-1.sql is empty for now

#include "../udfs/citus_finish_pg_upgrade/9.5-1.sql"

#include "../../../columnar/sql/downgrades/columnar--10.0-1--9.5-1.sql"

DROP VIEW public.citus_tables;
DROP FUNCTION pg_catalog.citus_total_relation_size(regclass,boolean);

#include "../udfs/citus_total_relation_size/7.0-1.sql"

DROP VIEW pg_catalog.citus_nodes;

DROP FUNCTION pg_catalog.citus_local_disk_space_stats();
DROP FUNCTION pg_catalog.citus_node_disk_space_stats(text,int);
DROP FUNCTION pg_catalog.citus_database_size(name);
DROP FUNCTION pg_catalog.citus_node_database_size(text,int,name);
