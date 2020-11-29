-- citus--10.0-1--9.5-1
-- this is an empty downgrade path since citus--9.5-1--10.0-1.sql is empty for now

#include "../udfs/citus_finish_pg_upgrade/9.5-1.sql"

#include "../../../columnar/sql/downgrades/columnar--10.0-1--9.5-1.sql"

DROP FUNCTION pg_catalog.read_distributed_intermediate_result(text);
DROP FUNCTION pg_catalog.create_distributed_intermediate_result(text, text, int, text);
