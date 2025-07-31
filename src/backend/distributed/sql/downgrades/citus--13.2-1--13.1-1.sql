-- citus--13.2-1--13.1-1
-- downgrade version to 13.1-1

DROP FUNCTION IF EXISTS pg_catalog.worker_last_saved_explain_analyze();
#include "../udfs/worker_last_saved_explain_analyze/9.4-1.sql"
