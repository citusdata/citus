-- citus--13.2-1--13.1-1
-- downgrade version to 13.1-1

DROP FUNCTION IF EXISTS pg_catalog.worker_last_saved_explain_analyze();
#include "../udfs/worker_last_saved_explain_analyze/9.4-1.sql"

-- Note that we intentionally don't add the old columnar objects back to the "citus"
-- extension in this downgrade script, even if they were present in the older version.
--
-- If the user wants to create "citus_columnar" extension later, "citus_columnar"
-- will anyway properly create them at the scope of that extension.
