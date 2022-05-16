-- citus--10.2-1--10.2-2

-- bump version to 10.2-2

--#include "../../columnar/sql/columnar--10.2-1--10.2-2.sql"
DO $check_columnar$
BEGIN
  IF NOT EXISTS (select 1 from pg_extension where extname='citus_columnar') THEN  
      #include "../../columnar/sql/columnar--10.2-1--10.2-2.sql"
  END IF;
END;
$check_columnar$;
