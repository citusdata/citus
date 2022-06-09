-- citus--10.2-2--10.2-3

-- bump version to 10.2-3

--#include "../../columnar/sql/columnar--10.2-2--10.2-3.sql"
DO $check_columnar$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_extension AS e
             INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
             INNER JOIN pg_catalog.pg_proc AS p ON (p.oid = d.objid)
             WHERE e.extname='citus_columnar' and p.proname = 'columnar_handler'
  ) THEN
      #include "../../columnar/sql/columnar--10.2-2--10.2-3.sql"
  END IF;
END;
$check_columnar$;
