-- columnar--10.2-4--10.2-3.sql

DROP FUNCTION citus_internal.columnar_ensure_am_depends_catalog();

-- Note that we intentionally do not delete pg_depend records that we inserted
-- via columnar--10.2-3--10.2-4.sql (by using columnar_ensure_am_depends_catalog).
