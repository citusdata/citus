-- columnar--10.2-1--10.1-1.sql

-- downgrade storage for all columnar relations
SELECT citus_internal.downgrade_columnar_storage(c.oid) FROM pg_class c, pg_am a
  WHERE c.relam = a.oid AND amname = 'columnar';

DROP FUNCTION citus_internal.upgrade_columnar_storage(regclass);
DROP FUNCTION citus_internal.downgrade_columnar_storage(regclass);

-- drop "first_row_number" column and the index defined on it
--
-- If we have a pg_depend entry for this index, we can not drop it as
-- the extension depends on it. Remove the pg_depend entry if it exists.
DELETE FROM pg_depend
WHERE classid = 'pg_am'::regclass::oid
    AND objid IN (select oid from pg_am where amname = 'columnar')
    AND objsubid = 0
    AND refclassid = 'pg_class'::regclass::oid
    AND refobjid = 'columnar.stripe_first_row_number_idx'::regclass::oid
    AND refobjsubid = 0
    AND deptype = 'n';
DROP INDEX columnar.stripe_first_row_number_idx;
ALTER TABLE columnar.stripe DROP COLUMN first_row_number;
