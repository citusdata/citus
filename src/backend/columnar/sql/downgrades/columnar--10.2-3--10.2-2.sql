-- columnar--10.2-3--10.2-2.sql
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
ALTER TABLE columnar.stripe DROP CONSTRAINT stripe_first_row_number_idx;
CREATE INDEX stripe_first_row_number_idx ON columnar.stripe USING BTREE(storage_id, first_row_number);
