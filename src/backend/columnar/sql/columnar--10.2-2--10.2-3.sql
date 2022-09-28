-- columnar--10.2-2--10.2-3.sql

-- Since stripe_first_row_number_idx is required to scan a columnar table, we
-- need to make sure that it is created before doing anything with columnar
-- tables during pg upgrades.
--
-- However, a plain btree index is not a dependency of a table, so pg_upgrade
-- cannot guarantee that stripe_first_row_number_idx gets created when
-- creating columnar.stripe, unless we make it a unique "constraint".
--
-- To do that, drop stripe_first_row_number_idx and create a unique
-- constraint with the same name to keep the code change at minimum.
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
ALTER TABLE columnar.stripe ADD CONSTRAINT stripe_first_row_number_idx
UNIQUE (storage_id, first_row_number);
