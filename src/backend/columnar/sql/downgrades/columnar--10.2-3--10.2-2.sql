-- columnar--10.2-3--10.2-2.sql

ALTER TABLE columnar.stripe DROP CONSTRAINT stripe_first_row_number_idx;
CREATE INDEX stripe_first_row_number_idx ON columnar.stripe USING BTREE(storage_id, first_row_number);
