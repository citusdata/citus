-- https://github.com/citusdata/citus/issues/7676
CREATE TABLE test_ref_multiexpr (
    id bigint primary key
  , col_int integer
  , col_bool bool
  , col_text text
  , col_timestamp timestamp
  );
select create_reference_table('test_ref_multiexpr');

/* TODO how to ensure in test that 'now()' is correctly pre-executed */
insert into test_ref_multiexpr values (1, 1, true, 'one', now());

update test_ref_multiexpr
SET (col_timestamp)
  = (SELECT now())
returning id, col_int, col_bool;

update test_ref_multiexpr
SET (col_bool, col_timestamp)
  = (SELECT true, now())
returning id, col_int, col_bool;

DROP TABLE test_ref_multiexpr;
