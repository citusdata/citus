SET citus.shard_count TO 2;
SET citus.next_shard_id TO 750000;
SET citus.next_placement_id TO 750000;

CREATE SCHEMA indirections;

-- First on reference table
-- test advanced UPDATE SET () with indirection and physical reordering.
CREATE TABLE test_ref_indirection (
    id bigint primary key
  , col_int integer
  , col_bool bool
  , col_text text
  );
select create_reference_table('test_ref_indirection');

insert into test_ref_indirection values (1, 1, true, 'three');

-- default physical ordering
update test_ref_indirection
SET (col_int, col_bool, col_text)
  = (SELECT 2, false, 'thirty')
returning *;
select * from test_ref_indirection;

-- check indirection
update test_ref_indirection
SET (col_bool, col_text, col_int)
  = (SELECT true, 'thirty-three', 11)
returning *;

select * from test_ref_indirection;
update test_ref_indirection
SET (col_bool, col_int)
  = (SELECT false, 111)
returning *;
select * from test_ref_indirection;

-- check more complex queries with indirection
insert into test_ref_indirection values (2, 0, false, 'empty');
update test_ref_indirection
SET (col_int, col_bool, col_text)
  = (SELECT 22, true, 'full')
where id = 2
returning *;
select * from test_ref_indirection where id = 2;

update test_ref_indirection
SET (col_bool, col_text, col_int)
  = (SELECT false, 'really full', 222)
where id = 2
returning *;
select * from test_ref_indirection where id = 2;

update test_ref_indirection
SET (col_bool, col_int)
  = (SELECT true, 2222)
where id = 2
returning *;
select * from test_ref_indirection where id = 2;

-- several updates
insert into test_ref_indirection values (3, 0, false, 'empty');
insert into test_ref_indirection values (4, 0, false, 'empty');
with qq3 as (
    update test_ref_indirection
    SET (col_text, col_bool)
      = (SELECT 'full', true)
    where id = 3
    returning *
),
qq4 as (
    update test_ref_indirection
    SET (col_text, col_bool)
      = (SELECT 'fully', true)
    where id = 4
    returning *
)
select * from qq3 union all select * from qq4;
select * from test_ref_indirection where id in (3, 4);

-- add more advanced queries ?
-- we want to ensure the reordering the targetlist
-- from indirection is not run when it should not
truncate test_ref_indirection;

-- change physical ordering
alter table test_ref_indirection drop col_int;
alter table test_ref_indirection add col_int integer;
insert into test_ref_indirection values (3, true, 'three', 1);
insert into test_ref_indirection values (4, true, 'four', 1);
update test_ref_indirection
SET (col_int, col_bool, col_text)
  = (SELECT 2, false, 'thirty')
returning *;
select * from test_ref_indirection;



-- then on distributed table
-- First on reference table
-- test advanced UPDATE SET () with indirection and physical reordering.
CREATE TABLE test_dist_indirection (
    id bigint primary key
  , col_int integer
  , col_bool bool
  , col_text text
  );
SELECT create_distributed_table('test_dist_indirection', 'id');

insert into test_dist_indirection values (1, 1, true, 'three');

-- default physical ordering
update test_dist_indirection
SET (col_int, col_bool, col_text)
  = (SELECT 2, false, 'thirty')
returning *;
select * from test_dist_indirection;

-- check indirection
update test_dist_indirection
SET (col_bool, col_text, col_int)
  = (SELECT true, 'thirty-three', 11)
returning *;

select * from test_dist_indirection;
update test_dist_indirection
SET (col_bool, col_int)
  = (SELECT false, 111)
returning *;
select * from test_dist_indirection;

-- check more complex queries with indirection
insert into test_dist_indirection values (2, 0, false, 'empty');
update test_dist_indirection
SET (col_int, col_bool, col_text)
  = (SELECT 22, true, 'full')
where id = 2
returning *;
select * from test_dist_indirection where id = 2;

update test_dist_indirection
SET (col_bool, col_text, col_int)
  = (SELECT false, 'really full', 222)
where id = 2
returning *;
select * from test_dist_indirection where id = 2;

update test_dist_indirection
SET (col_bool, col_int)
  = (SELECT true, 2222)
where id = 2
returning *;
select * from test_dist_indirection where id = 2;

-- several updates
insert into test_dist_indirection values (3, 0, false, 'empty');
insert into test_dist_indirection values (4, 0, false, 'empty');
with qq3 as (
    update test_dist_indirection
    SET (col_text, col_bool)
      = (SELECT 'full', true)
    where id = 3
    returning *
),
qq4 as (
    update test_dist_indirection
    SET (col_text, col_bool)
      = (SELECT 'fully', true)
    where id = 4
    returning *
)
select * from qq3 union all select * from qq4;
select * from test_dist_indirection where id in (3, 4);

-- add more advanced queries ?
-- we want to ensure the reordering the targetlist
-- from indirection is not run when it should not
truncate test_dist_indirection;

-- change physical ordering
alter table test_dist_indirection drop col_int;
alter table test_dist_indirection add col_int integer;
insert into test_dist_indirection values (3, true, 'three', 1);
insert into test_dist_indirection values (4, true, 'four', 1);
update test_dist_indirection
SET (col_int, col_bool, col_text)
  = (SELECT 2, false, 'thirty')
returning *;
select * from test_dist_indirection;



DROP TABLE test_dist_indirection;
DROP TABLE test_ref_indirection;
DROP SCHEMA indirections;
