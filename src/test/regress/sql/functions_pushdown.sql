create table foo( id int primary key, t text);
select create_distributed_table('foo', 'id');

create table ref_t( id int primary key, f_id int, t text);
select create_reference_table('ref_t');

CREATE OR REPLACE FUNCTION ref_func(i_id bigint)
RETURNS bigint LANGUAGE sql AS
$function$
SELECT count(*) FROM ref_t where f_id = i_id;
$function$;

insert into foo values (1, 'a');
insert into foo values (2, 'b');
insert into foo values (3, 'c');
insert into ref_t values (1, 1, 'aa');
insert into ref_t values (2, 2, 'bb');
insert into ref_t values (3, 3, 'cc');
insert into ref_t values (4, 2, 'BB');

select *,ref_func(id) from foo;
select *,ref_func(id) from foo where id = 1;
select *,ref_func(id) from foo where id = 2;
select *,ref_func(id) from foo where id = 3;

select ref_func(1);
select ref_func(2);
select ref_func(3);

select * from ref_func(1);
select * from ref_func(2);
select * from ref_func(3);

drop table ref_t;
drop table foo;
drop function ref_func(bigint);

-- https://github.com/citusdata/citus/issues/5887
CREATE SCHEMA table_1;
CREATE TABLE table_1.test(a int);
SELECT create_distributed_table('table_1.test', 'a');
INSERT INTO table_1.test SELECT i FROM generate_series(1,10)i;

CREATE OR REPLACE FUNCTION some_func() RETURNS integer
    AS 'select count(*) FROM table_1.test;'
    LANGUAGE SQL
    VOLATILE
    RETURNS NULL ON NULL INPUT;

SELECT some_func() FROM table_1.test;

drop table table_1.test;
drop schema table_1;
drop function some_func();
