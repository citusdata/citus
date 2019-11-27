--
-- MULTI_NAME_RESOLUTION
--

-- There was a failure compiling queries with shadowed subquery aliases
-- https://github.com/citusdata/citus/issues/2642

CREATE SCHEMA multi_name_resolution;
SET search_path TO multi_name_resolution;

create table namenest1 (id integer primary key, user_id integer);
create table namenest2 (id integer primary key, value_2 integer);
select * from create_distributed_table('namenest1', 'id');
select * from create_reference_table('namenest2');

SELECT r
FROM (
	SELECT id_deep, random() as r -- prevent pulling up the subquery
	FROM (
		namenest1
		JOIN namenest2 ON (namenest1.user_id = namenest2.value_2)
	) AS join_alias(id_deep)
) AS bar, (
	namenest1
	JOIN namenest2 ON (namenest1.user_id = namenest2.value_2)
) AS join_alias(id_deep)
WHERE bar.id_deep = join_alias.id_deep;

DROP SCHEMA multi_name_resolution CASCADE;
