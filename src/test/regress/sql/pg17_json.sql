--
-- PG17_JSON
-- PG17 has added basic JSON_TABLE() functionality
-- JSON_TABLE() allows JSON data to be converted into a relational view
-- and thus used, for example, in a FROM clause, like other tabular
-- data. We treat JSON_TABLE the same as correlated functions (e.g., recurring tuples).
-- In the end, for multi-shard JSON_TABLE commands, we apply the same
-- restrictions as reference tables (e.g., cannot perform a lateral outer join
-- when a distributed subquery references a (reference table)/JSON_TABLE etc.)
-- Relevant PG commit:
-- https://github.com/postgres/postgres/commit/de3600452
--

SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 17 AS server_version_ge_17
\gset
\if :server_version_ge_17
\else
\q
\endif

CREATE SCHEMA pg17_json;
SET search_path TO pg17_json;

SET citus.next_shard_id TO 1687000;

CREATE TABLE test_table(id bigserial, value text);
SELECT create_distributed_table('test_table', 'id');
INSERT INTO test_table (value) SELECT i::text FROM generate_series(0,100)i;


CREATE TABLE my_films(id bigserial, js jsonb);
SELECT create_distributed_table('my_films', 'id');

INSERT INTO my_films(js) VALUES (
'{ "favorites" : [
   { "kind" : "comedy", "films" : [ { "title" : "Bananas", "director" : "Woody Allen"},
                                    { "title" : "The Dinner Game", "director" : "Francis Veber" } ] },
   { "kind" : "horror", "films" : [{ "title" : "Psycho", "director" : "Alfred Hitchcock" } ] },
   { "kind" : "thriller", "films" : [{ "title" : "Vertigo", "director" : "Alfred Hitchcock" } ] },
   { "kind" : "drama", "films" : [{ "title" : "Yojimbo", "director" : "Akira Kurosawa" } ] }
  ] }');

INSERT INTO my_films(js) VALUES (
'{ "favorites" : [
   { "kind" : "comedy", "films" : [ { "title" : "Bananas2", "director" : "Woody Allen"},
                                    { "title" : "The Dinner Game2", "director" : "Francis Veber" } ] },
   { "kind" : "horror", "films" : [{ "title" : "Psycho2", "director" : "Alfred Hitchcock" } ] },
   { "kind" : "thriller", "films" : [{ "title" : "Vertigo2", "director" : "Alfred Hitchcock" } ] },
   { "kind" : "drama", "films" : [{ "title" : "Yojimbo2", "director" : "Akira Kurosawa" } ] }
  ] }');

-- a router query
SELECT jt.* FROM
 my_films,
 JSON_TABLE ( js, '$.favorites[*]' COLUMNS (
   id FOR ORDINALITY,
   kind text PATH '$.kind',
   NESTED PATH '$.films[*]' COLUMNS (
     title text PATH '$.title',
     director text PATH '$.director'))) AS jt
      WHERE my_films.id = 1
    ORDER BY 1,2,3,4;

-- router query with an explicit LATEREL SUBQUERY
SELECT sub.*
FROM my_films,
     lateral(SELECT * FROM JSON_TABLE (js, '$.favorites[*]' COLUMNS (id FOR ORDINALITY,
                           kind text PATH '$.kind',
                           NESTED PATH '$.films[*]' COLUMNS (title text PATH '$.title', director text PATH '$.director'))) AS jt) as sub
WHERE my_films.id = 1;

-- router query with an explicit LATEREL SUBQUERY and LIMIT
SELECT sub.*
FROM my_films,
     lateral(SELECT * FROM JSON_TABLE (js, '$.favorites[*]' COLUMNS (id FOR ORDINALITY,
                           kind text PATH '$.kind',
                           NESTED PATH '$.films[*]' COLUMNS (title text PATH '$.title', director text PATH '$.director'))) AS jt ORDER BY id DESC LIMIT 1) as sub
WHERE my_films.id = 1;

-- set it DEBUG1 in case the plan changes
-- we can see details
SET client_min_messages TO DEBUG1;

-- a mult-shard query
SELECT jt.* FROM
 my_films,
 JSON_TABLE ( js, '$.favorites[*]' COLUMNS (
   id FOR ORDINALITY,
   kind text PATH '$.kind',
   NESTED PATH '$.films[*]' COLUMNS (
     title text PATH '$.title',
     director text PATH '$.director'))) AS jt
    ORDER BY 1,2,3,4;

-- recursively plan subqueries that has JSON_TABLE
SELECT count(*) FROM
(
 SELECT jt.* FROM
 my_films,
 JSON_TABLE ( js, '$.favorites[*]' COLUMNS (
   id FOR ORDINALITY,
   kind text PATH '$.kind',
   NESTED PATH '$.films[*]' COLUMNS (
     title text PATH '$.title',
     director text PATH '$.director'))) AS jt
    LIMIT 1) as sub_with_json, test_table
WHERE test_table.id = sub_with_json.id;


-- multi-shard query with an explicit LATEREL SUBQUERY
SELECT sub.*
FROM my_films JOIN
     lateral
  (SELECT *
   FROM JSON_TABLE (js, '$.favorites[*]' COLUMNS (id FOR ORDINALITY,
   												  kind text PATH '$.kind', NESTED PATH '$.films[*]'
                            COLUMNS (title text PATH '$.title', director text PATH '$.director'))) AS jt
   LIMIT 1000) AS sub ON (true)
    ORDER BY 1,2,3,4;

-- JSON_TABLE can be on the inner part of an outer joion
SELECT sub.*
FROM my_films LEFT JOIN
     lateral
  (SELECT *
   FROM JSON_TABLE (js, '$.favorites[*]' COLUMNS (id FOR ORDINALITY,
                            kind text PATH '$.kind', NESTED PATH '$.films[*]'
                            COLUMNS (title text PATH '$.title', director text PATH '$.director'))) AS jt
   LIMIT 1000) AS sub ON (true)
    ORDER BY 1,2,3,4;

-- we can pushdown this correlated subquery in WHERE clause
SELECT count(*)
FROM my_films WHERE
  (SELECT count(*) > 0
   FROM JSON_TABLE (js, '$.favorites[*]' COLUMNS (id FOR ORDINALITY,
                            kind text PATH '$.kind', NESTED PATH '$.films[*]'
                            COLUMNS (title text PATH '$.title', director text PATH '$.director'))) AS jt
   LIMIT 1000);

-- we can pushdown this correlated subquery in SELECT clause
 SELECT (SELECT count(*) > 0
   FROM JSON_TABLE (js, '$.favorites[*]' COLUMNS (id FOR ORDINALITY,
                            kind text PATH '$.kind', NESTED PATH '$.films[*]'
                            COLUMNS (title text PATH '$.title', director text PATH '$.director'))) AS jt)
FROM my_films;

-- multi-shard query with an explicit LATEREL SUBQUERY
-- along with other tables
SELECT sub.*
FROM my_films JOIN
     lateral
  (SELECT *
   FROM JSON_TABLE (js, '$.favorites[*]' COLUMNS (id FOR ORDINALITY,
                            kind text PATH '$.kind', NESTED PATH '$.films[*]'
                            COLUMNS (title text PATH '$.title', director text PATH '$.director'))) AS jt
   LIMIT 1000) AS sub ON (true) JOIN test_table ON(my_films.id = test_table.id)
    ORDER BY 1,2,3,4;

-- non-colocated join fails
SELECT sub.*
FROM my_films JOIN
     lateral
  (SELECT *
   FROM JSON_TABLE (js, '$.favorites[*]' COLUMNS (id FOR ORDINALITY,
                            kind text PATH '$.kind', NESTED PATH '$.films[*]'
                            COLUMNS (title text PATH '$.title', director text PATH '$.director'))) AS jt
   LIMIT 1000) AS sub ON (true) JOIN test_table ON(my_films.id != test_table.id)
    ORDER BY 1,2,3,4;

-- JSON_TABLE can be in the outer part of the join
-- as long as there is a distributed table
SELECT sub.*
FROM my_films JOIN
     lateral
  (SELECT *
   FROM JSON_TABLE (js, '$.favorites[*]' COLUMNS (id FOR ORDINALITY,
                            kind text PATH '$.kind', NESTED PATH '$.films[*]'
                            COLUMNS (title text PATH '$.title', director text PATH '$.director'))) AS jt
   LIMIT 1000) AS sub ON (true) LEFT JOIN test_table ON(my_films.id = test_table.id)
    ORDER BY 1,2,3,4;

-- JSON_TABLE can be on the outer side of the join
-- We support outer joins where the outer rel is a recurring one
-- and the inner one is a non-recurring one if we don't reference the outer from the inner
-- https://github.com/citusdata/citus/pull/6512

SELECT *
FROM json_table('[{"a":10,"b":20},{"a":30,"b":40}]'::JSONB, '$[*]'
               COLUMNS (id FOR  ORDINALITY, column_a int4 PATH '$.a', column_b int4 PATH '$.b', a int4, b int4, c text))
LEFT JOIN LATERAL
  (SELECT *
   FROM my_films) AS foo on(foo.id = a);

-- However we don't support
-- when we reference the JSON_TABLE from the non-recurring distributed table subquery
SELECT *
FROM json_table('[{"a":10,"b":20},{"a":30,"b":40}]'::JSONB, '$[*]'
               COLUMNS (json_id FOR  ORDINALITY, column_a int4 PATH '$.a', column_b int4 PATH '$.b', a int4, b int4, c text))
LEFT JOIN LATERAL
  (SELECT *
   FROM my_films WHERE id::text LIKE c) AS foo on(foo.id = a);

-- JSON_TABLE cannot be on the FROM clause alone
SELECT *
FROM json_table('[{"a":10,"b":20},{"a":30,"b":40}]'::JSONB, '$[*]'
               COLUMNS (json_id FOR  ORDINALITY, column_a int4 PATH '$.a', column_b int4 PATH '$.b', a int4, b int4, c text)) as foo
WHERE b >
  (SELECT count(*)
   FROM my_films WHERE id = foo.a);

-- we can recursively plan json_tables on set operations
(SELECT *
FROM json_table('[{"a":10,"b":20},{"a":30,"b":40}]'::JSONB, '$[*]'
               COLUMNS (id FOR  ORDINALITY))  ORDER BY id ASC LIMIT 1)
UNION
(SELECT *
FROM json_table('[{"a":10,"b":20},{"a":30,"b":40}]'::JSONB, '$[*]'
               COLUMNS (id FOR  ORDINALITY)) ORDER BY id ASC LIMIT 1)
UNION
(SELECT id FROM test_table ORDER BY id ASC LIMIT 1);

-- LIMIT in subquery not supported when json_table exists
SELECT *
FROM json_table('[{"a":10,"b":20},{"a":30,"b":40}]'::JSONB, '$[*]'
               COLUMNS (id FOR  ORDINALITY, column_a int4 PATH '$.a', column_b int4 PATH '$.b', a int4, b int4, c text))
JOIN LATERAL
  (SELECT *
   FROM my_films WHERE json_table.id = a LIMIT 1) as foo ON (true);

RESET client_min_messages;

-- we can use JSON_TABLE in modification queries as well

-- use log level such that we can see trace changes
SET client_min_messages TO DEBUG1;

--the JSON_TABLE subquery is recursively planned
UPDATE test_table SET VALUE = 'XXX' FROM(
SELECT jt.* FROM
 my_films,
 JSON_TABLE ( js, '$.favorites[*]' COLUMNS (
   id FOR ORDINALITY,
   kind text PATH '$.kind',
   NESTED PATH '$.films[*]' COLUMNS (
     title text PATH '$.title',
     director text PATH '$.director'))) AS jt) as foo  WHERE foo.id = test_table.id;

-- Subquery with JSON table can be pushed down because two distributed tables
-- in the query are joined on distribution column
UPDATE test_table SET VALUE = 'XXX' FROM (
SELECT my_films.id, jt.* FROM
 my_films,
 JSON_TABLE ( js, '$.favorites[*]' COLUMNS (
   kind text PATH '$.kind',
   NESTED PATH '$.films[*]' COLUMNS (
     title text PATH '$.title',
     director text PATH '$.director'))) AS jt) as foo  WHERE foo.id = test_table.id;

-- we can pushdown with CTEs as well
WITH json_cte AS
(SELECT my_films.id, jt.* FROM
 my_films,
 JSON_TABLE ( js, '$.favorites[*]' COLUMNS (
   kind text PATH '$.kind',
   NESTED PATH '$.films[*]' COLUMNS (
     title text PATH '$.title',
     director text PATH '$.director'))) AS jt)
UPDATE test_table SET VALUE = 'XYZ' FROM json_cte
 WHERE json_cte.id = test_table.id;

 -- we can recursively with CTEs as well
WITH json_cte AS
(SELECT my_films.id as film_id, jt.* FROM
 my_films,
 JSON_TABLE ( js, '$.favorites[*]' COLUMNS (
   kind text PATH '$.kind',
   NESTED PATH '$.films[*]' COLUMNS (
      id FOR ORDINALITY,
     title text PATH '$.title',
     director text PATH '$.director'))) AS jt ORDER BY jt.id LIMIT 1)
UPDATE test_table SET VALUE = 'XYZ' FROM json_cte
 WHERE json_cte.film_id = test_table.id;

-- JSON_TABLE NESTED
-- JSON_TABLE: plan execution
-- Check output with Postgres table in sqljson_jsontable test
-- https://github.com/postgres/postgres/blob/REL_17_0/src/test/regress/expected/sqljson_jsontable.out#L776-L814

CREATE TABLE jsonb_table_test (id bigserial, js jsonb);
SELECT create_distributed_table('jsonb_table_test', 'id');

INSERT INTO jsonb_table_test
VALUES (1,
	'[
		{"a":  1,  "b": [], "c": []},
		{"a":  2,  "b": [1, 2, 3], "c": [10, null, 20]},
		{"a":  3,  "b": [1, 2], "c": []},
		{"x": "4", "b": [1, 2], "c": 123}
	 ]'
);

select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns (b_id for ordinality, b int path '$' ),
			nested path 'strict $.c[*]' as pc columns (c_id for ordinality, c int path '$' )
		)
	) jt;

-- test some utility functions on the target list & where clause: json_exists()
select jsonb_path_exists(js, '$.favorites') from my_films;
select bool_and(JSON_EXISTS(js, '$.favorites.films.title')) from my_films;
SELECT count(*) FROM my_films WHERE jsonb_path_exists(js, '$.favorites');
SELECT count(*) FROM my_films WHERE JSON_EXISTS(js, '$.favorites.films.title');

-- check constraint with json_exists, use json_scalar also
SET citus.shard_replication_factor TO 1;
create table user_profiles (
    id bigserial,
    addresses jsonb,
    anyjson  jsonb,
    serialized bytea,
    check (json_exists( addresses, '$.main' )) -- we should insert a key named main
);
select create_distributed_table('user_profiles', 'id');

insert into user_profiles (addresses) VALUES (JSON_SCALAR('1'));
insert into user_profiles (addresses, anyjson) VALUES ('{"main":"value"}', JSON_SCALAR('1')) RETURNING *;

-- use json() - we cannot insert because WITH UNIQUE KEYS
insert into user_profiles (addresses) VALUES (JSON ('{"main":"value", "main":"value"}' WITH UNIQUE KEYS));

-- we can insert with
insert into user_profiles (addresses) VALUES (JSON ('{"main":"value", "main":"value"}' WITHOUT UNIQUE KEYS)) RETURNING *;

-- JSON predicates
TRUNCATE user_profiles;
INSERT INTO user_profiles (anyjson) VALUES ('12'), ('"abc"'), ('[1,2,3]'), ('{"a":12}');
select anyjson, anyjson is json array as json_array, anyjson is json object as json_object, anyjson is json scalar as json_scalar,
anyjson is json with UNIQUE keys
from user_profiles WHERE anyjson IS NOT NULL ORDER BY 1;

-- use json_serialize
-- it is evaluated in the worker
SELECT JSON_SERIALIZE('{ "a" : 1 } ' RETURNING bytea);
SET citus.log_remote_commands TO on;
INSERT INTO user_profiles (serialized) VALUES (JSON_SERIALIZE('{ "a" : 1 } ' RETURNING bytea)) RETURNING *;
RESET citus.log_remote_commands;

-- use json_query
SELECT i,
       json_query('[{"x": "aaa"},{"x": "bbb"},{"x": "ccc"}]'::JSONB, '$[$i].x' passing id AS i RETURNING text omit quotes)
FROM generate_series(0, 3) i
JOIN my_films ON(id = i) ORDER BY 1;

-- use json_value
-- check output with sqljson_queryfuncs test
-- https://github.com/postgres/postgres/blob/REL_17_0/src/test/regress/expected/sqljson_queryfuncs.out#L439-L455
SELECT i,
       JSON_VALUE(
		jsonb '{"a": 1, "b": 2}',
		'$.* ? (@ > $i)' PASSING id AS i
		RETURNING int
		DEFAULT -1 ON EMPTY
		DEFAULT -2 ON ERROR
	)
FROM generate_series(0, 3) i
JOIN my_films ON(id = i) ORDER BY 1;

SET client_min_messages TO ERROR;
DROP SCHEMA pg17_json CASCADE;
