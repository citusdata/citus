--
-- PG15+ test
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 14 AS server_version_above_fourteen
\gset
\if :server_version_above_fourteen
\else
\q
\endif

SET search_path TO "json table";

CREATE SCHEMA "json table";
SET search_path TO "json table";
CREATE TABLE jsonb_table_test (id bigserial, js jsonb);
SELECT create_distributed_table('jsonb_table_test', 'id');

-- insert some data
INSERT INTO jsonb_table_test (js)
VALUES (
	'[
		{"a":  1,  "b": [], "c": []},
		{"a":  2,  "b": [1, 2, 3], "c": [10, null, 20]},
		{"a":  3,  "b": [1, 2], "c": []},
		{"x": "4", "b": [1, 2], "c": 123}
	 ]'
),
(
	'[
		{"a":  1,  "d": [], "c": []},
		{"a":  2,  "d": [1, 2, 3], "c": [10, null, 20]},
		{"a":  3,  "d": [1, 2], "c": []},
		{"x": "4", "d": [1, 2], "c": 123}
	 ]'
),
(
	'[
		{"a":  1,  "b": [], "c": []},
		{"a":  2,  "b": [100, 200, 300], "c": [10, null, 20]},
		{"a":  3,  "b": [1, 2], "c": [null]},
		{"x": "4", "b": [1, 2], "c": 2}
	 ]'
),
(
	'[
		{"y":  1,  "b": [], "c": []},
		{"a":  2,  "b": [1, 2, 3], "c": [10, null, 20]},
		{"a":  3,  "t": [1, 2], "c": []},
		{"x": "4", "b": [1, 200], "c": 96}
	 ]'
),
(
	'[
		{"a":  1,  "b": [], "c": []},
		{"a":  2,  "b": [1, 2, 3], "c": [10, null, 20]},
		{"a":  3,  "b": [1, 2], "c": []},
		{"x": "100", "b": [1, 2], "c": 123}
	 ]'
),
(
	'[
		{"t":  1,  "b": [], "c": []},
		{"t":  2,  "b": [1, 2, 3], "x": [10, null, 20]},
		{"t":  3,  "b": [1, 2], "c": []},
		{"x": "4", "b": [1, 2], "c": 123}
	 ]'
),
(
	'[
		{"a":  1,  "b": [], "c": []},
		{"a":  2,  "b": [1, 2, 3], "c": [10, null, 20]},
		{"a":  3,  "b": [1, 2], "c": []},
		{"U": "4", "b": [1, 2], "c": 123}
	 ]'
),
(
	'[
		{"a":  1,  "b": [], "c": []},
		{"a":  2,  "b": [1, 2, 3], "c": [10, null, 20]},
		{"a":  3,  "b": [1, 2], "c": []},
		{"x": "4", "b": [1, 2], "c": 123}
	 ]'
),
(
	'[
		{"a":  1,  "b": [], "c": []},
		{"a":  2,  "b": [1, 2, 3], "c": [10, null, 20]},
		{"a":  3,  "b": [1, 2], "c": []},
		{"x": "4", "b": [1, 2], "c": 123}
	 ]'
),
(
	'[
		{"a":  1,  "b": [], "c": []},
		{"a":  2,  "b": [1000, 2, 3], "c": [10, null, 20]},
		{"a":  3,  "b": [1, 2], "c": []},
		{"x": "4", "T": [1, 2], "c": 123}
	 ]'
),
(
	'[
		{"a":  1,  "b": [], "c": []},
		{"a":  2,  "b": [1, 2, 3], "c": [10, null, 20]},
		{"a":  3,  "b": [1, 2], "c": []},
		{"x": "4", "b": [1, 2], "c": 123}
	 ]'
),
(
	'[
		{"ffa":  1,  "b": [], "c": []},
		{"ffb":  2,  "b": [1, 2, 3], "c": [10, null, 20]},
		{"fffc":  3,  "b": [1, 2], "c": []},
		{"x": "4", "b": [1, 2], "c": 123}
	 ]'
);

-- unspecified plan (outer, union)
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
	) jt ORDER BY 1,2,3,4;



-- default plan (outer, union)
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan default (outer, union)
	) jt ORDER BY 1,2,3,4;

-- specific plan (p outer (pb union pc))
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p outer (pb union pc))
	) jt ORDER BY 1,2,3,4;

-- specific plan (p outer (pc union pb))
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p outer (pc union pb))
	) jt ORDER BY 1,2,3,4;

-- default plan (inner, union)
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan default (inner)
	) jt ORDER BY 1,2,3,4;

-- specific plan (p inner (pb union pc))
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p inner (pb union pc))
	) jt ORDER BY 1,2,3,4;

-- default plan (inner, cross)
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan default (cross, inner)
	) jt ORDER BY 1,2,3,4;

-- specific plan (p inner (pb cross pc))
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p inner (pb cross pc))
	) jt ORDER BY 1,2,3,4;

-- default plan (outer, cross)
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan default (outer, cross)
	) jt ORDER BY 1,2,3,4;

-- specific plan (p outer (pb cross pc))
select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns ( b int path '$' ),
			nested path 'strict $.c[*]' as pc columns ( c int path '$' )
		)
		plan (p outer (pb cross pc))
	) jt ORDER BY 1,2,3,4;


select
	jt.*, b1 + 100 as b
from
	json_table (jsonb
		'[
			{"a":  1,  "b": [[1, 10], [2], [3, 30, 300]], "c": [1, null, 2]},
			{"a":  2,  "b": [10, 20], "c": [1, null, 2]},
			{"x": "3", "b": [11, 22, 33, 44]}
		 ]',
		'$[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on error,
			nested path 'strict $.b[*]' as pb columns (
				b text format json path '$',
				nested path 'strict $[*]' as pb1 columns (
					b1 int path '$'
				)
			),
			nested path 'strict $.c[*]' as pc columns (
				c text format json path '$',
				nested path 'strict $[*]' as pc1 columns (
					c1 int path '$'
				)
			)
		)
		--plan default(outer, cross)
		plan(p outer ((pb inner pb1) cross (pc outer pc1)))
	) jt ORDER BY 1,2,3,4,5;

-- Should succeed (JSON arguments are passed to root and nested paths)
SELECT *
FROM
	generate_series(1, 4) x,
	generate_series(1, 3) y,
	JSON_TABLE(jsonb
		'[[1,2,3],[2,3,4,5],[3,4,5,6]]',
		'strict $[*] ? (@[*] < $x)'
		PASSING x AS x, y AS y
		COLUMNS (
			y text FORMAT JSON PATH '$',
			NESTED PATH 'strict $[*] ? (@ >= $y)'
			COLUMNS (
				z int PATH '$'
			)
		)
	) jt ORDER BY 4,1,2,3;

SET client_min_messages TO ERROR;
DROP SCHEMA "json table" CASCADE;

