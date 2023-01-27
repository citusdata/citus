SET search_path TO local_dist_join_mixed;

-- very simple 1-1 Joins
SELECT count(*) FROM distributed JOIN local USING (id);
SELECT count(*) FROM distributed JOIN local ON (name = title);
SELECT count(*) FROM distributed d1 JOIN local ON (name = d1.id::text);
SELECT count(*) FROM distributed d1 JOIN local ON (name = d1.id::text AND d1.id < local.title::int);
SELECT count(*) FROM distributed d1 JOIN local ON (name = d1.id::text AND d1.id < local.title::int) WHERE d1.id = 1;
SELECT count(*) FROM distributed JOIN local USING (id) WHERE false;
SELECT count(*) FROM distributed d1 JOIN local ON (name = d1.id::text AND d1.id < local.title::int) WHERE d1.id = 1 OR True;
SELECT count(*) FROM distributed d1 JOIN local ON (name::int + local.id > d1.id AND d1.id < local.title::int) WHERE d1.id = 1;
SELECT count(*) FROM distributed JOIN local ON (hashtext(name) = hashtext(title));
SELECT hashtext(local.id::text) FROM distributed JOIN local ON (hashtext(name) = hashtext(title)) ORDER BY 1 LIMIT 4;
SELECT '' as "xxx", local.*, 'xxx' as "test" FROM distributed JOIN local ON (hashtext(name) = hashtext(title)) ORDER BY 1,2,3 LIMIT 4;
SELECT local.title, count(*) FROM distributed JOIN local USING (id) GROUP BY 1 ORDER BY 1, 2 DESC LIMIT 5;
SELECT distributed.id as id1, local.id  as id2 FROM distributed JOIN local USING(id) ORDER BY distributed.id + local.id LIMIT 5;
SELECT distributed.id as id1, local.id  as id2, count(*) FROM distributed JOIN local USING(id) GROUP BY distributed.id, local.id ORDER BY 1,2 LIMIT 5;


-- basic subqueries that cannot be pulled up
SELECT count(*) FROM (SELECT *, random() FROM distributed) as d1 JOIN local USING (id);
SELECT count(*) FROM (SELECT *, random() FROM distributed) as d1  JOIN local ON (name = title);
SELECT count(*) FROM (SELECT *, random() FROM distributed) as d1  JOIN local ON (name = d1.id::text);
SELECT count(*) FROM (SELECT *, random() FROM distributed) as d1  JOIN local ON (name = d1.id::text AND d1.id < local.title::int);
SELECT count(*) FROM (SELECT *, random() FROM distributed) as d1  JOIN local ON (name = d1.id::text AND d1.id < local.title::int) WHERE d1.id = 1;
SELECT count(*) FROM (SELECT *, random() FROM distributed) as d1  JOIN local ON (name = d1.id::text AND d1.id < local.title::int) WHERE d1.id = 1 AND false;
SELECT count(*) FROM (SELECT *, random() FROM distributed) as d1  JOIN local ON (name = d1.id::text AND d1.id < local.title::int) WHERE d1.id = 1 OR true;

-- pull up subqueries as they are pretty simple, local table should be recursively planned
SELECT count(*) FROM (SELECT * FROM distributed) as d1 JOIN local USING (id);
SELECT count(*) FROM (SELECT * FROM distributed) as d1  JOIN local ON (name = title);
SELECT count(*) FROM (SELECT * FROM distributed) as d1  JOIN local ON (name = d1.id::text);
SELECT count(*) FROM (SELECT * FROM distributed) as d1  JOIN local ON (name = d1.id::text AND d1.id < local.title::int);
SELECT count(*) FROM (SELECT * FROM distributed) as d1  JOIN local ON (name = d1.id::text AND d1.id < local.title::int) WHERE d1.id = 1;
SELECT count(*) FROM (SELECT * FROM distributed) as d1  JOIN local ON (name = d1.id::text AND d1.id < local.title::int) WHERE d1.id = 1 AND false;
SELECT count(*) FROM (SELECT * FROM distributed) as d1  JOIN local ON (name = d1.id::text AND d1.id < local.title::int) WHERE d1.id = 1 OR true;
SELECT count(*) FROM (SELECT * FROM distributed WHERE id = 2) as d1  JOIN local ON (name = d1.id::text AND d1.id < local.title::int) WHERE d1.id = 1;
SELECT count(*) FROM (SELECT * FROM distributed WHERE false) as d1  JOIN local ON (name = d1.id::text AND d1.id < local.title::int) WHERE d1.id = 1;

-- final queries are pushdown queries
SELECT sum(d1.id + local.id) FROM distributed d1 JOIN local USING (id);
SELECT sum(d1.id + local.id) OVER (PARTITION BY d1.id) FROM distributed d1 JOIN local USING (id) ORDER BY 1 DESC LIMIT 4;
SELECT count(*) FROM distributed d1 JOIN local USING (id) LEFT JOIN distributed d2 USING (id) ORDER BY 1 DESC LIMIT 4;
SELECT count(DISTINCT d1.name::int * local.id) FROM distributed d1 JOIN local USING (id);

-- final queries are router queries
SELECT sum(d1.id + local.id) FROM distributed d1 JOIN local USING (id) WHERE d1.id = 1;
SELECT sum(d1.id + local.id) OVER (PARTITION BY d1.id) FROM distributed d1 JOIN local USING (id) WHERE d1.id = 1 ORDER BY 1 DESC LIMIT 4;
SELECT count(*) FROM distributed d1 JOIN local USING (id) LEFT JOIN distributed d2 USING (id) WHERE d2.id = 1 ORDER BY 1 DESC LIMIT 4;

-- final queries are pull to coordinator queries
SELECT sum(d1.id + local.id) OVER (PARTITION BY d1.id + local.id) FROM distributed d1 JOIN local USING (id) ORDER BY 1 DESC LIMIT 4;



-- nested subqueries
SELECT
	count(*)
FROM
	(SELECT * FROM (SELECT * FROM distributed) as foo) as bar
		JOIN
	local
		USING(id);


SELECT
	count(*)
FROM
	(SELECT *, random() FROM (SELECT *, random() FROM distributed) as foo) as bar
		JOIN
	local
		USING(id);

SELECT
	count(*)
FROM
	(SELECT *, random() FROM (SELECT *, random() FROM distributed) as foo) as bar
		JOIN
	local
		USING(id);
SELECT
	count(*)
FROM
	(SELECT *, random() FROM (SELECT *, random() FROM distributed) as foo) as bar
		JOIN
	(SELECT *, random() FROM (SELECT *,random() FROM local) as foo2) as bar2
		USING(id);

-- TODO: Unnecessary recursive planning for local
SELECT
	count(*)
FROM
	(SELECT *, random() FROM (SELECT *, random() FROM distributed LIMIT 1) as foo) as bar
		JOIN
	(SELECT *, random() FROM (SELECT *,random() FROM local) as foo2) as bar2
		USING(id);

-- subqueries in WHERE clause
-- is not colocated, and the JOIN inside as well.
-- so should be recursively planned twice
SELECT
	count(*)
FROM
	distributed
WHERE
	id  > (SELECT
				count(*)
			FROM
				(SELECT *, random() FROM (SELECT *, random() FROM distributed) as foo) as bar
					JOIN
				(SELECT *, random() FROM (SELECT *,random() FROM local) as foo2) as bar2
					USING(id)
			);

-- two distributed tables are co-located and JOINed on distribution
-- key, so should be fine to pushdown
SELECT
	count(*)
FROM
	distributed d_upper
WHERE
	(SELECT
				bar.id
			FROM
				(SELECT *, random() FROM (SELECT *, random() FROM distributed WHERE distributed.id = d_upper.id) as foo) as bar
					JOIN
				(SELECT *, random() FROM (SELECT *,random() FROM local) as foo2) as bar2
					USING(id)
			) IS NOT NULL;

SELECT
	count(*)
FROM
	distributed d_upper
WHERE
	(SELECT
				bar.id
			FROM
				(SELECT *, random() FROM (SELECT *, random() FROM distributed WHERE distributed.id = d_upper.id) as foo) as bar
					JOIN
				  local as foo
					USING(id)
			) IS NOT NULL;

SELECT
	count(*)
FROM
	distributed d_upper
WHERE d_upper.id >
	(SELECT
				bar.id
			FROM
				(SELECT *, random() FROM (SELECT *, random() FROM distributed WHERE distributed.id = d_upper.id) as foo) as bar
					JOIN
				  local as foo
					USING(id)
			);



-- subqueries in the target list

-- router, should work
select (SELECT local.id) FROM local, distributed WHERE distributed.id = 1 LIMIT 1;

-- set operations

SELECT local.* FROM distributed JOIN local USING (id)
	EXCEPT
SELECT local.* FROM distributed JOIN local USING (id);

SELECT distributed.* FROM distributed JOIN local USING (id)
	EXCEPT
SELECT distributed.* FROM distributed JOIN local USING (id);


SELECT count(*) FROM
(
	(SELECT * FROM (SELECT * FROM local) as f JOIN distributed USING (id))
		UNION ALL
	(SELECT * FROM (SELECT * FROM local) as f2 JOIN distributed USING (id))
) bar;

SELECT count(*) FROM
(
	(SELECT * FROM (SELECT distributed.* FROM local JOIN distributed USING (id)) as fo)
		UNION ALL
	(SELECT * FROM (SELECT distributed.* FROM local JOIN distributed USING (id)) as ba)
) bar;

select count(DISTINCT id)
FROM
(
	(SELECT * FROM (SELECT distributed.* FROM local JOIN distributed USING (id)) as fo)
		UNION ALL
	(SELECT * FROM (SELECT distributed.* FROM local JOIN distributed USING (id)) as ba)
) bar;

-- lateral joins

SELECT COUNT(*) FROM local JOIN LATERAL (SELECT * FROM distributed WHERE local.id = distributed.id) as foo ON (true);
SELECT COUNT(*) FROM local JOIN LATERAL (SELECT * FROM distributed WHERE local.id > distributed.id) as foo ON (true);



SELECT count(*) FROM distributed CROSS JOIN local;
SELECT count(*) FROM distributed CROSS JOIN local WHERE distributed.id = 1;

-- w count(*) it works fine as PG ignores the  inner tables
SELECT count(*) FROM distributed LEFT JOIN local USING (id);
SELECT count(*) FROM local LEFT JOIN distributed USING (id);

SELECT id, name FROM distributed LEFT JOIN local USING (id) ORDER BY 1 LIMIT 1;

 SELECT
        foo1.id
    FROM
 (SELECT local.id, local.title FROM local, distributed WHERE local.id = distributed.id ) as foo9,
 (SELECT local.id, local.title FROM local, distributed WHERE local.id = distributed.id ) as foo8,
 (SELECT local.id, local.title FROM local, distributed WHERE local.id = distributed.id ) as foo7,
 (SELECT local.id, local.title FROM local, distributed WHERE local.id = distributed.id ) as foo6,
 (SELECT local.id, local.title FROM local, distributed WHERE local.id = distributed.id ) as foo5,
 (SELECT local.id, local.title FROM local, distributed WHERE local.id = distributed.id ) as foo4,
 (SELECT local.id, local.title FROM local, distributed WHERE local.id = distributed.id ) as foo3,
 (SELECT local.id, local.title FROM local, distributed WHERE local.id = distributed.id ) as foo2,
 (SELECT local.id, local.title FROM local, distributed WHERE local.id = distributed.id ) as foo10,
 (SELECT local.id, local.title FROM local, distributed WHERE local.id = distributed.id ) as foo1
 WHERE
  foo1.id =  foo9.id AND
  foo1.id =  foo8.id AND
  foo1.id =  foo7.id AND
  foo1.id =  foo6.id AND
  foo1.id =  foo5.id AND
  foo1.id =  foo4.id AND
  foo1.id =  foo3.id AND
  foo1.id =  foo2.id AND
  foo1.id =  foo10.id AND
  foo1.id =  foo1.id
ORDER BY 1;

SELECT
	foo1.id
FROM
	(SELECT local.id FROM distributed, local WHERE local.id = distributed.id ) as foo1,
	(SELECT local.id FROM distributed, local WHERE local.id = distributed.id ) as foo2,
	(SELECT local.id FROM distributed, local WHERE local.id = distributed.id ) as foo3,
	(SELECT local.id FROM distributed, local WHERE local.id = distributed.id ) as foo4,
	(SELECT local.id FROM distributed, local WHERE local.id = distributed.id ) as foo5
WHERE
	foo1.id = foo4.id AND
	foo1.id = foo2.id AND
	foo1.id = foo3.id AND
	foo1.id = foo4.id AND
	foo1.id = foo5.id
ORDER BY 1;

SELECT
	foo1.id
FROM
	(SELECT local.id FROM distributed, local WHERE local.id = distributed.id  AND distributed.id = 1) as foo1,
	(SELECT local.id FROM distributed, local WHERE local.id = distributed.id  AND distributed.id = 2) as foo2,
	(SELECT local.id FROM distributed, local WHERE local.id = distributed.id  AND distributed.id = 3) as foo3,
	(SELECT local.id FROM distributed, local WHERE local.id = distributed.id  AND distributed.id = 4) as foo4,
	(SELECT local.id FROM distributed, local WHERE local.id = distributed.id  AND distributed.id = 5) as foo5
WHERE
	foo1.id = foo4.id AND
	foo1.id = foo2.id AND
	foo1.id = foo3.id AND
	foo1.id = foo4.id AND
	foo1.id = foo5.id
ORDER BY 1;

--https://github.com/citusdata/citus/issues/5384
-- SELECT
-- 	count(*)
-- FROM
--  distributed
-- JOIN LATERAL
-- 	(SELECT
-- 		*
-- 	FROM
-- 		local
-- 	JOIN
-- 		distributed d2
-- 	ON(true)
-- 		WHERE local.id = distributed.id AND d2.id = local.id) as foo
-- ON (true);

SELECT local.title, local.title FROM local JOIN distributed USING(id) ORDER BY 1,2 LIMIt 1;
SELECT NULL FROM local JOIN distributed USING(id) ORDER BY 1 LIMIt 1;
SELECT distributed.name, distributed.name,  local.title, local.title FROM local JOIN distributed USING(id) ORDER BY 1,2,3,4 LIMIT 1;

BEGIN;
SELECT COUNT(DISTINCT title) FROM local;
UPDATE
	local
SET
	title = 'test'
FROM
	distributed
WHERE
	distributed.id = local.id;
SELECT COUNT(DISTINCT title) FROM local;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT name) FROM distributed;
UPDATE
	distributed
SET
	name = 'test'
FROM
	local
WHERE
	distributed.id = local.id;
SELECT COUNT(DISTINCT name) FROM distributed;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT name) FROM distributed;
UPDATE
	distributed
SET
	name = 'test'
FROM
	local
WHERE
	distributed.id = local.id;
SELECT COUNT(DISTINCT name) FROM distributed;
ROLLBACK;

BEGIN;
SELECT COUNT(DISTINCT name) FROM distributed;
UPDATE
	distributed
SET
	name = 'test'
FROM
	local
WHERE
	distributed.id = local.id;
SELECT COUNT(DISTINCT name) FROM distributed;
ROLLBACK;
