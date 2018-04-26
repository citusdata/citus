--
-- MULTI_ORDERBY_LIMIT_PUSHDOWN
--

-- order by pushdown with aggregates
-- tests order by when pushing down order by clauses including aggregate
-- aggregate expressions.

SELECT user_id, avg(value_1)
FROM users_table
GROUP BY user_id
ORDER BY avg(value_1) DESC
LIMIT 5;

SELECT user_id, avg(value_1)
FROM users_table
GROUP BY user_id
ORDER BY avg(value_1) DESC
LIMIT 1;

EXPLAIN
SELECT user_id, avg(value_1)
FROM users_table
GROUP BY user_id
ORDER BY avg(value_1) DESC
LIMIT 1;

SELECT user_id, avg(value_1) + 1
FROM users_table
GROUP BY user_id
ORDER BY avg(value_1) + 1 DESC
LIMIT 1;

SELECT user_id, avg(value_1)
FROM users_table
GROUP BY user_id
ORDER BY avg(value_1) + 1 DESC
LIMIT 1;

SELECT user_id, avg(value_1) + sum(value_2)
FROM users_table
GROUP BY user_id
ORDER BY 2  DESC
LIMIT 1;

SELECT user_id, avg(value_1) + count(value_2)
FROM users_table
GROUP BY user_id
ORDER BY 2  DESC;

EXPLAIN 
SELECT user_id, avg(value_1) + count(value_2)
FROM users_table
GROUP BY user_id
ORDER BY 2  DESC;

SELECT user_id, avg(value_1) + count(value_2)
FROM users_table
GROUP BY user_id
ORDER BY 2  DESC
LIMIT 1;

SELECT user_id, sum(value_1) + sum(value_2)
FROM users_table
GROUP BY user_id
ORDER BY 2  DESC
LIMIT 1;

SELECT user_id, sum(value_1) + sum(value_2)
FROM users_table
GROUP BY user_id
ORDER BY sum(value_2)  DESC
LIMIT 1;

SELECT user_id, (100 / max(value_1))
FROM users_table
GROUP BY user_id
ORDER BY 2  DESC, 1 DESC
LIMIT 2;

SELECT user_id, (100 / (1 + min(value_1)))
FROM users_table
GROUP BY user_id
ORDER BY 2  DESC, 1
LIMIT 2;


SELECT user_id, sum(value_1 + value_2)
FROM users_table
GROUP BY user_id
ORDER BY 2  DESC
LIMIT 2;

SELECT user_id, 10000 / (sum(value_1 + value_2))
FROM users_table
GROUP BY user_id
ORDER BY 2  DESC
LIMIT 2;

SELECT user_id, sum(value_1 + value_2)
FROM users_table
GROUP BY user_id
ORDER BY (10000 / (sum(value_1 + value_2)))  DESC
LIMIT 2;

SELECT user_id
FROM users_table
GROUP BY user_id
ORDER BY (10000 / (sum(value_1 + value_2)))  DESC
LIMIT 2;

EXPLAIN (COSTS OFF)
SELECT user_id
FROM users_table
GROUP BY user_id
ORDER BY (10000 / (sum(value_1 + value_2)))  DESC
LIMIT 2;

SELECT 10000 / (sum(value_1 + value_2))
FROM users_table
ORDER BY 1 DESC
LIMIT 2;

SELECT user_id, AVG(value_1)
FROM users_table
GROUP BY user_id
ORDER BY user_id * avg(value_1) DESC
LIMIT 2;

SELECT user_id, AVG(value_1)
FROM users_table
GROUP BY user_id
ORDER BY user_id * avg(value_1 + value_2) DESC
LIMIT 2;

SELECT user_id
FROM users_table
GROUP BY user_id
ORDER BY sum(value_1) DESC
LIMIT 2;

EXPLAIN (COSTS OFF)
SELECT user_id
FROM users_table
GROUP BY user_id
ORDER BY sum(value_1) DESC
LIMIT 2;

SELECT ut.user_id, avg(ut.value_2)
FROM users_table ut, events_table et
WHERE ut.user_id = et.user_id and et.value_2 < 5
GROUP BY ut.user_id
ORDER BY MAX(et.time), AVG(ut.value_1)
LIMIT 5;

EXPLAIN (COSTS OFF)
SELECT ut.user_id, avg(ut.value_2)
FROM users_table ut, events_table et
WHERE ut.user_id = et.user_id and et.value_2 < 5
GROUP BY ut.user_id
ORDER BY MAX(et.time), AVG(ut.value_1)
LIMIT 5;


SELECT ut.user_id, avg(et.value_2)
FROM users_table ut, events_table et
WHERE ut.user_id = et.user_id and et.value_2 < 5
GROUP BY ut.user_id
ORDER BY avg(ut.value_2) DESC, AVG(et.value_2)
LIMIT 5;

SELECT ut.user_id, count(DISTINCT ut.value_2)
FROM users_table ut, events_table et
WHERE ut.user_id = et.user_id and et.value_2 < 5
GROUP BY ut.user_id
ORDER BY 2, AVG(ut.value_1), 1 DESC
LIMIT 2;

EXPLAIN (COSTS OFF)
SELECT ut.user_id, count(DISTINCT ut.value_2)
FROM users_table ut, events_table et
WHERE ut.user_id = et.user_id and et.value_2 < 5
GROUP BY ut.user_id
ORDER BY 2, AVG(ut.value_1), 1 DESC
LIMIT 5;
