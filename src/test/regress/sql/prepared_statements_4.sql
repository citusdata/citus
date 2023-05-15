SET search_path TO "prepared statements";


PREPARE prepared_relabel_insert(varchar) AS
	INSERT INTO text_partition_column_table VALUES ($1, 1);

EXECUTE prepared_relabel_insert('test');
EXECUTE prepared_relabel_insert('test');
EXECUTE prepared_relabel_insert('test');
EXECUTE prepared_relabel_insert('test');
EXECUTE prepared_relabel_insert('test');
EXECUTE prepared_relabel_insert('test');
EXECUTE prepared_relabel_insert('test');

SELECT key, value FROM text_partition_column_table ORDER BY key;

PREPARE prepared_coercion_to_domain_insert(text) AS
    INSERT INTO domain_partition_column_table VALUES ($1, 1);

EXECUTE prepared_coercion_to_domain_insert('test-1');
EXECUTE prepared_coercion_to_domain_insert('test-2');
EXECUTE prepared_coercion_to_domain_insert('test-3');
EXECUTE prepared_coercion_to_domain_insert('test-4');
EXECUTE prepared_coercion_to_domain_insert('test-5');
EXECUTE prepared_coercion_to_domain_insert('test-6');
EXECUTE prepared_coercion_to_domain_insert('test-7');

PREPARE FOO AS INSERT INTO http_request (
  site_id, ingest_time, url, request_country,
  ip_address, status_code, response_time_msec
) VALUES (
  1, clock_timestamp(), 'http://example.com/path', 'USA',
  inet '88.250.10.123', 200, 10
);
EXECUTE foo;
EXECUTE foo;
EXECUTE foo;
EXECUTE foo;
EXECUTE foo;
EXECUTE foo;
EXECUTE foo;
EXECUTE foo;

SELECT count(distinct ingest_time) FROM http_request WHERE site_id = 1;

-- Standard planner converted text and varchar casts to cstring in some cases
-- We make sure we convert it back to text when parsing the expression
INSERT INTO test VALUES ('2022-02-02', 0);
INSERT INTO test VALUES ('2022-01-01', 1);
INSERT INTO test VALUES ('2021-01-01', 2);

-- try different planners
PREPARE test_statement_regular(text) AS
SELECT user_id FROM test WHERE t >= $1::timestamp ORDER BY user_id;

EXECUTE test_statement_regular('2022-01-01');
EXECUTE test_statement_regular('2022-01-01');
EXECUTE test_statement_regular('2022-01-01');
EXECUTE test_statement_regular('2022-01-01');
EXECUTE test_statement_regular('2022-01-01');
EXECUTE test_statement_regular('2022-01-01');
EXECUTE test_statement_regular('2022-01-01');
EXECUTE test_statement_regular('2022-01-01');

PREPARE test_statement_router(int, text) AS
SELECT user_id FROM test WHERE user_id = $1 AND t >= $2::timestamp ORDER BY user_id;

EXECUTE test_statement_router(1, '2022-01-01');
EXECUTE test_statement_router(1, '2022-01-01');
EXECUTE test_statement_router(1, '2022-01-01');
EXECUTE test_statement_router(1, '2022-01-01');
EXECUTE test_statement_router(1, '2022-01-01');
EXECUTE test_statement_router(1, '2022-01-01');
EXECUTE test_statement_router(1, '2022-01-01');
EXECUTE test_statement_router(1, '2022-01-01');

PREPARE test_statement_repartition(int, text) AS
SELECT count(*) FROM test t1 JOIN test t2 USING (t) WHERE t1.user_id = $1 AND t >= $2::timestamp;

EXECUTE test_statement_repartition(1, '2022-01-01');
EXECUTE test_statement_repartition(1, '2022-01-01');
EXECUTE test_statement_repartition(1, '2022-01-01');
EXECUTE test_statement_repartition(1, '2022-01-01');
EXECUTE test_statement_repartition(1, '2022-01-01');
EXECUTE test_statement_repartition(1, '2022-01-01');
EXECUTE test_statement_repartition(1, '2022-01-01');
EXECUTE test_statement_repartition(1, '2022-01-01');

PREPARE test_statement_cte(text, text) AS
WITH cte_1 AS MATERIALIZED (SELECT user_id, t FROM test WHERE t >= $1::timestamp ORDER BY user_id)
SELECT user_id FROM cte_1 WHERE t <= $2::timestamp;

EXECUTE test_statement_cte('2022-01-01', '2022-01-01');
EXECUTE test_statement_cte('2022-01-01', '2022-01-01');
EXECUTE test_statement_cte('2022-01-01', '2022-01-01');
EXECUTE test_statement_cte('2022-01-01', '2022-01-01');
EXECUTE test_statement_cte('2022-01-01', '2022-01-01');
EXECUTE test_statement_cte('2022-01-01', '2022-01-01');
EXECUTE test_statement_cte('2022-01-01', '2022-01-01');
EXECUTE test_statement_cte('2022-01-01', '2022-01-01');

PREPARE test_statement_insert(int, text) AS
INSERT INTO test VALUES ($2::timestamp, $1);

EXECUTE test_statement_insert(3, '2022-03-03');
EXECUTE test_statement_insert(4, '2022-04-04');
EXECUTE test_statement_insert(5, '2022-05-05');
EXECUTE test_statement_insert(6, '2022-06-06');
EXECUTE test_statement_insert(7, '2022-07-07');
EXECUTE test_statement_insert(8, '2022-08-08');
EXECUTE test_statement_insert(9, '2022-09-09');
EXECUTE test_statement_insert(10, '2022-10-10');

SELECT count(*) FROM test;
EXECUTE test_statement_regular('2022-01-01');

PREPARE test_statement_null(text) AS
SELECT user_id , $1::timestamp FROM test ORDER BY user_id LIMIT 2;

EXECUTE test_statement_null(NULL);
EXECUTE test_statement_null(NULL);
EXECUTE test_statement_null(NULL);
EXECUTE test_statement_null(NULL);
EXECUTE test_statement_null(NULL);
EXECUTE test_statement_null(NULL);
EXECUTE test_statement_null(NULL);
EXECUTE test_statement_null(NULL);
