-- Test PostgreSQL 18 "?column?" fix for subqueries_deep
-- Create basic tables for testing
CREATE SCHEMA test_pg18_fix;
SET search_path TO test_pg18_fix, public;

-- Create test tables similar to what the regular tests use
CREATE TABLE users_table (
    user_id int,
    time timestamp,
    value_1 int,
    value_2 int,
    value_3 double precision,
    value_4 bigint
);

CREATE TABLE events_table (
    user_id int,
    time timestamp,
    event_type int,
    value_2 int,
    value_3 double precision,
    value_4 bigint
);

-- Create distributed tables  
SELECT create_distributed_table('users_table', 'user_id');
SELECT create_distributed_table('events_table', 'user_id');

-- Insert some test data
INSERT INTO users_table VALUES 
(1, '2024-01-01', 10, 1, 1.1, 100),
(2, '2024-01-02', 20, 2, 2.2, 200),
(3, '2024-01-03', 30, 3, 3.3, 300),
(4, '2024-01-04', 40, 4, 4.4, 400),
(5, '2024-01-05', 50, 5, 5.5, 500),
(6, '2024-01-06', 60, 6, 6.6, 600);

INSERT INTO events_table VALUES 
(1, '2024-01-01', 1, 1, 1.1, 100),
(2, '2024-01-02', 2, 2, 2.2, 200),
(3, '2024-01-03', 3, 3, 3.3, 300),
(4, '2024-01-04', 4, 4, 4.4, 400),
(5, '2024-01-05', 5, 5, 5.5, 500),
(6, '2024-01-06', 1, 6, 6.6, 600);

SET client_min_messages TO DEBUG1;

-- Test the original failing query from subqueries_deep.sql
SELECT
	DISTINCT user_id
FROM
	(
		SELECT users_table.user_id FROM users_table,
		(
			SELECT
				avg(event_type) as avg_val
			FROM
				(SELECT
					event_type, users_table.user_id
				 FROM
				 	users_table, (SELECT user_id, event_type FROM events_table WHERE value_2 < 3 ORDER BY 1, 2 OFFSET 3) as foo
				 WHERE
				 	foo.user_id = users_table.user_id) bar, users_table
			WHERE
				bar.user_id = users_table.user_id
		GROUP BY
			users_table.value_1
		) as baz
		WHERE
			baz.avg_val < users_table.user_id
		ORDER BY 1
		LIMIT 3
	) as sub1
	ORDER BY 1 DESC;

-- Clean up
SET search_path TO public;
DROP SCHEMA test_pg18_fix CASCADE;
