-- Test PostgreSQL 18 compatibility using existing distributed tables
\c citus

-- Test the exact pattern from subqueries_deep.sql that should fail without our fix
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
				 	users_table, (SELECT user_id, event_type FROM events_table WHERE value_2 < 3 ORDER BY 1, 2 OFFSET 0) as foo
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
