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

