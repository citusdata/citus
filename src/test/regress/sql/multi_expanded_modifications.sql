CREATE TABLE modifications (
	key VARCHAR,
	counter INT,
	time TIMESTAMPTZ
);
SELECT master_create_distributed_table('modifications', 'key', 'hash');
SELECT master_create_worker_shards('modifications', 4, 1);

INSERT INTO modifications VALUES ('one', 0, null);

UPDATE modifications SET counter = counter + 1;

UPDATE modifications SET counter = counter + 1 WHERE key = 'one';

UPDATE modifications SET time = now() WHERE key = 'one';

SELECT * FROM modifications;

DROP TABLE modifications;
