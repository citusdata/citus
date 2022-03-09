SET search_path TO sequences_schema;

INSERT INTO seq_test_0 VALUES (1,2) RETURNING *;
INSERT INTO seq_test_0_local_table VALUES (1,2) RETURNING *;

ALTER SEQUENCE seq_0 RENAME TO sequence_0;
ALTER SEQUENCE seq_0_local_table RENAME TO sequence_0_local_table;

-- see the renamed sequence objects
select * from pg_sequence where seqrelid = 'sequence_0'::regclass;
select * from pg_sequence where seqrelid = 'sequence_0_local_table'::regclass;

ALTER SEQUENCE sequence_0 OWNED BY seq_test_0.x;
ALTER SEQUENCE sequence_0_local_table OWNED BY seq_test_0_local_table.x;

-- dropping the tables should cascade to sequence objects
DROP TABLE seq_test_0 CASCADE;
DROP TABLE seq_test_0_local_table CASCADE;

-- verify dropped
select * from pg_sequence where seqrelid = 'sequence_0'::regclass;
select * from pg_sequence where seqrelid = 'sequence_0_local_table'::regclass;

DROP SCHEMA sequences_schema CASCADE;
