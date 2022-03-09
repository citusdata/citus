SET search_path TO sequences_schema;

INSERT INTO seq_test_0 VALUES (1,2);

ALTER SEQUENCE seq_0 RENAME TO sequence_0;

-- see the renamed sequence object
select count(*) from pg_sequence where seqrelid = 'sequence_0'::regclass;

DROP SCHEMA sequences_schema CASCADE;
