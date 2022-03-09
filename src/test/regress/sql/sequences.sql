SET search_path TO sequences_schema;

INSERT INTO seq_test_0 VALUES (1,2);

-- see the renamed sequence object
select count(*) from pg_sequence where seqrelid = 'seq_0'::regclass;
