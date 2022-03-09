SET search_path TO sequences_schema;

-- see the sequence object
select count(*) from pg_sequence where seqrelid = 'seq_0'::regclass;

INSERT INTO seq_test_0 VALUES (1,2);

-- verify that sequence works properly
select max(z)<nextval('seq_0') as check_sanity from seq_test_0 ;
