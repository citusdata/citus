SET search_path TO sequences_schema;

-- see the renamed sequence object
select count(*) from pg_sequence where seqrelid = 'renamed_seq'::regclass;

INSERT INTO seq_test_0 VALUES (1,2);

-- verify that sequence works properly
select max(z)<nextval('renamed_seq') as check_sanity from seq_test_0 ;
