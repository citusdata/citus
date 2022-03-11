SET search_path TO sequences_schema;

-- see the renamed sequence object
select count(*) from pg_sequence where seqrelid = 'renamed_seq'::regclass;

TRUNCATE seq_test_0;
INSERT INTO seq_test_0 VALUES (1);

-- verify that sequence works properly
select max(z)<nextval('renamed_seq') as check_sanity from seq_test_0 ;
select max(y)<nextval('seq_1') as check_sanity from seq_test_0 ;

TRUNCATE seq_test_0;
BEGIN;
    INSERT INTO seq_test_0 VALUES (199999, DEFAULT, DEFAULT);
    SELECT 1 from (select setval('renamed_seq', max(z)) FROM seq_test_0) as setvalue;
    SELECT currval('renamed_seq') = max(z) FROM seq_test_0;
COMMIT;

TRUNCATE seq_test_0;
BEGIN;
    INSERT INTO seq_test_0 VALUES (2);
    -- verify that sequence works properly
    select max(z)<nextval('renamed_seq') as check_sanity from seq_test_0 ;
    select max(y)<nextval('seq_1') as check_sanity from seq_test_0 ;
COMMIT;
