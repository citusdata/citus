SET search_path TO sequences_schema;

-- see the renamed sequence object
select count(*) from pg_sequence where seqrelid = 'renamed_seq'::regclass;

TRUNCATE seq_test_0;
INSERT INTO seq_test_0 VALUES (1);

-- verify that sequence works properly
select max(z) into maxval_z from seq_test_0;
select max(y) into maxval_y from seq_test_0;
select max+1=nextval('renamed_seq') as check_sanity from maxval_z;
select max+1=nextval('seq_1') as check_sanity from maxval_y;

TRUNCATE seq_test_0;
INSERT INTO seq_test_0 VALUES (199999, DEFAULT, DEFAULT);
drop table maxval_z;
select max(z) into maxval_z from seq_test_0;
SELECT pg_sequence_last_value('renamed_seq'::regclass) = max FROM maxval_z;

TRUNCATE seq_test_0;
BEGIN;
    INSERT INTO seq_test_0 VALUES (2);
    -- verify that sequence works properly
    select max(z)+1=nextval('renamed_seq') as check_sanity from seq_test_0 ;
    select max(y)+1=nextval('seq_1') as check_sanity from seq_test_0 ;
COMMIT;
