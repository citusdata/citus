SET search_path TO upgrade_type, public;
BEGIN;
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';

-- test distributed type
INSERT INTO tt VALUES (1, (2,3)::type1);
SELECT * FROM tt ORDER BY 1, 2;
ALTER TYPE type1 RENAME TO type1_newname;
INSERT INTO tt VALUES (3, (4,5)::type1_newname);

ROLLBACK;
