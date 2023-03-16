//
// How we organize this isolation test spec, is explained at README.md file in this directory.
//

// create distributed tables to test behavior of MERGE in concurrent operations
setup
{
	DO
	$do$
	DECLARE ver int;
	BEGIN
	  SELECT substring(version(), '\d+')::int into ver;
	  IF (ver < 15)
	  THEN
	    RAISE EXCEPTION 'MERGE is not supported on PG versions below 15';
	  END IF;
	  END
	$do$;

	SET citus.shard_replication_factor TO 2;
	SELECT 1 FROM master_add_node('localhost', 57637);
	SELECT 1 FROM master_add_node('localhost', 57638);

	CREATE TABLE prept(t1 int, t2 int);
	CREATE TABLE preps(s1 int, s2 int);
	SELECT create_distributed_table('prept', 't1'), create_distributed_table('preps', 's1');
	INSERT INTO prept VALUES(100, 0);
	INSERT INTO preps VALUES(100, 0);
	INSERT INTO preps VALUES(200, 0);
}

// drop distributed tables
teardown
{
	DROP TABLE IF EXISTS prept CASCADE;
	DROP TABLE IF EXISTS preps CASCADE;
}

// session 1
session "s1"

step "s1-begin" { BEGIN; }
step "s1-upd-ins" { MERGE INTO prept USING preps ON prept.t1 = preps.s1
			WHEN MATCHED THEN UPDATE SET t2 = t2 + 1
			WHEN NOT MATCHED THEN INSERT VALUES(s1, s2); }
step "s1-result" { SELECT * FROM preps ORDER BY 1; }
step "s1-commit" { COMMIT; }

// session 2
session "s2"

step "s2-begin" { BEGIN; }
step "s2-update" { UPDATE preps SET s2 = s2 + 1; }
step "s2-commit" { COMMIT; }
step "s2-result" { SELECT * FROM prept ORDER BY 1; }

// permutations - MERGE vs UPDATE (on source)
permutation "s1-begin" "s1-upd-ins" "s2-begin" "s2-update" "s1-commit" "s2-commit" "s1-result" "s2-result"
