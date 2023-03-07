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
	    RAISE EXCEPTION 'MERGE is not supported on PG version: %', ver;
	  END IF;
	  END
	$do$;

	SET citus.shard_replication_factor TO 1;
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
step "s1-insert" { MERGE INTO prept USING preps ON prept.t1 = preps.s1
			WHEN MATCHED THEN UPDATE SET t2 = t2 + 1
			WHEN NOT MATCHED THEN INSERT VALUES(s1, s2);
		}
step "s1-commit" { COMMIT; }

// session 2
session "s2"
step "s2-begin" { BEGIN; }
step "s2-delete" { MERGE INTO prept USING preps ON prept.t1 = preps.s1
			WHEN MATCHED AND prept.t2 = 0 THEN DELETE
			WHEN MATCHED THEN UPDATE SET t2 = t2 + 1;
		}
step "s2-commit" { COMMIT; }
step "s2-result" { SELECT * FROM prept; }

// permutations - MERGE vs MERGE
permutation "s1-begin" "s2-begin" "s1-insert" "s2-delete" "s1-commit" "s2-commit" "s2-result"
permutation "s2-begin" "s2-delete" "s1-begin" "s1-insert" "s2-commit" "s1-commit" "s2-result"
