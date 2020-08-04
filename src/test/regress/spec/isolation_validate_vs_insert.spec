//
// How we organize this isolation test spec, is explained at README.md file in this directory.
//

// create distributed table to test behavior of VALIDATE in concurrent operations
setup
{
	SET citus.shard_replication_factor TO 1;
	CREATE TABLE constrained_table(id integer, int_data int);
	SELECT create_distributed_table('constrained_table', 'id');
}

// drop distributed table
teardown
{
	DROP TABLE IF EXISTS constrained_table CASCADE;
}

// session 1
session "s1"
step "s1-initialize" { INSERT INTO constrained_table VALUES (0, 0), (1, 1), (2, 2), (3, 4); }
step "s1-begin" { BEGIN; }
step "s1-add-constraint" { ALTER TABLE constrained_table ADD CONSTRAINT check_constraint CHECK(int_data<30) NOT VALID; }
step "s1-validate" { ALTER TABLE constrained_table VALIDATE CONSTRAINT check_constraint; }
step "s1-commit" { COMMIT; }

// session 2
session "s2"
step "s2-begin" { BEGIN; }
step "s2-insert" { INSERT INTO constrained_table VALUES(10, 10); }
step "s2-select" { SELECT sum(int_data) FROM constrained_table; }
step "s2-commit" { COMMIT; }

// permutations - check read and write are not blocked during validate queries
permutation "s1-initialize" "s1-add-constraint" "s1-begin" "s2-begin" "s1-validate" "s2-insert" "s1-commit" "s2-commit"
permutation "s1-initialize" "s1-add-constraint" "s1-begin" "s2-begin" "s1-validate" "s2-select" "s1-commit" "s2-commit"
permutation "s1-initialize" "s1-add-constraint" "s1-begin" "s2-begin" "s2-insert" "s1-validate" "s1-commit" "s2-commit"
permutation "s1-initialize" "s1-add-constraint" "s1-begin" "s2-begin" "s2-select" "s1-validate" "s1-commit" "s2-commit"
