setup
{
    SELECT 1 FROM master_add_node('localhost', 57636, 0);
  	CREATE TABLE citus_local_table_1(a int);
    CREATE TABLE citus_local_table_2(a int unique);

    CREATE SCHEMA another_schema;
    CREATE TABLE another_schema.citus_local_table_3(a int unique);
}

teardown
{
	  DROP TABLE IF EXISTS citus_local_table_1, citus_local_table_2 CASCADE;
    DROP SCHEMA IF EXISTS another_schema CASCADE;
    -- remove coordinator only if it is added to pg_dist_node
    SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node WHERE nodeport=57636;
}

session "s1"

step "s1-begin" { BEGIN; }
step "s1-create-citus-local-table-1" { SELECT citus_add_local_table_to_metadata('citus_local_table_1'); }
step "s1-create-citus-local-table-3" { SELECT citus_add_local_table_to_metadata('another_schema.citus_local_table_3'); }
step "s1-drop-table" { DROP TABLE citus_local_table_1; }
step "s1-delete" { DELETE FROM citus_local_table_1 WHERE a=2; }
step "s1-select" { SELECT * FROM citus_local_table_1; }
step "s1-remove-coordinator" { SELECT master_remove_node('localhost', 57636); }
step "s1-add-coordinator" { SELECT 1 FROM master_add_node('localhost', 57636, 0); }
step "s1-commit" { COMMIT; }
step "s1-rollback" { ROLLBACK; }

session "s2"

step "s2-begin" { BEGIN; }
step "s2-create-citus-local-table-1" { SELECT citus_add_local_table_to_metadata('citus_local_table_1'); }
step "s2-create-citus-local-table-2" { SELECT citus_add_local_table_to_metadata('citus_local_table_2'); }
step "s2-create-citus-local-table-3" { SELECT citus_add_local_table_to_metadata('another_schema.citus_local_table_3'); }
step "s2-select" { SELECT * FROM citus_local_table_1; }
step "s2-update" { UPDATE citus_local_table_1 SET a=1 WHERE a=2; }
step "s2-truncate" { TRUNCATE citus_local_table_1; }
step "s2-fkey-to-another" { ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_c_to_c FOREIGN KEY(a) REFERENCES citus_local_table_2(a); }
step "s2-remove-coordinator" { SELECT master_remove_node('localhost', 57636); }
step "s2-commit" { COMMIT; }


// citus_add_local_table_to_metadata vs command/query //

// Second session should error out as the table becomes a citus local table after the first session commits ..
permutation "s1-begin" "s2-begin" "s1-create-citus-local-table-1" "s2-create-citus-local-table-1" "s1-commit" "s2-commit"
// .. and it should error out even if we are in a different schema than the table
permutation "s1-begin" "s2-begin" "s1-create-citus-local-table-3" "s2-create-citus-local-table-3" "s1-commit" "s2-commit"
// Second session should be able to create citus local table as the first one rollbacks
permutation "s1-begin" "s2-begin" "s1-create-citus-local-table-1" "s2-create-citus-local-table-1" "s1-rollback" "s2-commit"
// Any modifying queries, DML commands and SELECT will block
permutation "s1-begin" "s2-begin" "s1-create-citus-local-table-1" "s2-select" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-create-citus-local-table-1" "s2-update" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-create-citus-local-table-1" "s2-truncate" "s1-commit" "s2-commit"
// Foreign key creation should succeed as it will be blocked until first session creates citus local table
permutation "s2-create-citus-local-table-2" "s1-begin" "s2-begin" "s1-create-citus-local-table-1" "s2-fkey-to-another" "s1-commit" "s2-commit"
// master_remove_node should first block and then fail
permutation "s1-begin" "s2-begin" "s1-create-citus-local-table-1" "s2-remove-coordinator" "s1-commit" "s2-commit"


// command/query vs citus_add_local_table_to_metadata //

// citus_add_local_table_to_metadata_1 should first block and then error out as the first session drops the table
permutation "s1-begin" "s2-begin" "s1-drop-table" "s2-create-citus-local-table-1" "s1-commit" "s2-commit"
// Any modifying queries, DML commands and SELECT will block
permutation "s1-begin" "s2-begin" "s1-delete" "s2-create-citus-local-table-1" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-select" "s2-create-citus-local-table-1" "s1-commit" "s2-commit"
// citus_add_local_table_to_metadata should block on master_remove_node and then fail
permutation "s1-begin" "s2-begin" "s1-remove-coordinator" "s2-create-citus-local-table-1" "s1-commit" "s2-commit"
// citus_add_local_table_to_metadata should block on master_add_node and then succeed
permutation "s1-remove-coordinator" "s1-begin" "s2-begin" "s1-add-coordinator" "s2-create-citus-local-table-1" "s1-commit" "s2-commit"
