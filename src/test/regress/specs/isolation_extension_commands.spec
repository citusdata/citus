setup
{
	SELECT 1 FROM master_add_node('localhost', 57638);	
	
	create schema if not exists schema1;
	create schema if not exists schema2;
	CREATE schema if not exists schema3;
}

teardown
{
  SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-add-node-1"
{
	SELECT 1 FROM master_add_node('localhost', 57637);
}

step "s1-remove-node-1"
{
	SELECT 1 FROM master_remove_node('localhost', 57637);
}

step "s1-commit"
{
	COMMIT;
}

step "s1-create-extension-with-schema2"
{
	CREATE extension seg with schema schema2;
}

step "s1-print"
{
	select count(*) from citus.pg_dist_object ;
	select extname, extversion, nspname from pg_extension, pg_namespace where pg_namespace.oid=pg_extension.extnamespace and extname='seg';
	SELECT run_command_on_workers($$select extname from pg_extension where extname='seg'$$);
	SELECT run_command_on_workers($$select extversion from pg_extension where extname='seg'$$);
	SELECT run_command_on_workers($$select nspname from pg_extension, pg_namespace where extname='seg' and pg_extension.extnamespace=pg_namespace.oid$$);
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-add-node-1"
{
	SELECT 1 FROM master_add_node('localhost', 57637);
}

step "s2-create-extension-version-11"
{
	CREATE extension seg VERSION "1.1";
}

step "s2-alter-extension-version-13"
{
	ALTER extension seg update to "1.3";
}

step "s2-create-extension-with-schema1"
{
	CREATE extension seg with schema schema1;
}

step "s2-create-extension-with-schema2"
{
	CREATE extension seg with schema schema2;
}

step "s2-drop-extension"
{
	drop extension seg;
}

step "s2-alter-extension-update-to-version-12"
{
	ALTER extension seg update to "1.2";
}

step "s2-alter-extension-set-schema3"
{
	alter extension seg set schema schema3;
}

step "s2-commit"
{
	COMMIT;
}

step "s2-remove-node-1"
{
	SELECT 1 FROM master_remove_node('localhost', 57637);
}

# master_#_node vs extension command
permutation "s1-begin" "s1-add-node-1" "s2-create-extension-version-11" "s1-commit" "s1-print"
permutation "s1-begin" "s1-add-node-1" "s2-alter-extension-update-to-version-12" "s1-commit" "s1-print"
permutation "s1-add-node-1" "s1-begin" "s1-remove-node-1" "s2-drop-extension" "s1-commit" "s1-print"
permutation "s1-begin" "s1-add-node-1" "s2-create-extension-with-schema1" "s1-commit" "s1-print"
permutation "s1-begin" "s1-add-node-1" "s2-drop-extension" "s1-commit" "s1-print"
permutation "s1-add-node-1" "s1-create-extension-with-schema2" "s1-begin" "s1-remove-node-1" "s2-alter-extension-set-schema3" "s1-commit" "s1-print"
permutation "s1-add-node-1" "s2-drop-extension" "s1-begin" "s1-remove-node-1" "s2-create-extension-with-schema1" "s1-commit" "s1-print"

# extension command vs master_#_node 
permutation "s2-add-node-1" "s2-drop-extension" "s2-remove-node-1" "s2-begin" "s2-create-extension-version-11" "s1-add-node-1" "s2-commit" "s1-print" 
permutation "s2-drop-extension" "s2-add-node-1" "s2-create-extension-version-11" "s2-remove-node-1" "s2-begin" "s2-alter-extension-update-to-version-12" "s1-add-node-1" "s2-commit" "s1-print"
permutation "s2-add-node-1" "s2-begin" "s2-drop-extension" "s1-remove-node-1" "s2-commit" "s1-print"
permutation "s2-begin" "s2-create-extension-with-schema1" "s1-add-node-1" "s2-commit" "s1-print"
permutation "s2-drop-extension" "s2-add-node-1" "s2-create-extension-with-schema2" "s2-begin" "s2-alter-extension-version-13" "s1-remove-node-1" "s2-commit" "s1-print"
permutation "s2-drop-extension" "s2-add-node-1" "s2-begin" "s2-create-extension-version-11" "s1-remove-node-1" "s2-commit" "s1-print"
permutation "s2-drop-extension" "s2-add-node-1" "s2-create-extension-version-11" "s2-remove-node-1" "s2-begin" "s2-drop-extension" "s1-add-node-1" "s2-commit" "s1-print"
