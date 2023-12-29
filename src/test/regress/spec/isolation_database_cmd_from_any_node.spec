session "s1"

step "s1-begin" { BEGIN; }
step "s1-commit" { COMMIT; }
step "s1-rollback" { ROLLBACK; }
step "s1-enable-create-db-prop" { SET citus.enable_create_database_propagation TO ON; }

step "s1-create-user-dbuser" { CREATE USER dbuser; }
step "s1-drop-user-dbuser" { DROP USER dbuser; }

step "s1-acquire-citus-adv-oclass-lock" { SELECT citus_internal_acquire_citus_advisory_object_class_lock((SELECT CASE WHEN substring(version(), '\d+')::integer < 16 THEN 25 ELSE 26 END AS oclass_database), NULL); }
step "s1-acquire-citus-adv-oclass-lock-with-oid-testdb1" { SELECT citus_internal_acquire_citus_advisory_object_class_lock((SELECT CASE WHEN substring(version(), '\d+')::integer < 16 THEN 25 ELSE 26 END AS oclass_database), 'testdb1'); }

step "s1-create-testdb1" { CREATE DATABASE testdb1; }
step "s1-drop-testdb1" { DROP DATABASE testdb1; }
step "s1-alter-testdb1-rename-to-db1" { ALTER DATABASE testdb1 RENAME TO db1; }
step "s1-grant-on-testdb1-to-dbuser" { GRANT ALL ON DATABASE testdb1 TO dbuser;}

step "s1-drop-testdb2" { DROP DATABASE testdb2; }
step "s1-grant-on-testdb2-to-dbuser" { GRANT ALL ON DATABASE testdb2 TO dbuser;}

step "s1-create-db1" { CREATE DATABASE db1; }
step "s1-drop-db1" { DROP DATABASE db1; }

session "s2"

step "s2-begin" { BEGIN; }
step "s2-commit" { COMMIT; }
step "s2-rollback" { ROLLBACK; }
step "s2-enable-create-db-prop" { SET citus.enable_create_database_propagation TO ON; }

step "s2-acquire-citus-adv-oclass-lock" { SELECT citus_internal_acquire_citus_advisory_object_class_lock((SELECT CASE WHEN substring(version(), '\d+')::integer < 16 THEN 25 ELSE 26 END AS oclass_database), NULL); }
step "s2-acquire-citus-adv-oclass-lock-with-oid-testdb1" { SELECT citus_internal_acquire_citus_advisory_object_class_lock((SELECT CASE WHEN substring(version(), '\d+')::integer < 16 THEN 25 ELSE 26 END AS oclass_database), 'testdb1'); }
step "s2-acquire-citus-adv-oclass-lock-with-oid-testdb2" { SELECT citus_internal_acquire_citus_advisory_object_class_lock((SELECT CASE WHEN substring(version(), '\d+')::integer < 16 THEN 25 ELSE 26 END AS oclass_database), 'testdb2'); }

step "s2-alter-testdb1-rename-to-db1" { ALTER DATABASE testdb1 RENAME TO db1; }

step "s2-create-testdb2" { CREATE DATABASE testdb2; }
step "s2-drop-testdb2" { DROP DATABASE testdb2; }
step "s2-alter-testdb2-rename-to-db1" { ALTER DATABASE testdb2 RENAME TO db1; }
step "s2-alter-testdb2-rename-to-db2" { ALTER DATABASE testdb2 RENAME TO db2; }
step "s2-alter-testdb2-set-lc_monetary" { ALTER DATABASE testdb2 SET lc_monetary TO 'C'; }

step "s2-drop-db1" { DROP DATABASE db1; }

step "s2-drop-db2" { DROP DATABASE db2; }

// Given that we cannot execute CREATE / DROP DATABASE commands in a transaction block, we instead acquire the
// underlying advisory lock in some of below tests.

// e.g., CREATE DATABASE vs CREATE DATABASE
permutation "s1-begin" "s2-begin" "s1-acquire-citus-adv-oclass-lock" "s2-acquire-citus-adv-oclass-lock" "s1-commit" "s2-commit"

// e.g., DROP DATABASE vs DROP DATABASE
// dropping the same database
permutation "s1-enable-create-db-prop" "s1-create-testdb1" "s1-begin" "s2-begin" "s1-acquire-citus-adv-oclass-lock-with-oid-testdb1" "s2-acquire-citus-adv-oclass-lock-with-oid-testdb1" "s1-commit" "s2-commit" "s1-drop-testdb1"
// dropping a different database
permutation "s1-enable-create-db-prop" "s1-create-testdb1" "s2-enable-create-db-prop" "s2-create-testdb2" "s1-begin" "s2-begin" "s1-acquire-citus-adv-oclass-lock-with-oid-testdb1" "s2-acquire-citus-adv-oclass-lock-with-oid-testdb2" "s1-commit" "s2-commit" "s1-drop-testdb1" "s2-drop-testdb2"

// CREATE DATABASE vs DROP DATABASE
permutation "s2-enable-create-db-prop" "s2-create-testdb2" "s1-begin" "s2-begin" "s1-acquire-citus-adv-oclass-lock" "s2-acquire-citus-adv-oclass-lock-with-oid-testdb2" "s1-commit" "s2-commit" "s2-drop-testdb2"

// CREATE DATABASE vs ALTER DATABASE SET <config>
permutation "s2-enable-create-db-prop" "s2-create-testdb2" "s2-begin" "s2-alter-testdb2-set-lc_monetary" "s1-enable-create-db-prop" "s1-create-db1" "s2-rollback" "s2-drop-testdb2" "s1-drop-db1"

// GRANT .. ON DATABASE .. TO ... vs ALTER DATABASE SET <config>
// on the same database
permutation "s2-enable-create-db-prop" "s2-create-testdb2" "s2-begin" "s2-alter-testdb2-set-lc_monetary" "s1-create-user-dbuser" "s1-grant-on-testdb2-to-dbuser" "s2-rollback" "s2-drop-testdb2" "s1-drop-user-dbuser"
// on a different database
permutation "s2-enable-create-db-prop" "s2-create-testdb2" "s2-begin" "s2-alter-testdb2-set-lc_monetary" "s1-enable-create-db-prop" "s1-create-testdb1" "s1-create-user-dbuser" "s1-grant-on-testdb1-to-dbuser" "s2-rollback" "s2-drop-testdb2" "s1-drop-testdb1" "s1-drop-user-dbuser"

// ALTER DATABASE .. RENAME TO .. vs ALTER DATABASE .. RENAME TO ..
// try to rename different databases to the same name
permutation "s1-enable-create-db-prop" "s1-create-testdb1" "s2-enable-create-db-prop" "s2-create-testdb2" "s1-begin" "s2-begin" "s1-alter-testdb1-rename-to-db1" "s2-alter-testdb2-rename-to-db1" "s1-commit" "s2-rollback" "s1-drop-db1" "s2-drop-testdb2"
permutation "s1-enable-create-db-prop" "s1-create-testdb1" "s2-enable-create-db-prop" "s2-create-testdb2" "s1-begin" "s2-begin" "s1-alter-testdb1-rename-to-db1" "s2-alter-testdb2-rename-to-db1" "s1-rollback" "s2-commit" "s1-drop-testdb1" "s2-drop-db1"
// try to rename same database
permutation "s1-enable-create-db-prop" "s1-create-testdb1" "s1-begin" "s2-begin" "s1-alter-testdb1-rename-to-db1" "s2-alter-testdb1-rename-to-db1" "s1-commit" "s2-rollback" "s1-drop-db1"
permutation "s1-enable-create-db-prop" "s1-create-testdb1" "s1-begin" "s2-begin" "s1-alter-testdb1-rename-to-db1" "s2-alter-testdb1-rename-to-db1" "s1-rollback" "s2-commit" "s2-drop-db1"

// CREATE DATABASE vs ALTER DATABASE .. RENAME TO ..
permutation "s2-enable-create-db-prop" "s2-create-testdb2" "s2-begin" "s2-alter-testdb2-rename-to-db1" "s1-enable-create-db-prop" "s1-create-db1" "s2-rollback" "s2-drop-testdb2" "s1-drop-db1"
permutation "s2-enable-create-db-prop" "s2-create-testdb2" "s2-begin" "s2-alter-testdb2-rename-to-db1" "s1-enable-create-db-prop" "s1-create-db1" "s2-commit" "s2-drop-db1"
permutation "s2-enable-create-db-prop" "s2-create-testdb2" "s2-begin" "s2-alter-testdb2-rename-to-db2" "s1-enable-create-db-prop" "s1-create-db1" "s2-commit" "s2-drop-db2" "s1-drop-db1"

// DROP DATABASE vs ALTER DATABASE .. RENAME TO ..
// try to rename the same database
permutation "s2-enable-create-db-prop" "s2-create-testdb2" "s2-begin" "s2-alter-testdb2-rename-to-db1" "s1-drop-testdb2" "s2-rollback"
// try to rename a different database
permutation "s2-enable-create-db-prop" "s2-create-testdb2" "s1-enable-create-db-prop" "s1-create-db1" "s2-begin" "s2-alter-testdb2-rename-to-db2" "s1-drop-db1" "s2-commit" "s2-drop-db2"
