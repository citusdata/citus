# Rules to normalize test outputs. Our custom diff tool passes test output
# of tests in normalized_tests.lst through the substitution rules in this file
# before doing the actual comparison.
#
# An example of when this is useful is when an error happens on a different
# port number, or a different worker shard, or a different placement, etc.
# because we are running the tests in a different configuration.

# In all tests, normalize worker ports, placement ids, and shard ids
s/localhost:[0-9]+/localhost:xxxxx/g
s/placement [0-9]+/placement xxxxx/g
s/shard [0-9]+/shard xxxxx/g

# In foreign_key_to_reference_table, normalize shard table names, etc in
# the generated plan
s/"(foreign_key_2_|fkey_ref_to_dist_|fkey_ref_)[0-9]+"/"\1xxxxxxx"/g
s/"(referenced_table_|referencing_table_|referencing_table2_)[0-9]+"/"\1xxxxxxx"/g
s/\(id\)=\([0-9]+\)/(id)=(X)/g
s/\(ref_id\)=\([0-9]+\)/(ref_id)=(X)/g

# Savepoint error messages changed between postgres 10 and 11.
s/savepoint ".*" does not exist/no such savepoint/g

# shard table names for multi_subtransactions
s/"t2_[0-9]+"/"t2_xxxxxxx"/g

# In foreign_key_restriction_enforcement, normalize shard names
s/"(on_update_fkey_table_|fkey_)[0-9]+"/"\1xxxxxxx"/g

# In multi_insert_select_conflict, normalize shard name and constraints
s/"(target_table_|target_table_|test_ref_table_)[0-9]+"/"\1xxxxxxx"/g
s/\(col_1\)=\([0-9]+\)/(col_1)=(X)/g

# normalize pkey constraints in multi_insert_select.sql
s/"(raw_events_second_user_id_value_1_key_|agg_events_user_id_value_1_agg_key_)[0-9]+"/"\1xxxxxxx"/g

# normalize explain outputs, basically wipeout the executor name from the output
s/.*Custom Scan \(Citus.*/Custom Scan \(Citus\)/g
s/.*-------------.*/---------------------------------------------------------------------/g
s/.* QUERY PLAN .*/                              QUERY PLAN                              /g
s/.*Custom Plan Provider.*Citus.*/              \"Custom Plan Provider\": \"Citus\",     /g
s/.*Custom-Plan-Provide.*/\<Custom-Plan-Provider\>Citus Unified\<\/Custom-Plan-Provider\>     /g
