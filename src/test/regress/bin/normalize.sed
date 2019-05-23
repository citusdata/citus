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
s/"(fkey_ref_|referenced_table_|referencing_table_)[0-9]+"/"\1xxxxxxx"/g
s/\(id\)=\([0-9]+\)/(id)=(X)/g

# shard table names for multi_subtransactions
s/"t2_[0-9]+"/"t2_xxxxxxx"/g

# normalize pkey constraints in multi_insert_select.sql
s/"(raw_events_second_user_id_value_1_key_|agg_events_user_id_value_1_agg_key_)[0-9]+"/"\1xxxxxxx"/g
