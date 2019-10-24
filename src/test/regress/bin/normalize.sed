# Rules to normalize test outputs. Our custom diff tool passes test output
# of tests in normalized_tests.lst through the substitution rules in this file
# before doing the actual comparison.
#
# An example of when this is useful is when an error happens on a different
# port number, or a different worker shard, or a different placement, etc.
# because we are running the tests in a different configuration.

# In all tests, normalize worker ports, placement ids, and shard ids
s/localhost:[0-9]+/localhost:xxxxx/g
s/ port=[0-9]+ / port=xxxxx /g
s/placement [0-9]+/placement xxxxx/g
s/shard [0-9]+/shard xxxxx/g
s/assigned task [0-9]+ to node/assigned task to node/
s/node group [12] (but|does)/node group \1/

# Differing names can have differing table column widths
s/(-+\|)+-+/---/g
s/.*-------------.*/---------------------------------------------------------------------/g

# In foreign_key_to_reference_table, normalize shard table names, etc in
# the generated plan
s/"(foreign_key_2_|fkey_ref_to_dist_|fkey_ref_)[0-9]+"/"\1xxxxxxx"/g
s/"(referenced_table_|referencing_table_|referencing_table2_)[0-9]+"/"\1xxxxxxx"/g
s/"(referencing_table_0_|referenced_table2_)[0-9]+"/"\1xxxxxxx"/g
s/\(id\)=\([0-9]+\)/(id)=(X)/g
s/\(ref_id\)=\([0-9]+\)/(ref_id)=(X)/g

# Savepoint error messages changed between postgres 10 and 11.
s/savepoint ".*" does not exist/no such savepoint/g

# shard table names for multi_subtransactions
s/"t2_[0-9]+"/"t2_xxxxxxx"/g

# shard table names for custom_aggregate_support
s/ daily_uniques_[0-9]+ / daily_uniques_xxxxxxx /g

# In foreign_key_restriction_enforcement, normalize shard names
s/"(on_update_fkey_table_|fkey_)[0-9]+"/"\1xxxxxxx"/g

# In multi_insert_select_conflict, normalize shard name and constraints
s/"(target_table_|target_table_|test_ref_table_)[0-9]+"/"\1xxxxxxx"/g
s/\(col_1\)=\([0-9]+\)/(col_1)=(X)/g

# In multi_name_lengths, normalize shard names
s/name_len_12345678901234567890123456789012345678_fcd8ab6f_[0-9]+/name_len_12345678901234567890123456789012345678_fcd8ab6f_xxxxx/g

# normalize pkey constraints in multi_insert_select.sql
s/"(raw_events_second_user_id_value_1_key_|agg_events_user_id_value_1_agg_key_)[0-9]+"/"\1xxxxxxx"/g

# normalize failed task ids
s/ERROR:  failed to execute task [0-9]+/ERROR:  failed to execute task X/g

# ignore could not consume warnings
/WARNING:  could not consume data from worker node/d

# ignore WAL warnings
/DEBUG: .+creating and filling new WAL file/d

# normalize file names for partitioned files
s/(task_[0-9]+\.)[0-9]+/\1xxxx/g
s/(job_[0-9]+\/task_[0-9]+\/p_[0-9]+\.)[0-9]+/\1xxxx/g

# isolation_ref2ref_foreign_keys
s/"(ref_table_[0-9]_|ref_table_[0-9]_value_fkey_)[0-9]+"/"\1xxxxxxx"/g

# Line info varies between versions
/^LINE [0-9]+:.*$/d
/^ *\^$/d

# pg12 changes
s/Partitioned table "/Table "/g
s/\) TABLESPACE pg_default$/\)/g
s/invalid input syntax for type /invalid input syntax for /g
s/_id_ref_id_fkey/_id_fkey/g
s/_ref_id_id_fkey_/_ref_id_fkey_/g
s/fk_test_2_col1_col2_fkey/fk_test_2_col1_fkey/g
s/_id_other_column_ref_fkey/_id_fkey/g
