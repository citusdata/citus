# Rules to normalize test outputs. Our custom diff tool passes test output
# of tests through the substitution rules in this file before doing the
# actual comparison.
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

# discard "USING heap" in "CREATE TABLE ... USING heap"
s/CREATE(.*)TABLE(.*)USING heap/CREATE\1TABLE\2/g

# Differing names can have differing table column widths
s/^-[+-]{2,}$/---------------------------------------------------------------------/g

# In foreign_key_to_reference_table, normalize shard table names, etc in
# the generated plan
s/"(foreign_key_2_|fkey_ref_to_dist_|fkey_ref_)[0-9]+"/"\1xxxxxxx"/g
s/"(referenced_table_|referencing_table_|referencing_table2_)[0-9]+"/"\1xxxxxxx"/g
s/"(referencing_table_0_|referenced_table2_)[0-9]+"/"\1xxxxxxx"/g
s/\(id\)=\([0-9]+\)/(id)=(X)/g
s/\(ref_id\)=\([0-9]+\)/(ref_id)=(X)/g

# shard table names for multi_subtransactions
s/"t2_[0-9]+"/"t2_xxxxxxx"/g

# shard table names for multi_subquery
s/ keyval(1|2|ref)_[0-9]+ / keyval\1_xxxxxxx /g

# shard table names for custom_aggregate_support
s/ daily_uniques_[0-9]+ / daily_uniques_xxxxxxx /g

# shard table names for isolation_create_citus_local_table
s/"citus_local_table_([0-9]+)_[0-9]+"/"citus_local_table_\1_xxxxxxx"/g

# normalize relation oid suffix for the truncate triggers created by citus
s/truncate_trigger_[0-9]+/truncate_trigger_xxxxxxx/g

# (citus_table_triggers.sql)
# postgres generates create trigger commands for triggers with:
# "EXECUTE FUNCTION" in pg12
# "EXECUTE PROCEDURE" in pg11
s/FOR EACH (ROW|STATEMENT)(.*)EXECUTE PROCEDURE/FOR EACH \1\2EXECUTE FUNCTION/g

# In foreign_key_restriction_enforcement, normalize shard names
s/"(on_update_fkey_table_|fkey_)[0-9]+"/"\1xxxxxxx"/g

# In multi_insert_select_conflict, normalize shard name and constraints
s/"(target_table_|target_table_|test_ref_table_)[0-9]+"/"\1xxxxxxx"/g
s/\(col_1\)=\([0-9]+\)/(col_1)=(X)/g

# In multi_name_lengths, normalize shard names
s/name_len_12345678901234567890123456789012345678_fcd8ab6f_[0-9]+/name_len_12345678901234567890123456789012345678_fcd8ab6f_xxxxx/g

# normalize pkey constraints in multi_insert_select.sql
s/"(raw_events_second_user_id_value_1_key_|agg_events_user_id_value_1_agg_key_)[0-9]+"/"\1xxxxxxx"/g

# ignore could not consume warnings
/WARNING:  could not consume data from worker node/d

# ignore page split with pg13
/DEBUG:  concurrent ROOT page split/d

# ignore WAL warnings
/DEBUG: .+creating and filling new WAL file/d

# normalize file names for partitioned files
s/(task_[0-9]+\.)[0-9]+/\1xxxx/g
s/(job_[0-9]+\/task_[0-9]+\/p_[0-9]+\.)[0-9]+/\1xxxx/g

# isolation_ref2ref_foreign_keys
s/"(ref_table_[0-9]_|ref_table_[0-9]_value_fkey_)[0-9]+"/"\1xxxxxxx"/g

# pg11/pg12 varies in isolation debug output
s/s1: DEBUG:/DEBUG:/g

# commands cascading to shard relations
s/(NOTICE:  .*_)[0-9]{5,}( CASCADE)/\1xxxxx\2/g
s/(NOTICE:  [a-z]+ cascades to table ".*)_[0-9]{5,}"/\1_xxxxx"/g

# Line info varies between versions
/^LINE [0-9]+:.*$/d
/^ *\^$/d

# connection id
s/connectionId: [0-9]+/connectionId: xxxxxxx/g

# Remove trailing whitespace
s/ *$//g

# pg12 changes
s/Partitioned table "/Table "/g
s/\) TABLESPACE pg_default$/\)/g
s/invalid input syntax for type /invalid input syntax for /g
s/_id_ref_id_fkey/_id_fkey/g
s/_ref_id_id_fkey_/_ref_id_fkey_/g
s/fk_test_2_col1_col2_fkey/fk_test_2_col1_fkey/g
s/_id_other_column_ref_fkey/_id_fkey/g
s/"(collections_list_|collection_users_|collection_users_fkey_)[0-9]+"/"\1xxxxxxx"/g

# pg13 changes
s/of relation ".*" violates not-null constraint/violates not-null constraint/g
s/varnosyn/varnoold/g
s/varattnosyn/varoattno/g
/DEBUG:  index ".*" can safely use deduplication.*$/d
/DEBUG:  index ".*" cannot use deduplication.*$/d
/DEBUG:  building index ".*" on table ".*" serially.*$/d
s/partition ".*" would be violated by some row/partition would be violated by some row/g
/.*Peak Memory Usage:.*$/d
s/of relation ".*" contains null values/contains null values/g
s/of relation "t1" is violated by some row/is violated by some row/g
# can be removed when we remove PG_VERSION_NUM >= 120000
s/(.*)Output:.*$/\1Output: xxxxxx/g


# intermediate_results
s/(ERROR.*)pgsql_job_cache\/([0-9]+_[0-9]+_[0-9]+)\/(.*).data/\1pgsql_job_cache\/xx_x_xxx\/\3.data/g

# assign_distributed_transaction id params
s/(NOTICE.*)assign_distributed_transaction_id\([0-9]+, [0-9]+, '.*'\)/\1assign_distributed_transaction_id\(xx, xx, 'xxxxxxx'\)/g

# toast tables
s/pg_toast_[0-9]+/pg_toast_xxxxx/g

# Plan numbers are not very stable, so we normalize those
# subplan numbers are quite stable so we keep those
s/DEBUG:  Plan [0-9]+/DEBUG:  Plan XXX/g
s/generating subplan [0-9]+\_/generating subplan XXX\_/g
s/read_intermediate_result\('[0-9]+_/read_intermediate_result('XXX_/g
s/Subplan [0-9]+\_/Subplan XXX\_/g

# Plan numbers in insert select
s/read_intermediate_result\('insert_select_[0-9]+_/read_intermediate_result('insert_select_XXX_/g

# ignore job id in repartitioned insert/select
s/repartitioned_results_[0-9]+/repartitioned_results_xxxxx/g

# ignore job id in worker_hash_partition_table
s/worker_hash_partition_table  \([0-9]+/worker_hash_partition_table  \(xxxxxxx/g

# ignore first parameter for citus_extradata_container due to differences between pg11 and pg12
# can be removed when we remove PG_VERSION_NUM >= 120000
s/pg_catalog.citus_extradata_container\([0-9]+/pg_catalog.citus_extradata_container\(XXX/g

# ignore referene table replication messages
/replicating reference table.*$/d

# ignore memory usage output
/.*Memory Usage:.*/d

s/Citus.*currently supports/Citus currently supports/g

# Warnings in multi_explain
s/prepared transaction with identifier .* does not exist/prepared transaction with identifier "citus_x_yyyyyy_zzz_w" does not exist/g
s/failed to roll back prepared transaction '.*'/failed to roll back prepared transaction 'citus_x_yyyyyy_zzz_w'/g

# Table aliases for partitioned tables in explain outputs might change
# regardless of postgres appended an _int suffix to alias, we always append _xxx suffix
# Can be removed when we remove support for pg11 and pg12.
# "->  <scanMethod> Scan on <tableName>_<partitionId>_<shardId> <tableName>_<aliasId>" and
# "->  <scanMethod> Scan on <tableName>_<partitionId>_<shardId> <tableName>" becomes
# "->  <scanMethod> Scan on <tableName>_<partitionId>_<shardId> <tableName>_xxx"
s/(->.*Scan on\ +)(.*)(_[0-9]+)(_[0-9]+) \2(_[0-9]+|_xxx)?/\1\2\3\4 \2_xxx/g

# Table aliases for partitioned tables in "Hash Cond:" lines of explain outputs might change
# This is only for multi_partitioning.sql test file
# regardless of postgres appended an _int suffix to alias, we always append _xxx suffix
# Can be removed when we remove support for pg11 and pg12.
s/(partitioning_hash_join_test)(_[0-9]|_xxx)?(\.[a-zA-Z]+)/\1_xxx\3/g
s/(partitioning_hash_test)(_[0-9]|_xxx)?(\.[a-zA-Z]+)/\1_xxx\3/g

# multi_foreign_key_relation_graph.out
# suppress shardId's in drop constraint logs as the order of cascaded objects might be flaky
# e.g: drop cascades to constraint fkey_6_3000081 on table fkey_graph.distributed_table_1_3000081
s/(drop cascades to constraint fkey_[0-9]+_)([0-9]+)( on table fkey_graph\..+_)\2/\1xxx\3xxx/g

# Errors with binary decoding where OIDs should be normalized
s/wrong data type: [0-9]+, expected [0-9]+/wrong data type: XXXX, expected XXXX/g

# Errors with relation OID does not exist
s/relation with OID [0-9]+ does not exist/relation with OID XXXX does not exist/g

# ignore event triggers, mainly due to the event trigger for columnar
/^DEBUG:  EventTriggerInvoke [0-9]+$/d

# ignore DEBUG1 messages that Postgres generates
/^DEBUG:  rehashing catalog cache id [0-9]+$/d

# ignore timing statistics for VACUUM VERBOSE
/CPU: user: .*s, system: .*s, elapsed: .*s/d

# normalize storage id of columnar tables
s/^storage id: [0-9]+$/storage id: xxxxx/g
