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
s/Shard [0-9]+/Shard xxxxx/g
s/assigned task [0-9]+ to node/assigned task to node/
s/node group [12] (but|does)/node group \1/

# Differing names can have differing table column widths
s/^-[+-]{2,}$/---------------------------------------------------------------------/g

# In foreign_key_to_reference_table, normalize shard table names, etc in
# the generated plan
s/"(foreign_key_2_|fkey_ref_to_dist_|fkey_ref_|fkey_to_ref_)[0-9]+"/"\1xxxxxxx"/g
s/"(referenced_table_|referencing_table_|referencing_table2_)[0-9]+"/"\1xxxxxxx"/g
s/"(referencing_table_0_|referencing_table_4_|referenced_table2_)[0-9]+"/"\1xxxxxxx"/g
s/\(id\)=\([0-9]+\)/(id)=(X)/g
s/\(ref_id\)=\([0-9]+\)/(ref_id)=(X)/g

# shard table names for multi_subtransactions
s/"t2_[0-9]+"/"t2_xxxxxxx"/g

# shard table names for MERGE tests
s/merge_schema\.([_a-z0-9]+)_40[0-9]+ /merge_schema.\1_xxxxxxx /g
s/pgmerge_schema\.([_a-z0-9]+)_40[0-9]+ /pgmerge_schema.\1_xxxxxxx /g
s/merge_vcore_schema\.([_a-z0-9]+)_40[0-9]+ /pgmerge_schema.\1_xxxxxxx /g

# shard table names for multi_subquery
s/ keyval(1|2|ref)_[0-9]+ / keyval\1_xxxxxxx /g

# shard table names for custom_aggregate_support
s/ daily_uniques_[0-9]+ / daily_uniques_xxxxxxx /g

# shard table names for isolation_create_citus_local_table
s/"citus_local_table_([0-9]+)_[0-9]+"/"citus_local_table_\1_xxxxxxx"/g

# normalize relation oid suffix for the truncate triggers created by citus
s/truncate_trigger_[0-9]+/truncate_trigger_xxxxxxx/g

# shard move subscription and publication names contain the oid of the
# table owner, which can change across runs
s/(citus_shard_(move|split)_subscription_role_)[0-9]+_[0-9]+/\1xxxxxxx_xxxxxxx/g
s/(citus_shard_(move|split)_subscription_)[0-9]+_[0-9]+/\1xxxxxxx_xxxxxxx/g
s/(citus_shard_(move|split)_(slot|publication)_)[0-9]+_[0-9]+_[0-9]+/\1xxxxxxx_xxxxxxx_xxxxxxx/g

# In foreign_key_restriction_enforcement, normalize shard names
s/"(on_update_fkey_table_|fkey_)[0-9]+"/"\1xxxxxxx"/g

# In multi_insert_select_conflict, normalize shard name and constraints
s/"(target_table_|target_table_|test_ref_table_)[0-9]+"/"\1xxxxxxx"/g
s/\(col_1\)=\([0-9]+\)/(col_1)=(X)/g

# In multi_name_lengths, normalize shard names
s/name_len_12345678901234567890123456789012345678_fcd8ab6f_[0-9]+/name_len_12345678901234567890123456789012345678_fcd8ab6f_xxxxx/g

# ignore page split with pg13, and WAL warnings
# they can randomly appear when DEBUG logs are on
/DEBUG:  concurrent ROOT page split/d
/DEBUG: .+creating and filling new WAL file/d

# normalize debug connection failure
s/DEBUG:  connection to the remote node/WARNING:  connection to the remote node/g

# normalize file names for partitioned files
s/(task_[0-9]+\.)[0-9]+/\1xxxx/g
s/(job_[0-9]+\/task_[0-9]+\/p_[0-9]+\.)[0-9]+/\1xxxx/g

# isolation_ref2ref_foreign_keys
s/"(ref_table_[0-9]_|ref_table_[0-9]_value_fkey_)[0-9]+"/"\1xxxxxxx"/g

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

# pg13 changes
s/of relation ".*" violates not-null constraint/violates not-null constraint/g
/DEBUG:  index ".*" can safely use deduplication.*$/d
/DEBUG:  index ".*" cannot use deduplication.*$/d
/DEBUG:  building index ".*" on table ".*" serially.*$/d
s/partition ".*" would be violated by some row/partition would be violated by some row/g
s/of relation ".*" contains null values/contains null values/g

s/(Citus Background Task Queue Executor: regression\/postgres for \()[0-9]+\/[0-9]+\)/\1xxxxx\/xxxxx\)/g

# Changed outputs after minor bump to PG14.5 and PG13.8
s/(ERROR: |WARNING: |error:) invalid socket/\1 connection not open/g

# Extra outputs after minor bump to PG14.5 and PG13.8
/^\s*invalid socket$/d

# pg15 changes
s/ AS "\?column\?"//g
# We ignore multiline error messages, and substitute first line with a single line
# alternative that is used in some older libpq versions.
s/(ERROR: |WARNING: |error:) server closed the connection unexpectedly/\1 connection not open/g
/^\s*This probably means the server terminated abnormally$/d
/^\s*before or while processing the request.$/d
/^\s*connection not open$/d

# intermediate_results
s/(ERROR.*)pgsql_job_cache\/([0-9]+_[0-9]+_[0-9]+)\/(.*).data/\1pgsql_job_cache\/xx_x_xxx\/\3.data/g

# assign_distributed_transaction id params
s/(NOTICE.*)assign_distributed_transaction_id\([0-9]+, [0-9]+, '.*'\)/\1assign_distributed_transaction_id\(xx, xx, 'xxxxxxx'\)/g
s/(NOTICE.*)PREPARE TRANSACTION 'citus_[0-9]+_[0-9]+_[0-9]+_[0-9]+'/\1PREPARE TRANSACTION 'citus_xx_xx_xx_xx'/g
s/(NOTICE.*)COMMIT PREPARED 'citus_[0-9]+_[0-9]+_[0-9]+_[0-9]+'/\1COMMIT PREPARED 'citus_xx_xx_xx_xx'/g

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
# Plan numbers in merge into
s/read_intermediate_result\('merge_into_[0-9]+_/read_intermediate_result('merge_into_XXX_/g

# ignore job id in repartitioned insert/select
s/repartitioned_results_[0-9]+/repartitioned_results_xxxxx/g

# ignore job id in worker_hash_partition_table
s/worker_hash_partition_table  \([0-9]+/worker_hash_partition_table  \(xxxxxxx/g

# ignore referene table replication messages
/replicating reference table.*$/d

# ignore memory usage output
/.*Memory Usage:.*/d

# Warnings in multi_explain
s/prepared transaction with identifier .* does not exist/prepared transaction with identifier "citus_x_yyyyyy_zzz_w" does not exist/g
s/failed to roll back prepared transaction '.*'/failed to roll back prepared transaction 'citus_x_yyyyyy_zzz_w'/g

# Errors with binary decoding where OIDs should be normalized
s/wrong data type: [0-9]+, expected [0-9]+/wrong data type: XXXX, expected XXXX/g

# Errors with relation OID does not exist
s/relation with OID [0-9]+ does not exist/relation with OID XXXX does not exist/g

# ignore event triggers, mainly due to the event trigger for columnar
/^DEBUG:  EventTriggerInvoke [0-9]+$/d

# ignore DEBUG1 messages that Postgres generates
/^DEBUG:  rehashing catalog cache id .*$/d

# ignore JIT related messages
/^DEBUG:  probing availability of JIT.*/d
/^DEBUG:  provider not available, disabling JIT for current session.*/d
/^DEBUG:  time to inline:.*/d
/^DEBUG:  successfully loaded JIT.*/d

# ignore timing statistics for VACUUM VERBOSE
/CPU: user: .*s, system: .*s, elapsed: .*s/d

# normalize storage id of columnar tables
s/^storage id: [0-9]+$/storage id: xxxxx/g

# normalize notice messages in citus_local_tables
s/(NOTICE:  executing.*)citus_local_tables_test_schema.citus_local_table_4_[0-9]+(.*)/\1citus_local_tables_test_schema.citus_local_table_4_xxxx\2/g
s/(NOTICE:  executing.*)\([0-9]+, 'citus_local_tables_test_schema', [0-9]+(.*)/\1\(xxxxx, 'citus_local_tables_test_schema', xxxxx\2/g
s/citus_local_table_4_idx_[0-9]+/citus_local_table_4_idx_xxxxxx/g
s/citus_local_table_4_[0-9]+/citus_local_table_4_xxxxxx/g
s/ERROR:  cannot append to shardId [0-9]+/ERROR:  cannot append to shardId xxxxxx/g

# hide notice/hint message that we get when converting local tables automatically
/local tables that are added to metadata automatically by citus, but not chained with reference tables via foreign keys might be automatically converted back to postgres tables$/d
/Executing citus_add_local_table_to_metadata(.*) prevents this for the given relation, and all of the connected relations$/d

# normalize for distributed deadlock delay in isolation_metadata_sync_deadlock
# isolation tester first detects a lock, but then deadlock detector cancels the
# session. Sometimes happens that deadlock detector cancels the session before
# lock detection, so we normalize it by removing these two lines.
/^ <waiting ...>$/ {
    N; /\nstep s1-update-2: <... completed>$/ {
        s/.*//g
    }
}

# normalize long table shard name errors for alter_table_set_access_method and alter_distributed_table
s/^(ERROR:  child table is missing constraint "\w+)_([0-9])+"/\1_xxxxxx"/g
s/^(DEBUG:  the name of the shard \(abcde_01234567890123456789012345678901234567890_f7ff6612)_([0-9])+/\1_xxxxxx/g
s/^(ERROR:  cannot distribute relation: numeric_negative_scale)_([0-9]+)/\1_xxxxxx"/g

# normalize long index name errors for multi_index_statements
s/^(ERROR:  The index name \(test_index_creation1_p2020_09_26)_([0-9])+_(tenant_id_timeperiod_idx)/\1_xxxxxx_\3/g
s/^(DEBUG:  the index name on the shards of the partition is too long, switching to sequential and local execution mode to prevent self deadlocks: test_index_creation1_p2020_09_26)_([0-9])+_(tenant_id_timeperiod_idx)/\1_xxxxxx_\3/g

# normalize errors for not being able to connect to a non-existing host
s/could not translate host name "([A-Za-z0-9\.\-]+)" to address: .*$/could not translate host name "\1" to address: <system specific error>/g

# ignore PL/pgSQL line numbers that differ on Mac builds
s/(CONTEXT:  PL\/pgSQL function .* line )([0-9]+)/\1XX/g
s/^(PL\/pgSQL function .* line) [0-9]+ (.*)/\1 XX \2/g

# normalize a test difference in multi_move_mx
s/ connection to server at "\w+" \(127\.0\.0\.1\), port [0-9]+ failed://g

# normalize differences in tablespace of new index
s/pg14\.idx.*/pg14\.xxxxx/g
s/CREATE TABLESPACE test_tablespace LOCATION.*/CREATE TABLESPACE test_tablespace LOCATION XXXX/g

# columnar log for var correlation
s/(.*absolute correlation \()([0,1]\.[0-9]+)(\) of var attribute [0-9]+ is smaller than.*)/\1X\.YZ\3/g

# normalize differences in multi_fix_partition_shard_index_names test
s/NOTICE:  issuing WITH placement_data\(shardid, shardlength, groupid, placementid\)  AS \(VALUES \([0-9]+, [0-9]+, [0-9]+, [0-9]+\)\)/NOTICE:  issuing WITH placement_data\(shardid, shardlength, groupid, placementid\)  AS \(VALUES \(xxxxxx, xxxxxx, xxxxxx, xxxxxx\)\)/g

# global_pid when pg_cancel_backend is sent to workers
s/pg_cancel_backend\('[0-9]+'::bigint\)/pg_cancel_backend('xxxxx'::bigint)/g
s/issuing SELECT pg_cancel_backend\([0-9]+::integer\)/issuing SELECT pg_cancel_backend(xxxxx::integer)/g

# shard_rebalancer output for flaky nodeIds
s/issuing SELECT pg_catalog.citus_copy_shard_placement\(43[0-9]+,[0-9]+,[0-9]+,'block_writes'\)/issuing SELECT pg_catalog.citus_copy_shard_placement(43xxxx,xx,xx,'block_writes')/g

# node id in run_command_on_all_nodes warning
s/Error on node with node id [0-9]+/Error on node with node id xxxxx/g

# Temp schema names in error messages regarding dependencies that we cannot distribute
#
# 1) Schema of the depending object in the error message:
#
# e.g.:
#   WARNING:  "function pg_temp_3.f(bigint)" has dependency on unsupported object "<foo>"
# will be replaced with
#   WARNING:  "function pg_temp_xxx.f(bigint)" has dependency on unsupported object "<foo>"
s/^(WARNING|ERROR)(:  "[a-z\ ]+ )pg_temp_[0-9]+(\..*" has dependency on unsupported object ".*")$/\1\2pg_temp_xxx\3/g

# 2) Schema of the depending object in the error detail:
s/^(DETAIL:  "[a-z\ ]+ )pg_temp_[0-9]+(\..*" will be created only locally)$/\1pg_temp_xxx\2/g

# 3) Schema that the object depends in the error message:
# e.g.:
#   WARNING:  "function func(bigint)" has dependency on unsupported object "schema pg_temp_3"
# will be replaced with
#   WARNING:  "function func(bigint)" has dependency on unsupported object "schema pg_temp_xxx"
s/^(WARNING|ERROR)(:  "[a-z\ ]+ .*" has dependency on unsupported object) "schema pg_temp_[0-9]+"$/\1\2 "schema pg_temp_xxx"/g

# remove jobId's from the messages of the background rebalancer
s/^ERROR:  A rebalance is already running as job [0-9]+$/ERROR:  A rebalance is already running as job xxx/g
s/^NOTICE:  Scheduled ([0-9]+) moves as job [0-9]+$/NOTICE:  Scheduled \1 moves as job xxx/g
s/^HINT: (.*) job_id = [0-9]+ (.*)$/HINT: \1 job_id = xxx \2/g

# In clock tests, normalize epoch value(s) and the DEBUG messages printed
s/^(DEBUG:  |LOG:  )(coordinator|final global|Set) transaction clock [0-9]+.*$/\1\2 transaction clock xxxxxx/g
# Look for >= 13 digit logical value
s/^ (\(localhost,)([0-9]+)(,t,"\([1-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]+,[0-9]+\)")/\1 xxx,t,"(xxxxxxxxxxxxx,x)"/g
s/^ \([1-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]+,[0-9]+\)/ (xxxxxxxxxxxxx,x)/g
s/^(DEBUG:  |LOG:  )(node\([0-9]+\)) transaction clock [0-9]+.*$/\1node xxxx transaction clock xxxxxx/g
s/^(NOTICE:  )(clock).*LC:[0-9]+,.*C:[0-9]+,.*$/\1\2 xxxxxx/g
/^(DEBUG:  )(adjusted to remote clock: <logical)\([0-9]+\) counter\([0-9]+\)>$/d
/^DEBUG:  persisting transaction.*counter.*$/d
/^DEBUG:  both logical clock values are equal\([0-9]+\), pick remote.*$/d

# The following 2 lines are to normalize duration and cost in the EXPLAIN output
s/LOG:  duration: [0-9].[0-9]+ ms/LOG:  duration: xxxx ms/g
s/"Total Cost": [0-9].[0-9]+/"Total Cost": xxxx/g

# normalize gpids
s/(NOTICE:  issuing SET LOCAL application_name TO 'citus_rebalancer gpid=)[0-9]+/\1xxxxx/g

# shard_rebalancer output, flaky improvement number
s/improvement of 0.1[0-9]* is lower/improvement of 0.1xxxxx is lower/g
# normalize tenants statistics annotations
s/\/\*\{"cId":.*\*\///g

# Notice message that contains current columnar version that makes it harder to bump versions
s/(NOTICE:  issuing CREATE EXTENSION IF NOT EXISTS citus_columnar WITH SCHEMA  pg_catalog VERSION )"[0-9]+\.[0-9]+-[0-9]+"/\1 "x.y-z"/

# pg16 changes
# can be removed when dropping PG14&15 support
#if PG_VERSION_NUM < PG_VERSION_16
# (This is not preprocessor directive, but a reminder for the developer that will drop PG14&15 support )

s/, password_required=false//g
s/provide the file or change sslmode/provide the file, use the system's trusted roots with sslrootcert=system, or change sslmode/g
s/(:varcollid [0-9]+) :varlevelsup 0/\1 :varnullingrels (b) :varlevelsup 0/g
s/table_name_for_view\.([_a-z0-9]+)(,| |$)/\1\2/g
s/permission denied to terminate process/must be a superuser to terminate superuser process/g
s/permission denied to cancel query/must be a superuser to cancel superuser query/g

#endif /* PG_VERSION_NUM < PG_VERSION_16 */

# pg17 changes
# can be removed when dropping PG15&16 support
#if PG_VERSION_NUM < PG_VERSION_17
# (This is not preprocessor directive, but a reminder for the developer that will drop PG15&16 support )

s/COPY DEFAULT only available using COPY FROM/COPY DEFAULT cannot be used with COPY TO/
s/COPY delimiter must not appear in the DEFAULT specification/COPY delimiter character must not appear in the DEFAULT specification/

#endif /* PG_VERSION_NUM < PG_VERSION_17 */

# PG 17 Removes outer parentheses from CHECK constraints
# we add them back for pg15,pg16 compatibility
# e.g. change CHECK other_col >= 100 to CHECK (other_col >= 100)
s/\| CHECK ([a-zA-Z])(.*)/| CHECK \(\1\2\)/g

# pg17 change: this is a rule that ignores additional DEBUG logging
# for CREATE MATERIALIZED VIEW (commit b4da732fd64). This could be
# changed to a normalization rule when 17 becomes the minimum
# supported Postgres version.

/DEBUG:  drop auto-cascades to type [a-zA-Z_]*.pg_temp_[0-9]*/d
