/*-------------------------------------------------------------------------
 *
 * test/include/test_helper_functions.h
 *
 * Declarations for public functions and types related to unit testing
 * functionality.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_TEST_HELPER_FUNCTIONS_H
#define CITUS_TEST_HELPER_FUNCTIONS_H

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "libpq-fe.h"

#include "utils/array.h"


/* SQL statements for testing */
#define POPULATE_TEMP_TABLE "CREATE TEMPORARY TABLE numbers " \
							"AS SELECT * FROM generate_series(1, 100);"
#define COUNT_TEMP_TABLE "SELECT COUNT(*) FROM numbers;"


/* function declarations for generic test functions */
extern ArrayType * DatumArrayToArrayType(Datum *datumArray, int datumCount,
										 Oid datumTypeId);
extern void SetConnectionStatus(PGconn *connection, ConnStatusType status);

/* fake FDW for use in tests */
extern Datum fake_fdw_handler(PG_FUNCTION_ARGS);

/* function declarations for exercising connection functions */
extern Datum initialize_remote_temp_table(PG_FUNCTION_ARGS);
extern Datum count_remote_temp_table_rows(PG_FUNCTION_ARGS);
extern Datum get_and_purge_connection(PG_FUNCTION_ARGS);
extern Datum connect_and_purge_connection(PG_FUNCTION_ARGS);
extern Datum set_connection_status_bad(PG_FUNCTION_ARGS);

/* function declarations for exercising metadata functions */
extern Datum load_shard_id_array(PG_FUNCTION_ARGS);
extern Datum load_shard_interval_array(PG_FUNCTION_ARGS);
extern Datum load_shard_placement_array(PG_FUNCTION_ARGS);
extern Datum partition_column_id(PG_FUNCTION_ARGS);
extern Datum partition_type(PG_FUNCTION_ARGS);
extern Datum is_distributed_table(PG_FUNCTION_ARGS);
extern Datum distributed_tables_exist(PG_FUNCTION_ARGS);
extern Datum column_name_to_column(PG_FUNCTION_ARGS);
extern Datum column_name_to_column_id(PG_FUNCTION_ARGS);
extern Datum create_monolithic_shard_row(PG_FUNCTION_ARGS);
extern Datum next_shard_id(PG_FUNCTION_ARGS);
extern Datum acquire_shared_shard_lock(PG_FUNCTION_ARGS);

/* function declarations for exercising ddl generation functions */
extern Datum table_ddl_command_array(PG_FUNCTION_ARGS);

/* function declarations for exercising shard creation functions */
extern Datum sort_names(PG_FUNCTION_ARGS);

/* function declarations for exercising shard pruning functions */
extern Datum prune_using_no_values(PG_FUNCTION_ARGS);
extern Datum prune_using_single_value(PG_FUNCTION_ARGS);
extern Datum prune_using_either_value(PG_FUNCTION_ARGS);
extern Datum prune_using_both_values(PG_FUNCTION_ARGS);
extern Datum debug_equality_expression(PG_FUNCTION_ARGS);


#endif /* CITUS_TEST_HELPER_FUNCTIONS_H */
