/*-------------------------------------------------------------------------
 *
 * test/src/sequential_execution.c
 *
 * This file contains functions to test setting citus.multi_shard_modify_mode
 * GUC.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"

#include "distributed/shared_connection_stats.h"
#include "distributed/listutils.h"
#include "nodes/parsenodes.h"
#include "utils/guc.h"

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(wake_up_connection_pool_waiters);
PG_FUNCTION_INFO_V1(set_max_shared_pool_size);


/*
 * wake_up_waiters_backends is a SQL
 * interface for testing WakeupWaiterBackendsForSharedConnection().
 */
Datum
wake_up_connection_pool_waiters(PG_FUNCTION_ARGS)
{
	WakeupWaiterBackendsForSharedConnection();

	PG_RETURN_VOID();
}


/*
 * makeIntConst creates a Const Node that stores a given integer
 *
 * copied from backend/parser/gram.c
 */
static Node *
makeIntConst(int val, int location)
{
	A_Const *n = makeNode(A_Const);

#if PG_VERSION_NUM >= PG_VERSION_15
	n->val.ival.type = T_Integer;
	n->val.ival.ival = val;
#else
	n->val.type = T_Integer;
	n->val.val.ival = val;
#endif
	n->location = location;

	return (Node *) n;
}


/*
 * set_max_shared_pool_size is a SQL
 * interface for setting MaxSharedPoolSize. We use this function in isolation
 * tester where ALTER SYSTEM is not allowed.
 */
Datum
set_max_shared_pool_size(PG_FUNCTION_ARGS)
{
	int value = PG_GETARG_INT32(0);

	AlterSystemStmt *alterSystemStmt = palloc0(sizeof(AlterSystemStmt));

	A_Const *aConstValue = castNode(A_Const, makeIntConst(value, 0));

	alterSystemStmt->setstmt = makeNode(VariableSetStmt);
	alterSystemStmt->setstmt->name = "citus.max_shared_pool_size";
	alterSystemStmt->setstmt->is_local = false;
	alterSystemStmt->setstmt->kind = VAR_SET_VALUE;
	alterSystemStmt->setstmt->args = list_make1(aConstValue);

	AlterSystemSetConfigFile(alterSystemStmt);

	kill(PostmasterPid, SIGHUP);

	PG_RETURN_VOID();
}
