/*-------------------------------------------------------------------------
 *
 * owned.c
 *    Commands for DROP OWNED statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/genam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_type.h"
#include "catalog/objectaddress.h"
#include "commands/dbcommands.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/version_compat.h"
#include "distributed/worker_transaction.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/scansup.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/varlena.h"
#include "utils/syscache.h"

/*
 * PreprocessDropOwnedStmt finds the distributed role out of the ones
 * being dropped and unmarks them distributed and creates the drop statements
 * for the workers.
 */
List *
PreprocessDropOwnedStmt(Node *node, const char *queryString,
						ProcessUtilityContext processUtilityContext)
{
	DropOwnedStmt *stmt = castNode(DropOwnedStmt, node);
	List *allDropRoles = stmt->roles;

	List *distributedDropRoles = FilterDistributedRoles(allDropRoles);
	if (list_length(distributedDropRoles) <= 0)
	{
		return NIL;
	}

	if (!ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	stmt->roles = distributedDropRoles;
	char *sql = DeparseTreeNode((Node *) stmt);
	stmt->roles = allDropRoles;

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}
