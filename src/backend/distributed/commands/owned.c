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

#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/scansup.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/version_compat.h"
#include "distributed/worker_transaction.h"


static ObjectAddress * GetNewRoleAddress(ReassignOwnedStmt *stmt);

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

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
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


/*
 * PostprocessReassignOwnedStmt takes a Node pointer representing a REASSIGN
 * OWNED statement and performs any necessary post-processing after the statement
 * has been executed locally.
 *
 * We filter out local roles in OWNED BY clause before deparsing the command,
 * meaning that we skip reassigning what is owned by local roles. However,
 * if the role specified in TO clause is local, we automatically distribute
 * it before deparsing the command.
 */
List *
PostprocessReassignOwnedStmt(Node *node, const char *queryString)
{
	ReassignOwnedStmt *stmt = castNode(ReassignOwnedStmt, node);
	List *allReassignRoles = stmt->roles;

	List *distributedReassignRoles = FilterDistributedRoles(allReassignRoles);

	if (list_length(distributedReassignRoles) <= 0)
	{
		return NIL;
	}

	if (!ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	stmt->roles = distributedReassignRoles;
	char *sql = DeparseTreeNode((Node *) stmt);
	stmt->roles = allReassignRoles;

	ObjectAddress *newRoleAddress = GetNewRoleAddress(stmt);

	/*
	 * We temporarily enable create / alter role propagation to properly
	 * propagate the role specified in TO clause.
	 */
	int saveNestLevel = NewGUCNestLevel();
	set_config_option("citus.enable_create_role_propagation", "on",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
	set_config_option("citus.enable_alter_role_propagation", "on",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);

	set_config_option("citus.enable_alter_role_set_propagation", "on",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);

	EnsureObjectAndDependenciesExistOnAllNodes(newRoleAddress);

	/* rollback GUCs to the state before this session */
	AtEOXact_GUC(true, saveNestLevel);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * GetNewRoleAddress returns the ObjectAddress of the new role
 */
static ObjectAddress *
GetNewRoleAddress(ReassignOwnedStmt *stmt)
{
	Oid roleOid = get_role_oid(stmt->newrole->rolename, false);
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, AuthIdRelationId, roleOid);
	return address;
}
