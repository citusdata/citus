/*-------------------------------------------------------------------------
 *
 * non_main_db_distribute_object_ops.c
 *
 *    Routines to support node-wide object management commands from non-main
 *    databases.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_authid_d.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_transaction.h"


#define EXECUTE_COMMAND_ON_REMOTE_NODES_AS_USER \
	"SELECT citus_internal.execute_command_on_remote_nodes_as_user(%s, %s)"
#define START_MANAGEMENT_TRANSACTION \
	"SELECT citus_internal.start_management_transaction('%lu')"
#define MARK_OBJECT_DISTRIBUTED \
	"SELECT citus_internal.mark_object_distributed(%d, %s, %d, %s)"
#define UNMARK_OBJECT_DISTRIBUTED \
	"SELECT pg_catalog.citus_unmark_object_distributed(%d, %d, %d,%s)"


/* see NonMainDbDistributedStatementInfo for the explanation of these flags */
typedef enum DistObjectOperation
{
	NO_DIST_OBJECT_OPERATION,
	MARK_DISTRIBUTED_GLOBALLY,
	UNMARK_DISTRIBUTED_LOCALLY
} DistObjectOperation;

/*
 * NonMainDbDistributedStatementInfo is used to determine whether a statement is
 * supported from non-main databases and whether it should be marked or unmarked
 * as distributed.
 *
 * When creating a distributed object, we always have to mark such objects as
 * "distributed" but while for some object types we can delegate this to main
 * database, for some others we have to explicitly send a command to all nodes
 * in this code-path to achieve this. Callers need to provide
 * MARK_DISTRIBUTED_GLOBALLY when that is the case.
 *
 * Similarly when dropping a distributed object, we always have to unmark such
 * objects as "distributed" and our utility hook on remote nodes achieve this
 * via UnmarkNodeWideObjectsDistributed() because the commands that we send to
 * workers are executed via main db. However for the local node, this is not the
 * case as we're not in the main db. For this reason, callers need to provide
 * UNMARK_DISTRIBUTED_LOCALLY to unmark an object for local node as well.
 */
typedef struct NonMainDbDistributedStatementInfo
{
	int statementType;
	DistObjectOperation distObjectOperation;

	/*
	 * checkSupportedObjectTypes is a callback function that checks whether
	 * type of the object referred to by given statement is supported.
	 *
	 * Can be NULL if not applicable for the statement type.
	 */
	bool (*checkSupportedObjectTypes)(Node *node);

	/* indicates whether the statement cannot be executed in a transaction */
	bool cannotBeExecutedInTransaction;
} NonMainDbDistributedStatementInfo;

/*
 * DistObjectOperationParams is used to pass parameters to the
 * MarkObjectDistributedGloballyFromNonMainDb function and
 * UnMarkObjectDistributedLocallyFromNonMainDb functions.
 */
typedef struct DistObjectOperationParams
{
	char *name;
	Oid id;
	uint16 catalogRelId;
} DistObjectOperationParams;


/*
 * checkSupportedObjectTypes callbacks for
 * NonMainDbDistributedStatementInfo objects.
 */
static bool NonMainDbCheckSupportedObjectTypeForCreateDatabase(Node *node);
static bool NonMainDbCheckSupportedObjectTypeForDropDatabase(Node *node);
static bool NonMainDbCheckSupportedObjectTypeForGrant(Node *node);
static bool NonMainDbCheckSupportedObjectTypeForSecLabel(Node *node);

/*
 * NonMainDbSupportedStatements is an array of statements that are supported
 * from non-main databases.
 */
ObjectType supportedObjectTypesForGrantStmt[] = { OBJECT_DATABASE };
static const NonMainDbDistributedStatementInfo NonMainDbSupportedStatements[] = {
	{ T_GrantRoleStmt, NO_DIST_OBJECT_OPERATION, NULL, false },
	{ T_CreateRoleStmt, MARK_DISTRIBUTED_GLOBALLY, NULL, false },
	{ T_DropRoleStmt, UNMARK_DISTRIBUTED_LOCALLY, NULL, false },
	{ T_AlterRoleStmt, NO_DIST_OBJECT_OPERATION, NULL, false },
	{ T_GrantStmt, NO_DIST_OBJECT_OPERATION,
	  NonMainDbCheckSupportedObjectTypeForGrant, false },
	{ T_CreatedbStmt, NO_DIST_OBJECT_OPERATION,
	  NonMainDbCheckSupportedObjectTypeForCreateDatabase, true },
	{ T_DropdbStmt, NO_DIST_OBJECT_OPERATION,
	  NonMainDbCheckSupportedObjectTypeForDropDatabase, true },
	{ T_SecLabelStmt, NO_DIST_OBJECT_OPERATION,
	  NonMainDbCheckSupportedObjectTypeForSecLabel, false },
};


static bool IsStatementSupportedFromNonMainDb(Node *parsetree);
static bool StatementRequiresMarkDistributedGloballyFromNonMainDb(Node *parsetree);
static bool StatementRequiresUnmarkDistributedLocallyFromNonMainDb(Node *parsetree);
static bool StatementCannotBeExecutedInTransaction(Node *parsetree);
static void MarkObjectDistributedGloballyFromNonMainDb(Node *parsetree);
static void UnMarkObjectDistributedLocallyFromNonMainDb(List *unmarkDistributedList);
static List * GetDistObjectOperationParams(Node *parsetree);


/*
 * RunPreprocessNonMainDBCommand runs the necessary commands for a query, in main
 * database before query is run on the local node with PrevProcessUtility.
 *
 * Returns true if previous utility hook needs to be skipped after completing
 * preprocess phase.
 */
bool
RunPreprocessNonMainDBCommand(Node *parsetree)
{
	if (IsMainDB || !IsStatementSupportedFromNonMainDb(parsetree))
	{
		return false;
	}

	char *queryString = DeparseTreeNode(parsetree);

	/*
	 * For the commands that cannot be executed in a transaction, there are no
	 * transactional visibility issues. We directly route them to main database
	 * so that we only have to consider one code-path when creating databases.
	 */
	if (StatementCannotBeExecutedInTransaction(parsetree))
	{
		IsMainDBCommandInXact = false;
		RunCitusMainDBQuery((char *) queryString);
		return true;
	}

	IsMainDBCommandInXact = true;

	StringInfo mainDBQuery = makeStringInfo();
	appendStringInfo(mainDBQuery,
					 START_MANAGEMENT_TRANSACTION,
					 GetCurrentFullTransactionId().value);
	RunCitusMainDBQuery(mainDBQuery->data);

	mainDBQuery = makeStringInfo();
	appendStringInfo(mainDBQuery,
					 EXECUTE_COMMAND_ON_REMOTE_NODES_AS_USER,
					 quote_literal_cstr(queryString),
					 quote_literal_cstr(CurrentUserName()));
	RunCitusMainDBQuery(mainDBQuery->data);

	if (StatementRequiresUnmarkDistributedLocallyFromNonMainDb(parsetree))
	{
		List *unmarkParams = GetDistObjectOperationParams(parsetree);
		UnMarkObjectDistributedLocallyFromNonMainDb(unmarkParams);
	}

	return false;
}


/*
 * RunPostprocessNonMainDBCommand runs the necessary commands for a query, in main
 * database after query is run on the local node with PrevProcessUtility.
 */
void
RunPostprocessNonMainDBCommand(Node *parsetree)
{
	if (IsMainDB || !IsStatementSupportedFromNonMainDb(parsetree))
	{
		return;
	}

	if (StatementRequiresMarkDistributedGloballyFromNonMainDb(parsetree))
	{
		MarkObjectDistributedGloballyFromNonMainDb(parsetree);
	}
}


/*
 * IsStatementSupportedFromNonMainDb returns true if the statement is supported from a
 * non-main database.
 */
static bool
IsStatementSupportedFromNonMainDb(Node *parsetree)
{
	NodeTag type = nodeTag(parsetree);

	for (int i = 0; i < sizeof(NonMainDbSupportedStatements) /
		 sizeof(NonMainDbSupportedStatements[0]); i++)
	{
		if (type != NonMainDbSupportedStatements[i].statementType)
		{
			continue;
		}

		return !NonMainDbSupportedStatements[i].checkSupportedObjectTypes ||
			   NonMainDbSupportedStatements[i].checkSupportedObjectTypes(parsetree);
	}

	return false;
}


/*
 * StatementRequiresMarkDistributedGloballyFromNonMainDb returns true if the statement should be marked
 * as distributed when executed from a non-main database.
 */
static bool
StatementRequiresMarkDistributedGloballyFromNonMainDb(Node *parsetree)
{
	NodeTag type = nodeTag(parsetree);

	for (int i = 0; i < sizeof(NonMainDbSupportedStatements) /
		 sizeof(NonMainDbSupportedStatements[0]); i++)
	{
		if (type == NonMainDbSupportedStatements[i].statementType)
		{
			return NonMainDbSupportedStatements[i].distObjectOperation ==
				   MARK_DISTRIBUTED_GLOBALLY;
		}
	}

	return false;
}


/*
 * StatementRequiresUnmarkDistributedLocallyFromNonMainDb returns true if the statement should be unmarked
 * as distributed when executed from a non-main database.
 */
static bool
StatementRequiresUnmarkDistributedLocallyFromNonMainDb(Node *parsetree)
{
	NodeTag type = nodeTag(parsetree);

	for (int i = 0; i < sizeof(NonMainDbSupportedStatements) /
		 sizeof(NonMainDbSupportedStatements[0]); i++)
	{
		if (type == NonMainDbSupportedStatements[i].statementType)
		{
			return NonMainDbSupportedStatements[i].distObjectOperation ==
				   UNMARK_DISTRIBUTED_LOCALLY;
		}
	}

	return false;
}


/*
 * StatementCannotBeExecutedInTransaction returns true if the statement cannot be executed in a
 * transaction.
 */
static bool
StatementCannotBeExecutedInTransaction(Node *parsetree)
{
	NodeTag type = nodeTag(parsetree);

	for (int i = 0; i < sizeof(NonMainDbSupportedStatements) /
		 sizeof(NonMainDbSupportedStatements[0]); i++)
	{
		if (type == NonMainDbSupportedStatements[i].statementType)
		{
			return NonMainDbSupportedStatements[i].cannotBeExecutedInTransaction;
		}
	}

	return false;
}


/*
 * MarkObjectDistributedGloballyFromNonMainDb marks the given object as distributed on the
 * non-main database.
 */
static void
MarkObjectDistributedGloballyFromNonMainDb(Node *parsetree)
{
	List *distObjectOperationParams =
		GetDistObjectOperationParams(parsetree);

	DistObjectOperationParams *distObjectOperationParam = NULL;

	foreach_ptr(distObjectOperationParam, distObjectOperationParams)
	{
		StringInfo mainDBQuery = makeStringInfo();
		appendStringInfo(mainDBQuery,
						 MARK_OBJECT_DISTRIBUTED,
						 distObjectOperationParam->catalogRelId,
						 quote_literal_cstr(distObjectOperationParam->name),
						 distObjectOperationParam->id,
						 quote_literal_cstr(CurrentUserName()));
		RunCitusMainDBQuery(mainDBQuery->data);
	}
}


/*
 * UnMarkObjectDistributedLocallyFromNonMainDb unmarks the given object as distributed on the
 * non-main database.
 */
static void
UnMarkObjectDistributedLocallyFromNonMainDb(List *markObjectDistributedParamList)
{
	DistObjectOperationParams *markObjectDistributedParam = NULL;
	int subObjectId = 0;
	char *checkObjectExistence = "false";
	foreach_ptr(markObjectDistributedParam, markObjectDistributedParamList)
	{
		StringInfo query = makeStringInfo();
		appendStringInfo(query,
						 UNMARK_OBJECT_DISTRIBUTED,
						 AuthIdRelationId,
						 markObjectDistributedParam->id,
						 subObjectId, checkObjectExistence);
		RunCitusMainDBQuery(query->data);
	}
}


/*
 * GetDistObjectOperationParams returns DistObjectOperationParams for the target
 * object of given parsetree.
 */
List *
GetDistObjectOperationParams(Node *parsetree)
{
	List *paramsList = NIL;
	if (IsA(parsetree, CreateRoleStmt))
	{
		CreateRoleStmt *stmt = castNode(CreateRoleStmt, parsetree);
		DistObjectOperationParams *params =
			(DistObjectOperationParams *) palloc(sizeof(DistObjectOperationParams));
		params->name = stmt->role;
		params->catalogRelId = AuthIdRelationId;
		params->id = get_role_oid(stmt->role, false);

		paramsList = lappend(paramsList, params);
	}
	else if (IsA(parsetree, DropRoleStmt))
	{
		DropRoleStmt *stmt = castNode(DropRoleStmt, parsetree);
		RoleSpec *roleSpec;
		foreach_ptr(roleSpec, stmt->roles)
		{
			DistObjectOperationParams *params = (DistObjectOperationParams *) palloc(
				sizeof(DistObjectOperationParams));

			Oid roleOid = get_role_oid(roleSpec->rolename, true);

			if (roleOid == InvalidOid)
			{
				continue;
			}

			params->id = roleOid;
			params->name = roleSpec->rolename;
			params->catalogRelId = AuthIdRelationId;

			paramsList = lappend(paramsList, params);
		}
	}
	else
	{
		elog(ERROR, "unsupported statement type");
	}

	return paramsList;
}


/*
 * NonMainDbCheckSupportedObjectTypeForCreateDatabase implements checkSupportedObjectTypes
 * callback for CreatedbStmt.
 *
 * We don't try to send the query to the main database if the CREATE DATABASE
 * command is for the main database itself, this is a very rare case but it's
 * exercised by our test suite.
 */
static bool
NonMainDbCheckSupportedObjectTypeForCreateDatabase(Node *node)
{
	CreatedbStmt *stmt = castNode(CreatedbStmt, node);
	return strcmp(stmt->dbname, MainDb) != 0;
}


/*
 * NonMainDbCheckSupportedObjectTypeForDropDatabase implements checkSupportedObjectTypes
 * callback for DropdbStmt.
 *
 * We don't try to send the query to the main database if the DROP DATABASE
 * command is for the main database itself, this is a very rare case but it's
 * exercised by our test suite.
 */
static bool
NonMainDbCheckSupportedObjectTypeForDropDatabase(Node *node)
{
	DropdbStmt *stmt = castNode(DropdbStmt, node);
	return strcmp(stmt->dbname, MainDb) != 0;
}


/*
 * NonMainDbCheckSupportedObjectTypeForGrant implements checkSupportedObjectTypes
 * callback for GrantStmt.
 */
static bool
NonMainDbCheckSupportedObjectTypeForGrant(Node *node)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	return stmt->objtype == OBJECT_DATABASE;
}


/*
 * NonMainDbCheckSupportedObjectTypeForSecLabel implements checkSupportedObjectTypes
 * callback for SecLabel.
 */
static bool
NonMainDbCheckSupportedObjectTypeForSecLabel(Node *node)
{
	SecLabelStmt *stmt = castNode(SecLabelStmt, node);
	return stmt->objtype == OBJECT_ROLE;
}
