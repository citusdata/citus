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
	DistObjectOperation DistObjectOperation;

	/*
	 * checkSupportedObjectTypes is a callback function that checks whether
	 * type of the object referred to by given statement is supported.
	 *
	 * Can be NULL if not applicable for the statement type.
	 */
	bool (*checkSupportedObjectTypes)(Node *node);
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
static bool NonMainDbCheckSupportedObjectTypeForGrant(Node *node);
static bool NonMainDbCheckSupportedObjectTypeForSecLabel(Node *node);

/*
 * NonMainDbSupportedStatements is an array of statements that are supported
 * from non-main databases.
 */
ObjectType supportedObjectTypesForGrantStmt[] = { OBJECT_DATABASE };
static const NonMainDbDistributedStatementInfo NonMainDbSupportedStatements[] = {
	{ T_GrantRoleStmt, NO_DIST_OBJECT_OPERATION, NULL },
	{ T_CreateRoleStmt, MARK_DISTRIBUTED_GLOBALLY, NULL },
	{ T_DropRoleStmt, UNMARK_DISTRIBUTED_LOCALLY, NULL },
	{ T_AlterRoleStmt, NO_DIST_OBJECT_OPERATION, NULL },
	{ T_GrantStmt, NO_DIST_OBJECT_OPERATION, NonMainDbCheckSupportedObjectTypeForGrant },
	{ T_CreatedbStmt, NO_DIST_OBJECT_OPERATION, NULL },
	{ T_DropdbStmt, NO_DIST_OBJECT_OPERATION, NULL },
	{ T_SecLabelStmt, NO_DIST_OBJECT_OPERATION,
	  NonMainDbCheckSupportedObjectTypeForSecLabel },
};


static bool IsStatementSupportedFromNonMainDb(Node *parsetree);
static bool StatementRequiresMarkDistributedGloballyFromNonMainDb(Node *parsetree);
static bool StatementRequiresUnmarkDistributedLocallyFromNonMainDb(Node *parsetree);
static void MarkObjectDistributedGloballyFromNonMainDb(Node *parsetree);
static void UnMarkObjectDistributedLocallyFromNonMainDb(List *unmarkDistributedList);
static List * GetDistObjectOperationParams(Node *parsetree);


/*
 * IsCommandToCreateOrDropMainDB checks if this query creates or drops the
 * main database, so we can make an exception and not send this query to
 * the main database.
 */
bool
IsCommandToCreateOrDropMainDB(Node *parsetree)
{
	if (IsA(parsetree, CreatedbStmt))
	{
		CreatedbStmt *createdbStmt = castNode(CreatedbStmt, parsetree);
		return strcmp(createdbStmt->dbname, MainDb) == 0;
	}
	else if (IsA(parsetree, DropdbStmt))
	{
		DropdbStmt *dropdbStmt = castNode(DropdbStmt, parsetree);
		return strcmp(dropdbStmt->dbname, MainDb) == 0;
	}

	return false;
}


/*
 * RunPreprocessMainDBCommand runs the necessary commands for a query, in main
 * database before query is run on the local node with PrevProcessUtility
 */
void
RunPreprocessMainDBCommand(Node *parsetree)
{
	if (!IsStatementSupportedFromNonMainDb(parsetree))
	{
		return;
	}

	char *queryString = DeparseTreeNode(parsetree);

	if (IsA(parsetree, CreatedbStmt) ||
		IsA(parsetree, DropdbStmt))
	{
		IsMainDBCommandInXact = false;
		RunCitusMainDBQuery((char *) queryString);
		return;
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
}


/*
 * RunPostprocessMainDBCommand runs the necessary commands for a query, in main
 * database after query is run on the local node with PrevProcessUtility
 */
void
RunPostprocessMainDBCommand(Node *parsetree)
{
	if (IsStatementSupportedFromNonMainDb(parsetree) &&
		StatementRequiresMarkDistributedGloballyFromNonMainDb(parsetree))
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
			return NonMainDbSupportedStatements[i].DistObjectOperation ==
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
			return NonMainDbSupportedStatements[i].DistObjectOperation ==
				   UNMARK_DISTRIBUTED_LOCALLY;
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
