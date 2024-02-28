/*-------------------------------------------------------------------------
 *
 * non_main_db_distribute_object_ops.c
 *
 *    Routines to support node-wide object management commands from non-main
 *    databases.
 *
 *    RunPreprocessNonMainDBCommand and RunPostprocessNonMainDBCommand are
 *    the entrypoints for this module. These functions are called from
 *    utility_hook.c to support some of the node-wide object management
 *    commands from non-main databases.
 *
 *    To add support for a new command type, one needs to define a new
 *    NonMainDbDistributeObjectOps object and add it to
 *    GetNonMainDbDistributeObjectOps.
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
	"SELECT pg_catalog.citus_unmark_object_distributed(%d, %d, %d, %s)"


/*
 * Structs used to implement getMarkDistributedParams and
 * getUnmarkDistributedParams callbacks for NonMainDbDistributeObjectOps.
 *
 * Main difference between these two is that while
 * MarkDistributedGloballyParams contains the name of the object, the other
 * doesn't. This is because, when marking an object -that is created from a
 * non-main db- as distributed, citus_internal.mark_object_distributed()
 * cannot find its name since the object is not visible to outer transaction
 * (or, read as "the transaction in non-main db").
 */
typedef struct MarkDistributedGloballyParams
{
	char *name;
	Oid id;
	uint16 catalogRelId;
} MarkDistributedGloballyParams;

typedef struct UnmarkDistributedLocallyParams
{
	Oid id;
	uint16 catalogRelId;
} UnmarkDistributedLocallyParams;

/*
 * NonMainDbDistributeObjectOps contains the necessary callbacks / flags to
 * support node-wide object management commands from non-main databases.
 *
 * getMarkDistributedParams / getUnmarkDistributedParams:
 *   When creating a distributed object, we always have to mark such objects
 *   as "distributed" but while for some object types we can delegate this to
 *   main database, for some others we have to explicitly send a command to
 *   all nodes in this code-path to achieve this. Callers need to implement
 *   getMarkDistributedParams when that is the case.
 *
 *   Similarly when dropping a distributed object, we always have to unmark
 *   the target object as "distributed" and our utility hook on remote nodes
 *   achieve this via UnmarkNodeWideObjectsDistributed() because the commands
 *   that we send to workers are executed via main db. However for the local
 *   node, this is not the case as we're not in the main db. For this reason,
 *   callers need to implement getUnmarkDistributedParams to unmark an object
 *   for local node as well.
 *
 *   We don't expect both of these to be NULL at the same time. However, it's
 *   okay if both of these are NULL.
 *
 *   Finally, while getMarkDistributedParams is expected to return "a list of
 *   objects", this is not the case for getUnmarkDistributedParams. This is
 *   because, while we expect that a drop command might drop multiple objects
 *   at once, we don't expect a create command to create multiple objects at
 *   once.
 *
 *  cannotBeExecutedInTransaction:
 *   Indicates whether the statement cannot be executed in a transaction. If
 *   this is set to true, the statement will be executed directly on the main
 *   database because there are no transactional visibility issues for such
 *   commands.
 *
 *   And when this is true, we don't expect getMarkDistributedParams and
 *   getUnmarkDistributedParams to be implemented.
 */
typedef struct NonMainDbDistributeObjectOps
{
	MarkDistributedGloballyParams * (*getMarkDistributedParams)(Node * parsetree);
	List * (*getUnmarkDistributedParams)(Node * parsetree);
	bool cannotBeExecutedInTransaction;
} NonMainDbDistributeObjectOps;


/*
 * getMarkDistributedParams and getUnmarkDistributedParams callbacks for
 * NonMainDbDistributeObjectOps.
 */
static MarkDistributedGloballyParams * CreateRoleStmtGetMarkDistributedParams(
	Node *parsetree);
static List * DropRoleStmtGetUnmarkDistributedParams(Node *parsetree);


/* NonMainDbDistributeObjectOps for different command types */
static NonMainDbDistributeObjectOps Any_CreateRole = {
	.getMarkDistributedParams = CreateRoleStmtGetMarkDistributedParams,
	.getUnmarkDistributedParams = NULL,
	.cannotBeExecutedInTransaction = false
};
static NonMainDbDistributeObjectOps Any_DropRole = {
	.getMarkDistributedParams = NULL,
	.getUnmarkDistributedParams = DropRoleStmtGetUnmarkDistributedParams,
	.cannotBeExecutedInTransaction = false
};
static NonMainDbDistributeObjectOps Any_AlterRole = {
	.getMarkDistributedParams = NULL,
	.getUnmarkDistributedParams = NULL,
	.cannotBeExecutedInTransaction = false
};
static NonMainDbDistributeObjectOps Any_GrantRole = {
	.cannotBeExecutedInTransaction = false
};
static NonMainDbDistributeObjectOps Database_Create = {
	.getMarkDistributedParams = NULL,
	.getUnmarkDistributedParams = NULL,
	.cannotBeExecutedInTransaction = true
};
static NonMainDbDistributeObjectOps Database_Drop = {
	.getMarkDistributedParams = NULL,
	.getUnmarkDistributedParams = NULL,
	.cannotBeExecutedInTransaction = true
};
static NonMainDbDistributeObjectOps Database_Grant = {
	.getMarkDistributedParams = NULL,
	.getUnmarkDistributedParams = NULL,
	.cannotBeExecutedInTransaction = false
};
static NonMainDbDistributeObjectOps Role_SecLabel = {
	.getMarkDistributedParams = NULL,
	.getUnmarkDistributedParams = NULL,
	.cannotBeExecutedInTransaction = false
};


/* other static function declarations */
const NonMainDbDistributeObjectOps * GetNonMainDbDistributeObjectOps(Node *parsetree);
static void MarkObjectDistributedGloballyOnMainDbs(
	MarkDistributedGloballyParams *markDistributedParams);
static void UnmarkObjectsDistributedOnLocalMainDb(List *unmarkDistributedList);


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
	if (IsMainDB)
	{
		return false;
	}

	const NonMainDbDistributeObjectOps *ops = GetNonMainDbDistributeObjectOps(parsetree);
	if (!ops)
	{
		return false;
	}

	char *queryString = DeparseTreeNode(parsetree);

	/*
	 * For the commands that cannot be executed in a transaction, there are no
	 * transactional visibility issues. We directly route them to main database
	 * so that we only have to consider one code-path for such commands.
	 */
	if (ops->cannotBeExecutedInTransaction)
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

	if (ops->getUnmarkDistributedParams)
	{
		List *unmarkDistributedParamsList = ops->getUnmarkDistributedParams(parsetree);
		UnmarkObjectsDistributedOnLocalMainDb(unmarkDistributedParamsList);
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
	if (IsMainDB)
	{
		return;
	}

	const NonMainDbDistributeObjectOps *ops = GetNonMainDbDistributeObjectOps(parsetree);
	if (!ops)
	{
		return;
	}

	if (ops->getMarkDistributedParams)
	{
		MarkDistributedGloballyParams *markDistributedParams =
			ops->getMarkDistributedParams(parsetree);
		MarkObjectDistributedGloballyOnMainDbs(markDistributedParams);
	}
}


/*
 * GetNonMainDbDistributeObjectOps returns the NonMainDbDistributeObjectOps for given
 * command if it's node-wide object management command that's supported from non-main
 * databases.
 */
const NonMainDbDistributeObjectOps *
GetNonMainDbDistributeObjectOps(Node *parsetree)
{
	switch (nodeTag(parsetree))
	{
		case T_CreateRoleStmt:
		{
			return &Any_CreateRole;
		}

		case T_DropRoleStmt:
		{
			return &Any_DropRole;
		}

		case T_AlterRoleStmt:
		{
			return &Any_AlterRole;
		}

		case T_GrantRoleStmt:
		{
			return &Any_GrantRole;
		}

		case T_CreatedbStmt:
		{
			CreatedbStmt *stmt = castNode(CreatedbStmt, parsetree);

			/*
			 * We don't try to send the query to the main database if the CREATE
			 * DATABASE command is for the main database itself, this is a very
			 * rare case but it's exercised by our test suite.
			 */
			if (strcmp(stmt->dbname, MainDb) != 0)
			{
				return &Database_Create;
			}

			return NULL;
		}

		case T_DropdbStmt:
		{
			DropdbStmt *stmt = castNode(DropdbStmt, parsetree);

			/*
			 * We don't try to send the query to the main database if the DROP
			 * DATABASE command is for the main database itself, this is a very
			 * rare case but it's exercised by our test suite.
			 */
			if (strcmp(stmt->dbname, MainDb) != 0)
			{
				return &Database_Drop;
			}

			return NULL;
		}

		case T_GrantStmt:
		{
			GrantStmt *stmt = castNode(GrantStmt, parsetree);

			switch (stmt->objtype)
			{
				case OBJECT_DATABASE:
				{
					return &Database_Grant;
				}

				default:
					return NULL;
			}
		}

		case T_SecLabelStmt:
		{
			SecLabelStmt *stmt = castNode(SecLabelStmt, parsetree);

			switch (stmt->objtype)
			{
				case OBJECT_ROLE:
				{
					return &Role_SecLabel;
				}

				default:
					return NULL;
			}
		}

		default:
			return NULL;
	}
}


/*
 * MarkObjectDistributedGloballyOnMainDbs marks an object as
 * distributed on all main databases globally.
 */
static void
MarkObjectDistributedGloballyOnMainDbs(
	MarkDistributedGloballyParams *markDistributedParams)
{
	StringInfo mainDBQuery = makeStringInfo();
	appendStringInfo(mainDBQuery,
					 MARK_OBJECT_DISTRIBUTED,
					 markDistributedParams->catalogRelId,
					 quote_literal_cstr(markDistributedParams->name),
					 markDistributedParams->id,
					 quote_literal_cstr(CurrentUserName()));
	RunCitusMainDBQuery(mainDBQuery->data);
}


/*
 * UnmarkObjectsDistributedOnLocalMainDb unmarks a list of objects as
 * distributed on the local main database.
 */
static void
UnmarkObjectsDistributedOnLocalMainDb(List *unmarkDistributedParamsList)
{
	int subObjectId = 0;
	char *checkObjectExistence = "false";

	UnmarkDistributedLocallyParams *unmarkDistributedParams = NULL;
	foreach_ptr(unmarkDistributedParams, unmarkDistributedParamsList)
	{
		StringInfo query = makeStringInfo();
		appendStringInfo(query,
						 UNMARK_OBJECT_DISTRIBUTED,
						 unmarkDistributedParams->catalogRelId,
						 unmarkDistributedParams->id,
						 subObjectId, checkObjectExistence);
		RunCitusMainDBQuery(query->data);
	}
}


/*
 * getMarkDistributedParams and getUnmarkDistributedParams callback implementations
 * for NonMainDbDistributeObjectOps start here.
 */
static MarkDistributedGloballyParams *
CreateRoleStmtGetMarkDistributedParams(Node *parsetree)
{
	CreateRoleStmt *stmt = castNode(CreateRoleStmt, parsetree);
	MarkDistributedGloballyParams *params =
		(MarkDistributedGloballyParams *) palloc(sizeof(MarkDistributedGloballyParams));
	params->name = stmt->role;
	params->catalogRelId = AuthIdRelationId;

	/* object must exist as we've just created it */
	bool missingOk = false;
	params->id = get_role_oid(stmt->role, missingOk);

	return params;
}


static List *
DropRoleStmtGetUnmarkDistributedParams(Node *parsetree)
{
	DropRoleStmt *stmt = castNode(DropRoleStmt, parsetree);

	List *paramsList = NIL;

	RoleSpec *roleSpec = NULL;
	foreach_ptr(roleSpec, stmt->roles)
	{
		UnmarkDistributedLocallyParams *params =
			(UnmarkDistributedLocallyParams *) palloc(
				sizeof(UnmarkDistributedLocallyParams));

		Oid roleOid = get_role_oid(roleSpec->rolename, stmt->missing_ok);
		if (roleOid == InvalidOid)
		{
			continue;
		}

		params->id = roleOid;
		params->catalogRelId = AuthIdRelationId;

		paramsList = lappend(paramsList, params);
	}

	return paramsList;
}
