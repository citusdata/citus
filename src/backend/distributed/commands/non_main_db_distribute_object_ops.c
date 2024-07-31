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
 *    NonMainDbDistributeObjectOps object within OperationArray. Also, if
 *    the command requires marking or unmarking some objects as distributed,
 *    the necessary operations can be implemented in
 *    RunPreprocessNonMainDBCommand and RunPostprocessNonMainDBCommand.
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
#include "distributed/commands/utility_hook.h"
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
 * NonMainDbDistributeObjectOps contains the necessary callbacks / flags to
 * support node-wide object management commands from non-main databases.
 *
 *  cannotBeExecutedInTransaction:
 *   Indicates whether the statement cannot be executed in a transaction. If
 *   this is set to true, the statement will be executed directly on the main
 *   database because there are no transactional visibility issues for such
 *   commands.
 *
 *  checkSupportedObjectType:
 *   Callback function that checks whether type of the object referred to by
 *   given statement is supported. Can be NULL if not applicable for the
 *   statement type.
 */
typedef struct NonMainDbDistributeObjectOps
{
	bool (*cannotBeExecutedInTransaction)(Node *parsetree);
	bool (*checkSupportedObjectType)(Node *parsetree);
} NonMainDbDistributeObjectOps;


/*
 * checkSupportedObjectType callbacks for OperationArray.
 */
static bool CreateDbStmtCheckSupportedObjectType(Node *node);
static bool DropDbStmtCheckSupportedObjectType(Node *node);
static bool GrantStmtCheckSupportedObjectType(Node *node);
static bool SecLabelStmtCheckSupportedObjectType(Node *node);
static bool AlterDbRenameCheckSupportedObjectType(Node *node);
static bool AlterDbOwnerCheckSupportedObjectType(Node *node);

/*
 * cannotBeExecutedInTransaction callbacks for OperationArray.
 */
static bool CannotBeExecutedInTransaction_True(Node *node);
static bool CannotBeExecutedInTransaction_False(Node *node);
static bool AlterDbCannotBeExecutedInTransaction(Node *node);

/*
 * OperationArray that holds NonMainDbDistributeObjectOps for different command types.
 */
static const NonMainDbDistributeObjectOps *const OperationArray[] = {
	[T_CreateRoleStmt] = &(NonMainDbDistributeObjectOps) {
		.cannotBeExecutedInTransaction = CannotBeExecutedInTransaction_False,
		.checkSupportedObjectType = NULL
	},
	[T_DropRoleStmt] = &(NonMainDbDistributeObjectOps) {
		.cannotBeExecutedInTransaction = CannotBeExecutedInTransaction_False,
		.checkSupportedObjectType = NULL
	},
	[T_AlterRoleStmt] = &(NonMainDbDistributeObjectOps) {
		.cannotBeExecutedInTransaction = CannotBeExecutedInTransaction_False,
		.checkSupportedObjectType = NULL
	},
	[T_GrantRoleStmt] = &(NonMainDbDistributeObjectOps) {
		.cannotBeExecutedInTransaction = CannotBeExecutedInTransaction_False,
		.checkSupportedObjectType = NULL
	},
	[T_CreatedbStmt] = &(NonMainDbDistributeObjectOps) {
		.cannotBeExecutedInTransaction = CannotBeExecutedInTransaction_True,
		.checkSupportedObjectType = CreateDbStmtCheckSupportedObjectType
	},
	[T_DropdbStmt] = &(NonMainDbDistributeObjectOps) {
		.cannotBeExecutedInTransaction = CannotBeExecutedInTransaction_True,
		.checkSupportedObjectType = DropDbStmtCheckSupportedObjectType
	},
	[T_AlterDatabaseSetStmt] = &(NonMainDbDistributeObjectOps) {
		.cannotBeExecutedInTransaction = CannotBeExecutedInTransaction_False,
		.checkSupportedObjectType = NULL
	},
	[T_AlterDatabaseStmt] = &(NonMainDbDistributeObjectOps) {
		.cannotBeExecutedInTransaction = AlterDbCannotBeExecutedInTransaction,
		.checkSupportedObjectType = NULL
	},
#if PG_VERSION_NUM >= PG_VERSION_15
	[T_AlterDatabaseRefreshCollStmt] = &(NonMainDbDistributeObjectOps) {
		.cannotBeExecutedInTransaction = CannotBeExecutedInTransaction_False,
		.checkSupportedObjectType = NULL
	},
#endif
	[T_RenameStmt] = &(NonMainDbDistributeObjectOps) {
		.cannotBeExecutedInTransaction = CannotBeExecutedInTransaction_False,
		.checkSupportedObjectType = AlterDbRenameCheckSupportedObjectType
	},
	[T_AlterOwnerStmt] = &(NonMainDbDistributeObjectOps) {
		.cannotBeExecutedInTransaction = CannotBeExecutedInTransaction_False,
		.checkSupportedObjectType = AlterDbOwnerCheckSupportedObjectType
	},
	[T_GrantStmt] = &(NonMainDbDistributeObjectOps) {
		.cannotBeExecutedInTransaction = CannotBeExecutedInTransaction_False,
		.checkSupportedObjectType = GrantStmtCheckSupportedObjectType
	},
	[T_SecLabelStmt] = &(NonMainDbDistributeObjectOps) {
		.cannotBeExecutedInTransaction = CannotBeExecutedInTransaction_False,
		.checkSupportedObjectType = SecLabelStmtCheckSupportedObjectType
	},
};


/* other static function declarations */
const NonMainDbDistributeObjectOps * GetNonMainDbDistributeObjectOps(Node *parsetree);
static void CreateRoleStmtMarkDistGloballyOnMainDbs(CreateRoleStmt *createRoleStmt);
static void DropRoleStmtUnmarkDistOnLocalMainDb(DropRoleStmt *dropRoleStmt);
static void MarkObjectDistributedGloballyOnMainDbs(Oid catalogRelId, Oid objectId,
												   char *objectName);
static void UnmarkObjectDistributedOnLocalMainDb(uint16 catalogRelId, Oid objectId);


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
	if (IsMainDB || !EnableDDLPropagation)
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
	if (ops->cannotBeExecutedInTransaction(parsetree))
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

	if (IsA(parsetree, DropRoleStmt))
	{
		DropRoleStmtUnmarkDistOnLocalMainDb((DropRoleStmt *) parsetree);
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
	if (IsMainDB || !EnableDDLPropagation || !GetNonMainDbDistributeObjectOps(parsetree))
	{
		return;
	}

	if (IsA(parsetree, CreateRoleStmt))
	{
		CreateRoleStmtMarkDistGloballyOnMainDbs((CreateRoleStmt *) parsetree);
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
	NodeTag tag = nodeTag(parsetree);
	if (tag >= lengthof(OperationArray))
	{
		return NULL;
	}

	const NonMainDbDistributeObjectOps *ops = OperationArray[tag];

	if (ops == NULL)
	{
		return NULL;
	}

	if (!ops->checkSupportedObjectType ||
		ops->checkSupportedObjectType(parsetree))
	{
		return ops;
	}

	return NULL;
}


/*
 * CreateRoleStmtMarkDistGloballyOnMainDbs marks the role as
 * distributed on all main databases globally.
 */
static void
CreateRoleStmtMarkDistGloballyOnMainDbs(CreateRoleStmt *createRoleStmt)
{
	/* object must exist as we've just created it */
	bool missingOk = false;
	Oid roleId = get_role_oid(createRoleStmt->role, missingOk);

	MarkObjectDistributedGloballyOnMainDbs(AuthIdRelationId, roleId,
										   createRoleStmt->role);
}


/*
 * DropRoleStmtUnmarkDistOnLocalMainDb unmarks the roles as
 * distributed on the local main database.
 */
static void
DropRoleStmtUnmarkDistOnLocalMainDb(DropRoleStmt *dropRoleStmt)
{
	RoleSpec *roleSpec = NULL;
	foreach_ptr(roleSpec, dropRoleStmt->roles)
	{
		Oid roleOid = get_role_oid(roleSpec->rolename,
								   dropRoleStmt->missing_ok);
		if (roleOid == InvalidOid)
		{
			continue;
		}

		UnmarkObjectDistributedOnLocalMainDb(AuthIdRelationId, roleOid);
	}
}


/*
 * MarkObjectDistributedGloballyOnMainDbs marks an object as
 * distributed on all main databases globally.
 */
static void
MarkObjectDistributedGloballyOnMainDbs(Oid catalogRelId, Oid objectId, char *objectName)
{
	StringInfo mainDBQuery = makeStringInfo();
	appendStringInfo(mainDBQuery,
					 MARK_OBJECT_DISTRIBUTED,
					 catalogRelId,
					 quote_literal_cstr(objectName),
					 objectId,
					 quote_literal_cstr(CurrentUserName()));
	RunCitusMainDBQuery(mainDBQuery->data);
}


/*
 * UnmarkObjectDistributedOnLocalMainDb unmarks an object as
 * distributed on the local main database.
 */
static void
UnmarkObjectDistributedOnLocalMainDb(uint16 catalogRelId, Oid objectId)
{
	const int subObjectId = 0;
	const char *checkObjectExistence = "false";

	StringInfo query = makeStringInfo();
	appendStringInfo(query,
					 UNMARK_OBJECT_DISTRIBUTED,
					 catalogRelId, objectId,
					 subObjectId, checkObjectExistence);
	RunCitusMainDBQuery(query->data);
}


/*
 * checkSupportedObjectTypes callbacks for OperationArray lie below.
 */
static bool
CreateDbStmtCheckSupportedObjectType(Node *node)
{
	/*
	 * We don't try to send the query to the main database if the CREATE
	 * DATABASE command is for the main database itself, this is a very
	 * rare case but it's exercised by our test suite.
	 */
	CreatedbStmt *stmt = castNode(CreatedbStmt, node);
	return strcmp(stmt->dbname, MainDb) != 0;
}


static bool
DropDbStmtCheckSupportedObjectType(Node *node)
{
	/*
	 * We don't try to send the query to the main database if the DROP
	 * DATABASE command is for the main database itself, this is a very
	 * rare case but it's exercised by our test suite.
	 */
	DropdbStmt *stmt = castNode(DropdbStmt, node);
	return strcmp(stmt->dbname, MainDb) != 0;
}


static bool
AlterDbRenameCheckSupportedObjectType(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	return stmt->renameType == OBJECT_DATABASE;
}


static bool
AlterDbOwnerCheckSupportedObjectType(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	return stmt->objectType == OBJECT_DATABASE;
}


static bool
GrantStmtCheckSupportedObjectType(Node *node)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	return stmt->objtype == OBJECT_DATABASE;
}


static bool
SecLabelStmtCheckSupportedObjectType(Node *node)
{
	SecLabelStmt *stmt = castNode(SecLabelStmt, node);
	return stmt->objtype == OBJECT_ROLE;
}


/*
 * cannotBeExecutedInTransaction callbacks for OperationArray lie below.
 */
static bool
CannotBeExecutedInTransaction_True(Node *node)
{
	return true;
}


static bool
CannotBeExecutedInTransaction_False(Node *node)
{
	return false;
}


static bool
AlterDbCannotBeExecutedInTransaction(Node *node)
{
	AlterDatabaseStmt *stmt = castNode(AlterDatabaseStmt, node);
	return IsSetTablespaceStatement(stmt);
}
