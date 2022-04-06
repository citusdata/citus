/*-------------------------------------------------------------------------
 *
 * view.c
 *    Commands for distributing CREATE OR REPLACE VIEW statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

#include "catalog/objectaddress.h"
#include "commands/extension.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/errormessage.h"
#include "distributed/listutils.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_executor.h"
#include "distributed/namespace_utils.h"
#include "distributed/worker_transaction.h"
#include "executor/spi.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static List * FilterNameListForDistributedViews(List *viewNamesList, bool missing_ok);

/*
 * PreprocessViewStmt is called during the planning phase for CREATE OR REPLACE VIEW
 * before it is created on the local node internally.
 *
 * We do our basic housekeeping where we make sure we are on the coordinator and
 * qualify the given statement.
 */
List *
PreprocessViewStmt(Node *node, const char *queryString,
				   ProcessUtilityContext processUtilityContext)
{
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

	return NIL;
}


/*
 * PostprocessViewStmt actually creates the plan we need to execute for view propagation.
 *
 * If view depends on any undistributable object, Citus can not distribute it. In order to
 * not to prevent users from creating local views on the coordinator WARNING message will
 * be sent to the customer about the case instead of erroring out.
 *
 * Besides creating the plan we also make sure all (new) dependencies of the view are
 * created on all nodes.
 */
List *
PostprocessViewStmt(Node *node, const char *queryString)
{
	ViewStmt *stmt = castNode(ViewStmt, node);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	ObjectAddress viewAddress = GetObjectAddressFromParseTree((Node *) stmt, false);

	if (IsObjectAddressOwnedByExtension(&viewAddress, NULL))
	{
		return NIL;
	}

	/* If the view has any unsupported dependency, create it locally */
	DeferredErrorMessage *errMsg = DeferErrorIfHasUnsupportedDependency(&viewAddress);

	if (errMsg != NULL)
	{
		RaiseDeferredError(errMsg, WARNING);
		return NIL;
	}

	EnsureSequentialMode(OBJECT_VIEW);

	EnsureDependenciesExistOnAllNodes(&viewAddress);

	const char *sql = DeparseTreeNode((Node *) stmt);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * ViewStmtObjectAddress returns the ObjectAddress for the subject of the
 * CREATE [OR REPLACE] VIEW statement. If missing_ok is false it will error with the
 * normal postgres error for unfound views.
 */
ObjectAddress
ViewStmtObjectAddress(Node *node, bool missing_ok)
{
	ViewStmt *stmt = castNode(ViewStmt, node);

	Oid schemaOid = RangeVarGetCreationNamespace(stmt->view);
	Oid viewOid = get_relname_relid(stmt->view->relname, schemaOid);

	ObjectAddress viewAddress = { 0 };
	ObjectAddressSet(viewAddress, RelationRelationId, viewOid);

	return viewAddress;
}


/*
 * PreprocessDropViewStmt gets called during the planning phase of a DROP VIEW statement
 * and returns a list of DDLJob's that will drop any distributed view from the
 * workers.
 *
 * The DropStmt could have multiple objects to drop, the list of objects will be filtered
 * to only keep the distributed views for deletion on the workers. Non-distributed
 * views will still be dropped locally but not on the workers.
 */
List *
PreprocessDropViewStmt(Node *node, const char *queryString, ProcessUtilityContext
					   processUtilityContext)
{
	DropStmt *stmt = castNode(DropStmt, node);
	List *distributedViews = NIL;

	if (!ShouldPropagate())
	{
		return NIL;
	}

	/*
	 * Our statements need to be fully qualified so we can drop them from the right schema
	 * on the workers
	 */
	QualifyTreeNode((Node *) stmt);

	distributedViews = FilterNameListForDistributedViews(stmt->objects, stmt->missing_ok);

	if (list_length(distributedViews) < 1)
	{
		/* no distributed view to drop */
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_VIEW);

	/*
	 * Swap the list of objects before deparsing and restore the old list after. This
	 * ensures we only have distributed views in the deparsed drop statement.
	 */
	DropStmt *stmtCopy = copyObject(stmt);
	stmtCopy->objects = distributedViews;
	const char *dropStmtSql = DeparseTreeNode((Node *) stmtCopy);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) dropStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * FilterNameListForDistributedViews takes a list of view names and filters against the
 * views that are distributed. Given list of view names must be qualified before.
 *
 * The original list will not be touched, a new list will be created with only the objects
 * in there.
 */
static List *
FilterNameListForDistributedViews(List *viewNamesList, bool missing_ok)
{
	List *distributedViewNames = NIL;

	List *qualifiedViewName = NULL;
	foreach_ptr(qualifiedViewName, viewNamesList)
	{
		/*
		 * Name of the view must be qualified before calling this function
		 */
		Assert(list_length(qualifiedViewName) == 2);

		char *schemaName = strVal(linitial(qualifiedViewName));
		char *viewName = strVal(lsecond(qualifiedViewName));

		Oid schemaId = get_namespace_oid(schemaName, missing_ok);
		Oid viewOid = get_relname_relid(viewName, schemaId);

		if (!OidIsValid(viewOid))
		{
			continue;
		}

		ObjectAddress viewAddress = { 0 };
		ObjectAddressSet(viewAddress, RelationRelationId, viewOid);

		if (IsObjectDistributed(&viewAddress))
		{
			distributedViewNames = lappend(distributedViewNames, qualifiedViewName);
		}
	}
	return distributedViewNames;
}
