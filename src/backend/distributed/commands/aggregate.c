/*-------------------------------------------------------------------------
 *
 * aggregate.c
 *    Commands for distributing AGGREGATE statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_executor.h"
#include "nodes/parsenodes.h"
#include "utils/lsyscache.h"


/*
 * PreprocessDefineAggregateStmt only qualifies the node with schema name.
 * We will handle the rest in the Postprocess phase.
 */
List *
PreprocessDefineAggregateStmt(Node *node, const char *queryString,
							  ProcessUtilityContext processUtilityContext)
{
	QualifyTreeNode((Node *) node);

	return NIL;
}


/*
 * PostprocessDefineAggregateStmt actually creates the plan we need to execute for
 * aggregate propagation.
 * This is the downside of using the locally created aggregate to get the sql statement.
 *
 * If the aggregate depends on any non-distributed relation, Citus can not distribute it.
 * In order to not to prevent users from creating local aggregates on the coordinator,
 * a WARNING message will be sent to the user about the case instead of erroring out.
 *
 * Besides creating the plan we also make sure all (new) dependencies of the aggregate
 * are created on all nodes.
 */
List *
PostprocessDefineAggregateStmt(Node *node, const char *queryString)
{
	DefineStmt *stmt = castNode(DefineStmt, node);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	ObjectAddress address = GetObjectAddressFromParseTree((Node *) stmt, false);

	EnsureCoordinator();

	EnsureSequentialMode(OBJECT_AGGREGATE);

	/* If the aggregate has any unsupported dependency, create it locally */
	if (ErrorOrWarnIfObjectHasUnsupportedDependency(&address))
	{
		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&address);

	List *commands = CreateFunctionDDLCommandsIdempotent(&address);

	commands = lcons(DISABLE_DDL_PROPAGATION, commands);
	commands = lappend(commands, ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}
