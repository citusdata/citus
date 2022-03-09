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
	QualifyTreeNode((Node *) node);

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

	ObjectAddress *undistributableDependency = GetUndistributableDependency(
		&address);
	if (undistributableDependency != NULL)
	{
		if (SupportedDependencyByCitus(undistributableDependency))
		{
			/*
			 * Citus can't distribute some relations as dependency, although those
			 * types as supported by Citus. So we can use get_rel_name directly
			 */
			RangeVar *aggRangeVar = makeRangeVarFromNameList(stmt->defnames);
			char *aggName = aggRangeVar->relname;
			char *dependentRelationName =
				get_rel_name(undistributableDependency->objectId);

			ereport(WARNING, (errmsg("Citus can't distribute aggregate \"%s\" having "
									 "dependency on non-distributed relation \"%s\"",
									 aggName, dependentRelationName),
							  errdetail("Aggregate will be created only locally"),
							  errhint("To distribute aggregate, distribute dependent "
									  "relations first. Then, re-create the aggregate")));
		}
		else
		{
			char *objectType = NULL;
			#if PG_VERSION_NUM >= PG_VERSION_14
			objectType = getObjectTypeDescription(undistributableDependency, false);
			#else
			objectType = getObjectTypeDescription(undistributableDependency);
			#endif
			ereport(WARNING, (errmsg("Citus can't distribute functions having "
									 "dependency on unsupported object of type \"%s\"",
									 objectType),
							  errdetail("Aggregate will be created only locally")));
		}

		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&address);

	List *commands = CreateFunctionDDLCommandsIdempotent(&address);

	commands = lcons(DISABLE_DDL_PROPAGATION, commands);
	commands = lappend(commands, ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}
