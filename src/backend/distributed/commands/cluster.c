/*-------------------------------------------------------------------------
 *
 * cluster.c
 *    Commands for CLUSTER statement
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "commands/defrem.h"

#include "catalog/namespace.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"


static bool IsClusterStmtVerbose_compat(ClusterStmt *clusterStmt);

/*
 * PreprocessClusterStmt first determines whether a given cluster statement involves
 * a distributed table. If so (and if it is supported, i.e. no verbose), it
 * creates a DDLJob to encapsulate information needed during the worker node
 * portion of DDL execution before returning that DDLJob in a List. If no
 * distributed table is involved, this function returns NIL.
 */
List *
PreprocessClusterStmt(Node *node, const char *clusterCommand,
					  ProcessUtilityContext processUtilityContext)
{
	ClusterStmt *clusterStmt = castNode(ClusterStmt, node);
	bool missingOK = false;

	if (clusterStmt->relation == NULL)
	{
		if (EnableUnsupportedFeatureMessages)
		{
			ereport(WARNING, (errmsg("not propagating CLUSTER command to worker nodes"),
							  errhint("Provide a specific table in order to CLUSTER "
									  "distributed tables.")));
		}

		return NIL;
	}

	/* PostgreSQL uses access exclusive lock for CLUSTER command */
	Oid relationId = RangeVarGetRelid(clusterStmt->relation, AccessExclusiveLock,
									  missingOK);

	/*
	 * If the table does not exist, don't do anything here to allow PostgreSQL
	 * to throw the appropriate error or notice message later.
	 */
	if (!OidIsValid(relationId))
	{
		return NIL;
	}

	/* we have no planning to do unless the table is distributed */
	bool isCitusRelation = IsCitusTable(relationId);
	if (!isCitusRelation)
	{
		return NIL;
	}

	if (IsClusterStmtVerbose_compat(clusterStmt))
	{
		ereport(ERROR, (errmsg("cannot run CLUSTER command"),
						errdetail("VERBOSE option is currently unsupported "
								  "for distributed tables.")));
	}

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));
	ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId, relationId);
	ddlJob->metadataSyncCommand = clusterCommand;
	ddlJob->taskList = DDLTaskList(relationId, clusterCommand);

	return list_make1(ddlJob);
}


/*
 * IsClusterStmtVerbose_compat returns true if the given statement
 * is a cluster statement with verbose option.
 */
static bool
IsClusterStmtVerbose_compat(ClusterStmt *clusterStmt)
{
#if PG_VERSION_NUM < PG_VERSION_14
	if (clusterStmt->options & CLUOPT_VERBOSE)
	{
		return true;
	}
	return false;
#else
	DefElem *opt = NULL;
	foreach_ptr(opt, clusterStmt->params)
	{
		if (strcmp(opt->defname, "verbose") == 0)
		{
			return defGetBoolean(opt);
		}
	}
	return false;
#endif
}
