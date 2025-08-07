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

#include "catalog/namespace.h"
#include "commands/defrem.h"

#include "pg_version_constants.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_partitioning_utils.h"


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

	/*
	 * We do not support CLUSTER command on partitioned tables as it can not be run inside
	 * transaction blocks. PostgreSQL currently does not support CLUSTER command on
	 * partitioned tables in a transaction block. Although Citus can execute commands
	 * outside of transaction block -- such as VACUUM -- we cannot do that here because
	 * CLUSTER command is also not allowed from a function call as well. By default, Citus
	 * uses `worker_apply_shard_ddl_command()`, where we should avoid it for this case.
	 */
	if (PartitionedTable(relationId))
	{
		if (EnableUnsupportedFeatureMessages)
		{
			ereport(WARNING, (errmsg("not propagating CLUSTER command for partitioned "
									 "table to worker nodes"),
							  errhint("Provide a child partition table names in order to "
									  "CLUSTER distributed partitioned tables.")));
		}

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
	DefElem *opt = NULL;
	foreach_declared_ptr(opt, clusterStmt->params)
	{
		if (strcmp(opt->defname, "verbose") == 0)
		{
			return defGetBoolean(opt);
		}
	}
	return false;
}
