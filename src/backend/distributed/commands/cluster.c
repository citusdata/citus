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
#if PG_VERSION_NUM >= PG_VERSION_19
static bool RepackStmtHasOption(ClusterStmt *clusterStmt, const char *optionName);
#endif

/*
 * PreprocessClusterStmt first determines whether a given cluster statement involves
 * a distributed table. If so (and if it is supported, i.e. no verbose), it
 * creates a DDLJob to encapsulate information needed during the worker node
 * portion of DDL execution before returning that DDLJob in a List. If no
 * distributed table is involved, this function returns NIL.
 *
 * On PG19 the same node (RepackStmt, aliased as ClusterStmt) backs both CLUSTER
 * and REPACK; the two are told apart by ->command (ClusterStmtIsRepack).  Citus
 * propagates REPACK exactly like CLUSTER -- the original command text is shipped
 * to every shard placement -- so the only command-specific behaviour here is the
 * wording of the user-facing WARNING/ERROR messages.  VACUUM FULL never reaches
 * this path (it stays on the T_VacuumStmt / vacuum code path).
 */
List *
PreprocessClusterStmt(Node *node, const char *clusterCommand,
					  ProcessUtilityContext processUtilityContext)
{
	ClusterStmt *clusterStmt = castNode(ClusterStmt, node);
	bool missingOK = false;
	const char *commandName = ClusterStmtCommandName(clusterStmt);

	if (clusterStmt->relation == NULL)
	{
		if (EnableUnsupportedFeatureMessages)
		{
			ereport(WARNING, (errmsg("not propagating %s command to worker nodes",
									 commandName),
							  errhint("Provide a specific table in order to %s "
									  "distributed tables.", commandName)));
		}

		return NIL;
	}

	/* PostgreSQL uses access exclusive lock for CLUSTER command */
#if PG_VERSION_NUM >= PG_VERSION_19
	Oid relationId = RangeVarGetRelid(clusterStmt->relation->relation,
									  AccessExclusiveLock, missingOK);
#else
	Oid relationId = RangeVarGetRelid(clusterStmt->relation, AccessExclusiveLock,
									  missingOK);
#endif

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
			ereport(WARNING, (errmsg("not propagating %s command for partitioned "
									 "table to worker nodes", commandName),
							  errhint("Provide a child partition table names in order to "
									  "%s distributed partitioned tables.", commandName)));
		}

		return NIL;
	}

	if (IsClusterStmtVerbose_compat(clusterStmt))
	{
		ereport(ERROR, (errmsg("cannot run %s command", commandName),
						errdetail("VERBOSE option is currently unsupported "
								  "for distributed tables.")));
	}

#if PG_VERSION_NUM >= PG_VERSION_19

	/*
	 * PG19 REPACK adds CONCURRENTLY and ANALYZE options that CLUSTER never had.
	 * Citus can not honour them on a distributed table: CONCURRENTLY relies on
	 * PreventInTransactionBlock and can not be shipped through
	 * worker_apply_shard_ddl_command, and ANALYZE has no defined per-shard
	 * semantics yet.  Reject both here, before any shard placement is touched.
	 */
	if (RepackStmtHasOption(clusterStmt, "concurrently"))
	{
		ereport(ERROR, (errmsg("cannot run %s command", commandName),
						errdetail("CONCURRENTLY option is currently unsupported "
								  "for distributed tables.")));
	}

	if (RepackStmtHasOption(clusterStmt, "analyze"))
	{
		ereport(ERROR, (errmsg("cannot run %s command", commandName),
						errdetail("ANALYZE option is currently unsupported "
								  "for distributed tables.")));
	}
#endif

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


#if PG_VERSION_NUM >= PG_VERSION_19

/*
 * RepackStmtHasOption returns true when the given REPACK/CLUSTER statement
 * carries the named boolean option (for example "concurrently" or "analyze")
 * set to true.  PG19-only: these options exist only on the RepackStmt grammar.
 */
static bool
RepackStmtHasOption(ClusterStmt *clusterStmt, const char *optionName)
{
	DefElem *opt = NULL;
	foreach_declared_ptr(opt, clusterStmt->params)
	{
		if (strcmp(opt->defname, optionName) == 0)
		{
			return defGetBoolean(opt);
		}
	}
	return false;
}

#endif
