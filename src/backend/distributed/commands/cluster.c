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
#include "distributed/commands.h"
#include "distributed/metadata_cache.h"

static List * PreprocessClusterStmt(Node *node, const char *clusterCommand);
static DistributeObjectOps Any_Cluster = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessClusterStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(ClusterStmt, Any_Cluster);

/* placeholder for PreprocessClusterStmt */
static List *
PreprocessClusterStmt(Node *node, const char *clusterCommand)
{
	ClusterStmt *clusterStmt = castNode(ClusterStmt, node);
	bool showPropagationWarning = false;

	/* CLUSTER all */
	if (clusterStmt->relation == NULL)
	{
		showPropagationWarning = true;
	}
	else
	{
		bool missingOK = false;

		Oid relationId = RangeVarGetRelid(clusterStmt->relation, AccessShareLock,
										  missingOK);

		if (OidIsValid(relationId))
		{
			showPropagationWarning = IsDistributedTable(relationId);
		}
	}

	if (showPropagationWarning)
	{
		ereport(WARNING, (errmsg("not propagating CLUSTER command to worker nodes")));
	}

	return NIL;
}
