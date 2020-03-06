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


/* placeholder for PreprocessClusterStmt */
List *
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
			showPropagationWarning = IsCitusTable(relationId);
		}
	}

	if (showPropagationWarning)
	{
		ereport(WARNING, (errmsg("not propagating CLUSTER command to worker nodes")));
	}

	return NIL;
}
