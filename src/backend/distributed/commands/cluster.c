/*-------------------------------------------------------------------------
 *
 * cluster.c
 *    Commands for CLUSTER statement
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "distributed/commands.h"
#include "distributed/metadata_cache.h"


/* placeholder for PlanClusterStmt */
List *
PlanClusterStmt(ClusterStmt *clusterStmt, const char *clusterCommand)
{
	bool showPropagationWarning = false;

	/* CLUSTER all */
	if (clusterStmt->relation == NULL)
	{
		showPropagationWarning = true;
	}
	else
	{
		Oid relationId = InvalidOid;
		bool missingOK = false;

		relationId = RangeVarGetRelid(clusterStmt->relation, AccessShareLock,
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
