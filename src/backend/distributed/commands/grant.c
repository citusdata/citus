/*-------------------------------------------------------------------------
 *
 * grant.c
 *    Commands for granting access to distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "distributed/commands.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/master_protocol.h"


/* placeholder for PreprocessGrantStmt */
List *
PreprocessGrantStmt(Node *node, const char *queryString)
{
	GrantStmt *grantStmt = castNode(GrantStmt, node);
	bool showPropagationWarning = false;

	/* don't emit warnings in worker nodes */
	if (!IsCoordinator())
	{
		return NIL;
	}

	if (grantStmt->targtype == ACL_TARGET_ALL_IN_SCHEMA)
	{
		showPropagationWarning = true;
	}
	else if (grantStmt->targtype == ACL_TARGET_OBJECT)
	{
		switch (grantStmt->objtype)
		{
#if (PG_VERSION_NUM >= 110000)
			case OBJECT_SCHEMA:
			case OBJECT_DATABASE:
#else
			case ACL_OBJECT_DATABASE:
#endif
			{
				showPropagationWarning = true;
				break;
			}

#if (PG_VERSION_NUM >= 110000)
			case OBJECT_TABLE:
#else
			case ACL_OBJECT_RELATION:
#endif
			{
				ListCell *rangeVarCell = NULL;

				foreach(rangeVarCell, grantStmt->objects)
				{
					RangeVar *rangeVar = (RangeVar *) lfirst(rangeVarCell);

					Oid relationId = RangeVarGetRelid(rangeVar, NoLock, false);
					if (OidIsValid(relationId) && IsDistributedTable(relationId))
					{
						showPropagationWarning = true;
						break;
					}
				}

				break;
			}

			/* no need to warn when object is sequence, domain, function, etc. */
			default:
			{
				break;
			}
		}
	}

	if (showPropagationWarning)
	{
		const char *type = grantStmt->is_grant ? "GRANT" : "REVOKE";
		ereport(WARNING, (errmsg("not propagating %s command to worker nodes", type)));
	}

	return NIL;
}
