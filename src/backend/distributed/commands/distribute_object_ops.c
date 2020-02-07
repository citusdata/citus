/*-------------------------------------------------------------------------
 *
 * distribute_object_ops.c
 *
 *    Contains declarations for DistributeObjectOps, along with their
 *    lookup function, GetDistributeObjectOps.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"

static bool MatchOnAllNestedObjectTypes(Node *node,
										DistributedObjectOpsContainerNestedInfo *
										nestedInfo);

static DistributeObjectOps NoDistributeOps = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = NULL,
	.address = NULL,
};

/* linker provided pointers */
SECTION_ARRAY(DistributedObjectOpsContainer *, opscontainer);

/*
 * GetDistributeObjectOps looks up the DistributeObjectOps which handles the node.
 *
 * Never returns NULL.
 */
const DistributeObjectOps *
GetDistributeObjectOps(Node *node)
{
	int i = 0;
	size_t sz = SECTION_SIZE(opscontainer);
	for (i = 0; i < sz; i++)
	{
		DistributedObjectOpsContainer *container = opscontainer_array[i];
		if (node->type == container->type &&
			MatchOnAllNestedObjectTypes(node, container->nestedInfo))
		{
			/* this DistributedObjectOps is a match for the current statement */
			return container->ops;
		}
	}

	/* no DistributedObjectOps linked for this statement type */
	return &NoDistributeOps;
}


static bool
MatchOnAllNestedObjectTypes(Node *node,
							DistributedObjectOpsContainerNestedInfo *nestedInfo)
{
	if (nestedInfo == NULL)
	{
		/* No nested information, matching by convention  */
		return true;
	}

	/*
	 * Iterate till you find the last entry { 0 }. For convenience it is the only one with
	 * offset 0
	 */
	for (; nestedInfo->offset != 0; ++nestedInfo)
	{
		ObjectType nestedType = *((ObjectType *) (((char *) node) + nestedInfo->offset));
		if (nestedInfo->type != nestedType)
		{
			/* nested object type is not matching*/
			return false;
		}
	}

	return true;
}
