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

static DistributeObjectOps NoDistributeOps = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = NULL,
	.address = NULL,
};

/* TODO this is a 2 level nested statement which we do not currently support */
static DistributeObjectOps Attribute_Rename = {
	.deparse = DeparseRenameAttributeStmt,
	.qualify = QualifyRenameAttributeStmt,
	.preprocess = PreprocessRenameAttributeStmt,
	.postprocess = NULL,
	.address = RenameAttributeStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(RenameStmt, renameType, OBJECT_ATTRIBUTE,
									  Attribute_Rename);

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
		if (node->type == container->type)
		{
			if (container->nested)
			{
				/* nested types are not perse a match */
				ObjectType nestedType = *((ObjectType *) (((char *) node) +
														  container->nestedOffset));
				if (container->nestedType != nestedType)
				{
					/* nested types do not match, skip this entry */
					continue;
				}
			}

			/* this DistributedObjectOps is a match for the current statement */
			return container->ops;
		}
	}

	/* no DistributedObjectOps linked for this statement type */
	return &NoDistributeOps;
}
