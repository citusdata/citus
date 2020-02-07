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

static DistributeObjectOps Any_Index = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessIndexStmt,
	.postprocess = PostprocessIndexStmt,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(IndexStmt, Any_Index);

static DistributeObjectOps Any_Reindex = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessReindexStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(ReindexStmt, Any_Reindex);

static DistributeObjectOps Any_Rename = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessRenameStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(RenameStmt, renameType, OBJECT_TABLE, Any_Rename);
REGISTER_DISTRIBUTED_OPERATION_NESTED(RenameStmt, renameType, OBJECT_FOREIGN_TABLE,
									  Any_Rename);
REGISTER_DISTRIBUTED_OPERATION_NESTED(RenameStmt, renameType, OBJECT_COLUMN, Any_Rename);
REGISTER_DISTRIBUTED_OPERATION_NESTED(RenameStmt, renameType, OBJECT_TABCONSTRAINT,
									  Any_Rename);
REGISTER_DISTRIBUTED_OPERATION_NESTED(RenameStmt, renameType, OBJECT_INDEX, Any_Rename);

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

static DistributeObjectOps Collation_AlterObjectSchema = {
	.deparse = DeparseAlterCollationSchemaStmt,
	.qualify = QualifyAlterCollationSchemaStmt,
	.preprocess = PreprocessAlterCollationSchemaStmt,
	.postprocess = PostprocessAlterCollationSchemaStmt,
	.address = AlterCollationSchemaStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterObjectSchemaStmt, objectType, OBJECT_COLLATION,
									  Collation_AlterObjectSchema);

static DistributeObjectOps Collation_AlterOwner = {
	.deparse = DeparseAlterCollationOwnerStmt,
	.qualify = QualifyAlterCollationOwnerStmt,
	.preprocess = PreprocessAlterCollationOwnerStmt,
	.postprocess = NULL,
	.address = AlterCollationOwnerObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterOwnerStmt, objectType, OBJECT_COLLATION,
									  Collation_AlterOwner);

static DistributeObjectOps Collation_Define = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessDefineCollationStmt,
	.address = DefineCollationStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(DefineStmt, kind, OBJECT_COLLATION,
									  Collation_Define);

static DistributeObjectOps Collation_Drop = {
	.deparse = DeparseDropCollationStmt,
	.qualify = QualifyDropCollationStmt,
	.preprocess = PreprocessDropCollationStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(DropStmt, removeType, OBJECT_COLLATION,
									  Collation_Drop);

static DistributeObjectOps Collation_Rename = {
	.deparse = DeparseRenameCollationStmt,
	.qualify = QualifyRenameCollationStmt,
	.preprocess = PreprocessRenameCollationStmt,
	.postprocess = NULL,
	.address = RenameCollationStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(RenameStmt, renameType, OBJECT_COLLATION,
									  Collation_Rename);

static DistributeObjectOps Index_Drop = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessDropIndexStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(DropStmt, removeType, OBJECT_INDEX, Index_Drop);

static DistributeObjectOps Schema_Drop = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessDropSchemaStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(DropStmt, removeType, OBJECT_SCHEMA, Schema_Drop);

static DistributeObjectOps Schema_Grant = {
	.deparse = DeparseGrantOnSchemaStmt,
	.qualify = NULL,
	.preprocess = PreprocessGrantOnSchemaStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(GrantStmt, objtype, OBJECT_SCHEMA, Schema_Grant);

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
