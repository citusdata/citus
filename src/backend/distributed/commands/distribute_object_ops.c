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

static DistributeObjectOps Any_AlterFunction = {
	.deparse = DeparseAlterFunctionStmt,
	.qualify = QualifyAlterFunctionStmt,
	.preprocess = PreprocessAlterFunctionStmt,
	.postprocess = NULL,
	.address = AlterFunctionStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION(AlterFunctionStmt, Any_AlterFunction);

static DistributeObjectOps Any_AlterPolicy = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterPolicyStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(AlterPolicyStmt, Any_AlterPolicy);

static DistributeObjectOps Any_AlterRole = {
	.deparse = DeparseAlterRoleStmt,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessAlterRoleStmt,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(AlterRoleStmt, Any_AlterRole);

static DistributeObjectOps Any_AlterTableMoveAll = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterTableMoveAllStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(AlterTableMoveAllStmt, Any_AlterTableMoveAll);

static DistributeObjectOps Any_Cluster = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessClusterStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(ClusterStmt, Any_Cluster);

static DistributeObjectOps Any_CreateFunction = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessCreateFunctionStmt,
	.postprocess = PostprocessCreateFunctionStmt,
	.address = CreateFunctionStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION(CreateFunctionStmt, Any_CreateFunction);

static DistributeObjectOps Any_CreatePolicy = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessCreatePolicyStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(CreatePolicyStmt, Any_CreatePolicy);

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

static DistributeObjectOps ForeignTable_AlterTable = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterTableStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterTableStmt, relkind, OBJECT_FOREIGN_TABLE,
									  ForeignTable_AlterTable);

static DistributeObjectOps Index_AlterTable = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterTableStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterTableStmt, relkind, OBJECT_INDEX,
									  Index_AlterTable);

static DistributeObjectOps Index_Drop = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessDropIndexStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(DropStmt, removeType, OBJECT_INDEX, Index_Drop);

static DistributeObjectOps Policy_Drop = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessDropPolicyStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(DropStmt, removeType, OBJECT_POLICY, Policy_Drop);

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

static DistributeObjectOps Table_AlterTable = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterTableStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterTableStmt, relkind, OBJECT_TABLE,
									  Table_AlterTable);

static DistributeObjectOps Table_AlterObjectSchema = {
	.deparse = DeparseAlterTableSchemaStmt,
	.qualify = QualifyAlterTableSchemaStmt,
	.preprocess = PreprocessAlterTableSchemaStmt,
	.postprocess = PostprocessAlterTableSchemaStmt,
	.address = AlterTableSchemaStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterObjectSchemaStmt, objectType, OBJECT_TABLE,
									  Table_AlterObjectSchema);

static DistributeObjectOps Table_Drop = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessDropTableStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(DropStmt, removeType, OBJECT_TABLE, Table_Drop);

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
