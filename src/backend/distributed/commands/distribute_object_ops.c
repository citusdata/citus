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

static DistributeObjectOps Aggregate_AlterObjectSchema = {
	.deparse = DeparseAlterFunctionSchemaStmt,
	.qualify = QualifyAlterFunctionSchemaStmt,
	.preprocess = PreprocessAlterFunctionSchemaStmt,
	.postprocess = PostprocessAlterFunctionSchemaStmt,
	.address = AlterFunctionSchemaStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterObjectSchemaStmt, objectType,
									  OBJECT_AGGREGATE, Aggregate_AlterObjectSchema);

static DistributeObjectOps Aggregate_AlterOwner = {
	.deparse = DeparseAlterFunctionOwnerStmt,
	.qualify = QualifyAlterFunctionOwnerStmt,
	.preprocess = PreprocessAlterFunctionOwnerStmt,
	.postprocess = NULL,
	.address = AlterFunctionOwnerObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterOwnerStmt, objectType,
									  OBJECT_AGGREGATE, Aggregate_AlterOwner);

static DistributeObjectOps Aggregate_Define = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = NULL,
	.address = DefineAggregateStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(DefineStmt, kind,
									  OBJECT_AGGREGATE, Aggregate_Define);

static DistributeObjectOps Aggregate_Drop = {
	.deparse = DeparseDropFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropFunctionStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(DropStmt, removeType,
									  OBJECT_AGGREGATE, Aggregate_Drop);

static DistributeObjectOps Aggregate_Rename = {
	.deparse = DeparseRenameFunctionStmt,
	.qualify = QualifyRenameFunctionStmt,
	.preprocess = PreprocessRenameFunctionStmt,
	.postprocess = NULL,
	.address = RenameFunctionStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(RenameStmt, renameType,
									  OBJECT_AGGREGATE, Aggregate_Rename);

static DistributeObjectOps Any_AlterEnum = {
	.deparse = DeparseAlterEnumStmt,
	.qualify = QualifyAlterEnumStmt,
	.preprocess = PreprocessAlterEnumStmt,
	.postprocess = PostprocessAlterEnumStmt,
	.address = AlterEnumStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION(AlterEnumStmt, Any_AlterEnum);

static DistributeObjectOps Any_AlterExtension = {
	.deparse = DeparseAlterExtensionStmt,
	.qualify = NULL,
	.preprocess = PreprocessAlterExtensionUpdateStmt,
	.postprocess = NULL,
	.address = AlterExtensionUpdateStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION(AlterExtensionStmt, Any_AlterExtension);

static DistributeObjectOps Any_AlterExtensionContents = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterExtensionContentsStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(AlterExtensionContentsStmt, Any_AlterExtensionContents);

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

static DistributeObjectOps Any_CompositeType = {
	.deparse = DeparseCompositeTypeStmt,
	.qualify = QualifyCompositeTypeStmt,
	.preprocess = PreprocessCompositeTypeStmt,
	.postprocess = PostprocessCompositeTypeStmt,
	.address = CompositeTypeStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION(CompositeTypeStmt, Any_CompositeType);

static DistributeObjectOps Any_CreateEnum = {
	.deparse = DeparseCreateEnumStmt,
	.qualify = QualifyCreateEnumStmt,
	.preprocess = PreprocessCreateEnumStmt,
	.postprocess = PostprocessCreateEnumStmt,
	.address = CreateEnumStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION(CreateEnumStmt, Any_CreateEnum);

static DistributeObjectOps Any_CreateExtension = {
	.deparse = DeparseCreateExtensionStmt,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessCreateExtensionStmt,
	.address = CreateExtensionStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION(CreateExtensionStmt, Any_CreateExtension);

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

static DistributeObjectOps Extension_AlterObjectSchema = {
	.deparse = DeparseAlterExtensionSchemaStmt,
	.qualify = NULL,
	.preprocess = PreprocessAlterExtensionSchemaStmt,
	.postprocess = PostprocessAlterExtensionSchemaStmt,
	.address = AlterExtensionSchemaStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterObjectSchemaStmt, objectType, OBJECT_EXTENSION,
									  Extension_AlterObjectSchema);

static DistributeObjectOps Extension_Drop = {
	.deparse = DeparseDropExtensionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropExtensionStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(DropStmt, removeType, OBJECT_EXTENSION,
									  Extension_Drop);

static DistributeObjectOps ForeignTable_AlterTable = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterTableStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterTableStmt, relkind, OBJECT_FOREIGN_TABLE,
									  ForeignTable_AlterTable);

static DistributeObjectOps Function_AlterObjectDepends = {
	.deparse = DeparseAlterFunctionDependsStmt,
	.qualify = QualifyAlterFunctionDependsStmt,
	.preprocess = PreprocessAlterFunctionDependsStmt,
	.postprocess = NULL,
	.address = AlterFunctionDependsStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterObjectDependsStmt, objectType, OBJECT_FUNCTION,
									  Function_AlterObjectDepends);

static DistributeObjectOps Function_AlterObjectSchema = {
	.deparse = DeparseAlterFunctionSchemaStmt,
	.qualify = QualifyAlterFunctionSchemaStmt,
	.preprocess = PreprocessAlterFunctionSchemaStmt,
	.postprocess = PostprocessAlterFunctionSchemaStmt,
	.address = AlterFunctionSchemaStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterObjectSchemaStmt, objectType, OBJECT_FUNCTION,
									  Function_AlterObjectSchema);

static DistributeObjectOps Function_AlterOwner = {
	.deparse = DeparseAlterFunctionOwnerStmt,
	.qualify = QualifyAlterFunctionOwnerStmt,
	.preprocess = PreprocessAlterFunctionOwnerStmt,
	.postprocess = NULL,
	.address = AlterFunctionOwnerObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterOwnerStmt, objectType, OBJECT_FUNCTION,
									  Function_AlterOwner);

static DistributeObjectOps Function_Drop = {
	.deparse = DeparseDropFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropFunctionStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(DropStmt, removeType, OBJECT_FUNCTION,
									  Function_Drop);

static DistributeObjectOps Function_Rename = {
	.deparse = DeparseRenameFunctionStmt,
	.qualify = QualifyRenameFunctionStmt,
	.preprocess = PreprocessRenameFunctionStmt,
	.postprocess = NULL,
	.address = RenameFunctionStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(RenameStmt, renameType, OBJECT_FUNCTION,
									  Function_Rename);

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

static DistributeObjectOps Procedure_AlterObjectDepends = {
	.deparse = DeparseAlterFunctionDependsStmt,
	.qualify = QualifyAlterFunctionDependsStmt,
	.preprocess = PreprocessAlterFunctionDependsStmt,
	.postprocess = NULL,
	.address = AlterFunctionDependsStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterObjectDependsStmt, objectType,
									  OBJECT_PROCEDURE, Procedure_AlterObjectDepends);

static DistributeObjectOps Procedure_AlterObjectSchema = {
	.deparse = DeparseAlterFunctionSchemaStmt,
	.qualify = QualifyAlterFunctionSchemaStmt,
	.preprocess = PreprocessAlterFunctionSchemaStmt,
	.postprocess = PostprocessAlterFunctionSchemaStmt,
	.address = AlterFunctionSchemaStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterObjectSchemaStmt, objectType, OBJECT_PROCEDURE,
									  Procedure_AlterObjectSchema);

static DistributeObjectOps Procedure_AlterOwner = {
	.deparse = DeparseAlterFunctionOwnerStmt,
	.qualify = QualifyAlterFunctionOwnerStmt,
	.preprocess = PreprocessAlterFunctionOwnerStmt,
	.postprocess = NULL,
	.address = AlterFunctionOwnerObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterOwnerStmt, objectType, OBJECT_PROCEDURE,
									  Procedure_AlterOwner);

static DistributeObjectOps Procedure_Drop = {
	.deparse = DeparseDropFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropFunctionStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(DropStmt, removeType, OBJECT_PROCEDURE,
									  Procedure_Drop);

static DistributeObjectOps Procedure_Rename = {
	.deparse = DeparseRenameFunctionStmt,
	.qualify = QualifyRenameFunctionStmt,
	.preprocess = PreprocessRenameFunctionStmt,
	.postprocess = NULL,
	.address = RenameFunctionStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(RenameStmt, renameType, OBJECT_PROCEDURE,
									  Procedure_Rename);

static DistributeObjectOps Routine_AlterObjectDepends = {
	.deparse = DeparseAlterFunctionDependsStmt,
	.qualify = QualifyAlterFunctionDependsStmt,
	.preprocess = PreprocessAlterFunctionDependsStmt,
	.postprocess = NULL,
	.address = AlterFunctionDependsStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterObjectDependsStmt, objectType, OBJECT_ROUTINE,
									  Routine_AlterObjectDepends);

static DistributeObjectOps Routine_AlterObjectSchema = {
	.deparse = DeparseAlterFunctionSchemaStmt,
	.qualify = QualifyAlterFunctionSchemaStmt,
	.preprocess = PreprocessAlterFunctionSchemaStmt,
	.postprocess = PostprocessAlterFunctionSchemaStmt,
	.address = AlterFunctionSchemaStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterObjectSchemaStmt, objectType, OBJECT_ROUTINE,
									  Routine_AlterObjectSchema);

static DistributeObjectOps Routine_AlterOwner = {
	.deparse = DeparseAlterFunctionOwnerStmt,
	.qualify = QualifyAlterFunctionOwnerStmt,
	.preprocess = PreprocessAlterFunctionOwnerStmt,
	.postprocess = NULL,
	.address = AlterFunctionOwnerObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterOwnerStmt, objectType, OBJECT_ROUTINE,
									  Routine_AlterOwner);

static DistributeObjectOps Routine_Drop = {
	.deparse = DeparseDropFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropFunctionStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(DropStmt, removeType, OBJECT_ROUTINE, Routine_Drop);

static DistributeObjectOps Routine_Rename = {
	.deparse = DeparseRenameFunctionStmt,
	.qualify = QualifyRenameFunctionStmt,
	.preprocess = PreprocessRenameFunctionStmt,
	.postprocess = NULL,
	.address = RenameFunctionStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(RenameStmt, renameType, OBJECT_ROUTINE,
									  Routine_Rename);

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

static DistributeObjectOps Type_AlterObjectSchema = {
	.deparse = DeparseAlterTypeSchemaStmt,
	.qualify = QualifyAlterTypeSchemaStmt,
	.preprocess = PreprocessAlterTypeSchemaStmt,
	.postprocess = PostprocessAlterTypeSchemaStmt,
	.address = AlterTypeSchemaStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterObjectSchemaStmt, objectType, OBJECT_TYPE,
									  Type_AlterObjectSchema);

static DistributeObjectOps Type_AlterOwner = {
	.deparse = DeparseAlterTypeOwnerStmt,
	.qualify = QualifyAlterTypeOwnerStmt,
	.preprocess = PreprocessAlterTypeOwnerStmt,
	.postprocess = NULL,
	.address = AlterTypeOwnerObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterOwnerStmt, objectType, OBJECT_TYPE,
									  Type_AlterOwner);

static DistributeObjectOps Type_AlterTable = {
	.deparse = DeparseAlterTypeStmt,
	.qualify = QualifyAlterTypeStmt,
	.preprocess = PreprocessAlterTypeStmt,
	.postprocess = NULL,
	.address = AlterTypeStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(AlterTableStmt, relkind, OBJECT_TYPE,
									  Type_AlterTable);

static DistributeObjectOps Type_Drop = {
	.deparse = DeparseDropTypeStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropTypeStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(DropStmt, removeType, OBJECT_TYPE, Type_Drop);

static DistributeObjectOps Type_Rename = {
	.deparse = DeparseRenameTypeStmt,
	.qualify = QualifyRenameTypeStmt,
	.preprocess = PreprocessRenameTypeStmt,
	.postprocess = NULL,
	.address = RenameTypeStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION_NESTED(RenameStmt, renameType, OBJECT_TYPE, Type_Rename);

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
