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
	.statementTag = T_AlterObjectSchemaStmt,
	.subObjectType = OBJECT_AGGREGATE,

	.deparse = DeparseAlterFunctionSchemaStmt,
	.qualify = QualifyAlterFunctionSchemaStmt,
	.preprocess = PreprocessAlterFunctionSchemaStmt,
	.postprocess = PostprocessAlterFunctionSchemaStmt,
	.address = AlterFunctionSchemaStmtObjectAddress,
};

static DistributeObjectOps Aggregate_AlterOwner = {
	.deparse = DeparseAlterFunctionOwnerStmt,
	.qualify = QualifyAlterFunctionOwnerStmt,
	.preprocess = PreprocessAlterFunctionOwnerStmt,
	.postprocess = NULL,
	.address = AlterFunctionOwnerObjectAddress,
};
static DistributeObjectOps Aggregate_Define = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = NULL,
	.address = DefineAggregateStmtObjectAddress,
};
static DistributeObjectOps Aggregate_Drop = {
	.deparse = DeparseDropFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropFunctionStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Aggregate_Rename = {
	.deparse = DeparseRenameFunctionStmt,
	.qualify = QualifyRenameFunctionStmt,
	.preprocess = PreprocessRenameFunctionStmt,
	.postprocess = NULL,
	.address = RenameFunctionStmtObjectAddress,
};

static DistributeObjectOps Any_AlterEnum = {
	.statementTag = T_AlterEnumStmt,

	.deparse = DeparseAlterEnumStmt,
	.qualify = QualifyAlterEnumStmt,
	.preprocess = PreprocessAlterEnumStmt,
	.postprocess = PostprocessAlterEnumStmt,
	.address = AlterEnumStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION(Any_AlterEnum);

static DistributeObjectOps Any_AlterExtension = {
	.statementTag = T_AlterExtensionStmt,

	.deparse = DeparseAlterExtensionStmt,
	.qualify = NULL,
	.preprocess = PreprocessAlterExtensionUpdateStmt,
	.postprocess = NULL,
	.address = AlterExtensionUpdateStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION(Any_AlterExtension);

static DistributeObjectOps Any_AlterExtensionContents = {
	.statementTag = T_AlterExtensionContentsStmt,

	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterExtensionContentsStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(Any_AlterExtensionContents);

static DistributeObjectOps Any_AlterFunction = {
	.statementTag = T_AlterFunctionStmt,

	.deparse = DeparseAlterFunctionStmt,
	.qualify = QualifyAlterFunctionStmt,
	.preprocess = PreprocessAlterFunctionStmt,
	.postprocess = NULL,
	.address = AlterFunctionStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION(Any_AlterFunction);

static DistributeObjectOps Any_AlterPolicy = {
	.statementTag = T_AlterPolicyStmt,

	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterPolicyStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(Any_AlterPolicy);

static DistributeObjectOps Any_AlterRole = {
	.statementTag = T_AlterRoleStmt,

	.deparse = DeparseAlterRoleStmt,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessAlterRoleStmt,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(Any_AlterRole);

static DistributeObjectOps Any_AlterTableMoveAll = {
	.statementTag = T_AlterTableMoveAllStmt,

	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterTableMoveAllStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(Any_AlterTableMoveAll);

static DistributeObjectOps Any_Cluster = {
	.statementTag = T_ClusterStmt,

	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessClusterStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(Any_Cluster);

static DistributeObjectOps Any_CompositeType = {
	.statementTag = T_CompositeTypeStmt,

	.deparse = DeparseCompositeTypeStmt,
	.qualify = QualifyCompositeTypeStmt,
	.preprocess = PreprocessCompositeTypeStmt,
	.postprocess = PostprocessCompositeTypeStmt,
	.address = CompositeTypeStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION(Any_CompositeType);

static DistributeObjectOps Any_CreateEnum = {
	.statementTag = T_CreateEnumStmt,

	.deparse = DeparseCreateEnumStmt,
	.qualify = QualifyCreateEnumStmt,
	.preprocess = PreprocessCreateEnumStmt,
	.postprocess = PostprocessCreateEnumStmt,
	.address = CreateEnumStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION(Any_CreateEnum);

static DistributeObjectOps Any_CreateExtension = {
	.statementTag = T_CreateExtensionStmt,

	.deparse = DeparseCreateExtensionStmt,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessCreateExtensionStmt,
	.address = CreateExtensionStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION(Any_CreateExtension);

static DistributeObjectOps Any_CreateFunction = {
	.statementTag = T_CreateFunctionStmt,

	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessCreateFunctionStmt,
	.postprocess = PostprocessCreateFunctionStmt,
	.address = CreateFunctionStmtObjectAddress,
};
REGISTER_DISTRIBUTED_OPERATION(Any_CreateFunction);

static DistributeObjectOps Any_CreatePolicy = {
	.statementTag = T_CreatePolicyStmt,

	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessCreatePolicyStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(Any_CreatePolicy);

static DistributeObjectOps Any_Grant = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessGrantStmt,
	.postprocess = NULL,
	.address = NULL,
};

static DistributeObjectOps Any_Index = {
	.statementTag = T_IndexStmt,

	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessIndexStmt,
	.postprocess = PostprocessIndexStmt,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(Any_Index);

static DistributeObjectOps Any_Reindex = {
	.statementTag = T_ReindexStmt,

	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessReindexStmt,
	.postprocess = NULL,
	.address = NULL,
};
REGISTER_DISTRIBUTED_OPERATION(Any_Reindex);

static DistributeObjectOps Any_Rename = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessRenameStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Attribute_Rename = {
	.deparse = DeparseRenameAttributeStmt,
	.qualify = QualifyRenameAttributeStmt,
	.preprocess = PreprocessRenameAttributeStmt,
	.postprocess = NULL,
	.address = RenameAttributeStmtObjectAddress,
};
static DistributeObjectOps Collation_AlterObjectSchema = {
	.deparse = DeparseAlterCollationSchemaStmt,
	.qualify = QualifyAlterCollationSchemaStmt,
	.preprocess = PreprocessAlterCollationSchemaStmt,
	.postprocess = PostprocessAlterCollationSchemaStmt,
	.address = AlterCollationSchemaStmtObjectAddress,
};
static DistributeObjectOps Collation_AlterOwner = {
	.deparse = DeparseAlterCollationOwnerStmt,
	.qualify = QualifyAlterCollationOwnerStmt,
	.preprocess = PreprocessAlterCollationOwnerStmt,
	.postprocess = NULL,
	.address = AlterCollationOwnerObjectAddress,
};
static DistributeObjectOps Collation_Define = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessDefineCollationStmt,
	.address = DefineCollationStmtObjectAddress,
};
static DistributeObjectOps Collation_Drop = {
	.deparse = DeparseDropCollationStmt,
	.qualify = QualifyDropCollationStmt,
	.preprocess = PreprocessDropCollationStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Collation_Rename = {
	.deparse = DeparseRenameCollationStmt,
	.qualify = QualifyRenameCollationStmt,
	.preprocess = PreprocessRenameCollationStmt,
	.postprocess = NULL,
	.address = RenameCollationStmtObjectAddress,
};
static DistributeObjectOps Extension_AlterObjectSchema = {
	.deparse = DeparseAlterExtensionSchemaStmt,
	.qualify = NULL,
	.preprocess = PreprocessAlterExtensionSchemaStmt,
	.postprocess = PostprocessAlterExtensionSchemaStmt,
	.address = AlterExtensionSchemaStmtObjectAddress,
};
static DistributeObjectOps Extension_Drop = {
	.deparse = DeparseDropExtensionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropExtensionStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps ForeignTable_AlterTable = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterTableStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Function_AlterObjectDepends = {
	.deparse = DeparseAlterFunctionDependsStmt,
	.qualify = QualifyAlterFunctionDependsStmt,
	.preprocess = PreprocessAlterFunctionDependsStmt,
	.postprocess = NULL,
	.address = AlterFunctionDependsStmtObjectAddress,
};
static DistributeObjectOps Function_AlterObjectSchema = {
	.deparse = DeparseAlterFunctionSchemaStmt,
	.qualify = QualifyAlterFunctionSchemaStmt,
	.preprocess = PreprocessAlterFunctionSchemaStmt,
	.postprocess = PostprocessAlterFunctionSchemaStmt,
	.address = AlterFunctionSchemaStmtObjectAddress,
};
static DistributeObjectOps Function_AlterOwner = {
	.deparse = DeparseAlterFunctionOwnerStmt,
	.qualify = QualifyAlterFunctionOwnerStmt,
	.preprocess = PreprocessAlterFunctionOwnerStmt,
	.postprocess = NULL,
	.address = AlterFunctionOwnerObjectAddress,
};
static DistributeObjectOps Function_Drop = {
	.deparse = DeparseDropFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropFunctionStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Function_Rename = {
	.deparse = DeparseRenameFunctionStmt,
	.qualify = QualifyRenameFunctionStmt,
	.preprocess = PreprocessRenameFunctionStmt,
	.postprocess = NULL,
	.address = RenameFunctionStmtObjectAddress,
};
static DistributeObjectOps Index_AlterTable = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterTableStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Index_Drop = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessDropIndexStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Policy_Drop = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessDropPolicyStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Procedure_AlterObjectDepends = {
	.deparse = DeparseAlterFunctionDependsStmt,
	.qualify = QualifyAlterFunctionDependsStmt,
	.preprocess = PreprocessAlterFunctionDependsStmt,
	.postprocess = NULL,
	.address = AlterFunctionDependsStmtObjectAddress,
};
static DistributeObjectOps Procedure_AlterObjectSchema = {
	.deparse = DeparseAlterFunctionSchemaStmt,
	.qualify = QualifyAlterFunctionSchemaStmt,
	.preprocess = PreprocessAlterFunctionSchemaStmt,
	.postprocess = PostprocessAlterFunctionSchemaStmt,
	.address = AlterFunctionSchemaStmtObjectAddress,
};
static DistributeObjectOps Procedure_AlterOwner = {
	.deparse = DeparseAlterFunctionOwnerStmt,
	.qualify = QualifyAlterFunctionOwnerStmt,
	.preprocess = PreprocessAlterFunctionOwnerStmt,
	.postprocess = NULL,
	.address = AlterFunctionOwnerObjectAddress,
};
static DistributeObjectOps Procedure_Drop = {
	.deparse = DeparseDropFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropFunctionStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Procedure_Rename = {
	.deparse = DeparseRenameFunctionStmt,
	.qualify = QualifyRenameFunctionStmt,
	.preprocess = PreprocessRenameFunctionStmt,
	.postprocess = NULL,
	.address = RenameFunctionStmtObjectAddress,
};
static DistributeObjectOps Routine_AlterObjectDepends = {
	.deparse = DeparseAlterFunctionDependsStmt,
	.qualify = QualifyAlterFunctionDependsStmt,
	.preprocess = PreprocessAlterFunctionDependsStmt,
	.postprocess = NULL,
	.address = AlterFunctionDependsStmtObjectAddress,
};
static DistributeObjectOps Routine_AlterObjectSchema = {
	.deparse = DeparseAlterFunctionSchemaStmt,
	.qualify = QualifyAlterFunctionSchemaStmt,
	.preprocess = PreprocessAlterFunctionSchemaStmt,
	.postprocess = PostprocessAlterFunctionSchemaStmt,
	.address = AlterFunctionSchemaStmtObjectAddress,
};
static DistributeObjectOps Routine_AlterOwner = {
	.deparse = DeparseAlterFunctionOwnerStmt,
	.qualify = QualifyAlterFunctionOwnerStmt,
	.preprocess = PreprocessAlterFunctionOwnerStmt,
	.postprocess = NULL,
	.address = AlterFunctionOwnerObjectAddress,
};
static DistributeObjectOps Routine_Drop = {
	.deparse = DeparseDropFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropFunctionStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Routine_Rename = {
	.deparse = DeparseRenameFunctionStmt,
	.qualify = QualifyRenameFunctionStmt,
	.preprocess = PreprocessRenameFunctionStmt,
	.postprocess = NULL,
	.address = RenameFunctionStmtObjectAddress,
};
static DistributeObjectOps Schema_Drop = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessDropSchemaStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Schema_Grant = {
	.deparse = DeparseGrantOnSchemaStmt,
	.qualify = NULL,
	.preprocess = PreprocessGrantOnSchemaStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Table_AlterTable = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterTableStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Table_AlterObjectSchema = {
	.deparse = DeparseAlterTableSchemaStmt,
	.qualify = QualifyAlterTableSchemaStmt,
	.preprocess = PreprocessAlterTableSchemaStmt,
	.postprocess = PostprocessAlterTableSchemaStmt,
	.address = AlterTableSchemaStmtObjectAddress,
};
static DistributeObjectOps Table_Drop = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessDropTableStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Type_AlterObjectSchema = {
	.deparse = DeparseAlterTypeSchemaStmt,
	.qualify = QualifyAlterTypeSchemaStmt,
	.preprocess = PreprocessAlterTypeSchemaStmt,
	.postprocess = PostprocessAlterTypeSchemaStmt,
	.address = AlterTypeSchemaStmtObjectAddress,
};
static DistributeObjectOps Type_AlterOwner = {
	.deparse = DeparseAlterTypeOwnerStmt,
	.qualify = QualifyAlterTypeOwnerStmt,
	.preprocess = PreprocessAlterTypeOwnerStmt,
	.postprocess = NULL,
	.address = AlterTypeOwnerObjectAddress,
};
static DistributeObjectOps Type_AlterTable = {
	.deparse = DeparseAlterTypeStmt,
	.qualify = QualifyAlterTypeStmt,
	.preprocess = PreprocessAlterTypeStmt,
	.postprocess = NULL,
	.address = AlterTypeStmtObjectAddress,
};
static DistributeObjectOps Type_Drop = {
	.deparse = DeparseDropTypeStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropTypeStmt,
	.postprocess = NULL,
	.address = NULL,
};
static DistributeObjectOps Type_Rename = {
	.deparse = DeparseRenameTypeStmt,
	.qualify = QualifyRenameTypeStmt,
	.preprocess = PreprocessRenameTypeStmt,
	.postprocess = NULL,
	.address = RenameTypeStmtObjectAddress,
};

/* linker provided pointers */
SECTION_ARRAY(DistributeObjectOps *, dist_object_ops);

/*
 * GetDistributeObjectOps looks up the DistributeObjectOps which handles the node.
 *
 * Never returns NULL.
 */
const DistributeObjectOps *
GetDistributeObjectOps(Node *node)
{
	int i = 0;
	size_t sz = SECTION_SIZE(dist_object_ops);
	for (i = 0; i < sz; i++)
	{
		DistributeObjectOps *ops = dist_object_ops_array[i];
		if (node->type == ops->statementTag)
		{
			return ops;
		}
	}

	/* multi level statements */
	switch (nodeTag(node))
	{
		case T_AlterObjectDependsStmt:
		{
			AlterObjectDependsStmt *stmt = castNode(AlterObjectDependsStmt, node);
			switch (stmt->objectType)
			{
				case OBJECT_FUNCTION:
				{
					return &Function_AlterObjectDepends;
				}

				case OBJECT_PROCEDURE:
				{
					return &Procedure_AlterObjectDepends;
				}

				case OBJECT_ROUTINE:
				{
					return &Routine_AlterObjectDepends;
				}

				default:
				{
					return &NoDistributeOps;
				}
			}
		}

		case T_AlterObjectSchemaStmt:
		{
			AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
			switch (stmt->objectType)
			{
				case OBJECT_AGGREGATE:
				{
					return &Aggregate_AlterObjectSchema;
				}

				case OBJECT_COLLATION:
				{
					return &Collation_AlterObjectSchema;
				}

				case OBJECT_EXTENSION:
				{
					return &Extension_AlterObjectSchema;
				}

				case OBJECT_FUNCTION:
				{
					return &Function_AlterObjectSchema;
				}

				case OBJECT_PROCEDURE:
				{
					return &Procedure_AlterObjectSchema;
				}

				case OBJECT_ROUTINE:
				{
					return &Routine_AlterObjectSchema;
				}

				case OBJECT_TABLE:
				{
					return &Table_AlterObjectSchema;
				}

				case OBJECT_TYPE:
				{
					return &Type_AlterObjectSchema;
				}

				default:
				{
					return &NoDistributeOps;
				}
			}
		}

		case T_AlterOwnerStmt:
		{
			AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
			switch (stmt->objectType)
			{
				case OBJECT_AGGREGATE:
				{
					return &Aggregate_AlterOwner;
				}

				case OBJECT_COLLATION:
				{
					return &Collation_AlterOwner;
				}

				case OBJECT_FUNCTION:
				{
					return &Function_AlterOwner;
				}

				case OBJECT_PROCEDURE:
				{
					return &Procedure_AlterOwner;
				}

				case OBJECT_ROUTINE:
				{
					return &Routine_AlterOwner;
				}

				case OBJECT_TYPE:
				{
					return &Type_AlterOwner;
				}

				default:
				{
					return &NoDistributeOps;
				}
			}
		}

		case T_AlterTableStmt:
		{
			AlterTableStmt *stmt = castNode(AlterTableStmt, node);
			switch (stmt->relkind)
			{
				case OBJECT_TYPE:
				{
					return &Type_AlterTable;
				}

				case OBJECT_TABLE:
				{
					return &Table_AlterTable;
				}

				case OBJECT_FOREIGN_TABLE:
				{
					return &ForeignTable_AlterTable;
				}

				case OBJECT_INDEX:
				{
					return &Index_AlterTable;
				}

				default:
				{
					return &NoDistributeOps;
				}
			}
		}

		case T_DefineStmt:
		{
			DefineStmt *stmt = castNode(DefineStmt, node);
			switch (stmt->kind)
			{
				case OBJECT_AGGREGATE:
				{
					return &Aggregate_Define;
				}

				case OBJECT_COLLATION:
				{
					return &Collation_Define;
				}

				default:
				{
					return &NoDistributeOps;
				}
			}
		}

		case T_DropStmt:
		{
			DropStmt *stmt = castNode(DropStmt, node);
			switch (stmt->removeType)
			{
				case OBJECT_AGGREGATE:
				{
					return &Aggregate_Drop;
				}

				case OBJECT_COLLATION:
				{
					return &Collation_Drop;
				}

				case OBJECT_EXTENSION:
				{
					return &Extension_Drop;
				}

				case OBJECT_FUNCTION:
				{
					return &Function_Drop;
				}

				case OBJECT_INDEX:
				{
					return &Index_Drop;
				}

				case OBJECT_POLICY:
				{
					return &Policy_Drop;
				}

				case OBJECT_PROCEDURE:
				{
					return &Procedure_Drop;
				}

				case OBJECT_ROUTINE:
				{
					return &Routine_Drop;
				}

				case OBJECT_SCHEMA:
				{
					return &Schema_Drop;
				}

				case OBJECT_TABLE:
				{
					return &Table_Drop;
				}

				case OBJECT_TYPE:
				{
					return &Type_Drop;
				}

				default:
				{
					return &NoDistributeOps;
				}
			}
		}

		case T_GrantStmt:
		{
			GrantStmt *stmt = castNode(GrantStmt, node);
			switch (stmt->objtype)
			{
				case OBJECT_SCHEMA:
				{
					return &Schema_Grant;
				}

				default:
				{
					return &Any_Grant;
				}
			}
		}

		case T_RenameStmt:
		{
			RenameStmt *stmt = castNode(RenameStmt, node);
			switch (stmt->renameType)
			{
				case OBJECT_AGGREGATE:
				{
					return &Aggregate_Rename;
				}

				case OBJECT_ATTRIBUTE:
				{
					return &Attribute_Rename;
				}

				case OBJECT_COLLATION:
				{
					return &Collation_Rename;
				}

				case OBJECT_FUNCTION:
				{
					return &Function_Rename;
				}

				case OBJECT_PROCEDURE:
				{
					return &Procedure_Rename;
				}

				case OBJECT_ROUTINE:
				{
					return &Routine_Rename;
				}

				case OBJECT_TYPE:
				{
					return &Type_Rename;
				}

				default:
				{
					return &Any_Rename;
				}
			}
		}

		default:
		{
			return &NoDistributeOps;
		}
	}
}
