/*-------------------------------------------------------------------------
 *
 * commands.h
 *    Declarations for public functions and variables used for all commands
 *    and DDL operations for citus. All declarations are grouped by the
 *    file that implements them.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_COMMANDS_H
#define CITUS_COMMANDS_H

#include "postgres.h"

#include "utils/rel.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"

/*
 * DistributeObjectOps specifies handlers for node/object type pairs.
 * Instances of this type should all be declared in deparse.c.
 * Handlers expect to receive the same Node passed to GetDistributeObjectOps.
 * Handlers may be NULL.
 *
 * Handlers:
 * deparse: return a string representation of the node.
 * qualify: replace names in tree with qualified names.
 * preprocess: executed before standard_ProcessUtility.
 * postprocess: executed after standard_ProcessUtility.
 * address: return an ObjectAddress for the subject of the statement.
 *          2nd parameter is missing_ok.
 *
 * preprocess/postprocess return a List of DDLJobs.
 */
typedef struct DistributeObjectOps
{
	char * (*deparse)(Node *);
	void (*qualify)(Node *);
	List * (*preprocess)(Node *, const char *);
	List * (*postprocess)(Node *, const char *);
	ObjectAddress (*address)(Node *, bool);
} DistributeObjectOps;

typedef struct DistributedObjectOpsContainer
{
	NodeTag type;

	/*
	 * Nested information is only set for statements that can operate on multiple
	 * different objects like ALTER ... SET SCHEMA. The object type is encoded in a field
	 * in the statement.
	 *
	 * bool nested
	 *     signals this container describes a nested tyoe
	 * size_t nestedOffset
	 *     the offest of the ObjectType field within the statement
	 * ObjectType nestedType
	 *     the object type the DistributedObjectOps of this container operates on
	 */
	bool nested;
	size_t nestedOffset;
	ObjectType nestedType;

	DistributeObjectOps *ops;
} DistributedObjectOpsContainer;

/* Marco for registering a DistributedObjectOps struct */
/* works for mac */

/*
 * This is some crazy magic that helps produce __BASE__247
 * Vanilla interpolation of __BASE__##__LINE__ would produce __BASE____LINE__
 * I still can't figure out why it works, but it has to do with macro resolution ordering
 */
#define PP_CAT3(a, b, c) PP_CAT3_I(a, b, c)
#define PP_CAT3_I(a, b, c) PP_CAT_II(~, a ## b ## c)
#define PP_CAT(a, b) PP_CAT_I(a, b)
#define PP_CAT_I(a, b) PP_CAT_II(~, a ## b)
#define PP_CAT_II(p, res) res

#define REGISTER_SECTION_POINTER(section_name, ptr) \
	void *PP_CAT3(section_name, ptr, __COUNTER__) \
	__attribute__((unused)) \
	__attribute__((section("__DATA," # section_name))) \
		= &ptr


#define SECTION_ARRAY(_type, _section) \
	extern _type __start_ ## _section[] __asm("section$start$__DATA$" # _section); \
	extern _type __stop_ ## _section[] __asm("section$end$__DATA$" # _section); \
	_type *_section ## _array = __start_ ## _section;

#define SECTION_SIZE(sect) \
	((size_t) ((__stop_ ## sect - __start_ ## sect)))

#define REGISTER_DISTRIBUTED_OPERATION(stmt, opsvar) \
	static DistributedObjectOpsContainer PP_CAT(opscontainer, opsvar) = { \
		.type = T_ ## stmt, \
		.ops = &opsvar, \
	}; \
	REGISTER_SECTION_POINTER(opscontainer, PP_CAT(opscontainer, opsvar))

#define REGISTER_DISTRIBUTED_OPERATION_NESTED(stmt, objectVarName, objtype, opsvar) \
	static DistributedObjectOpsContainer PP_CAT3(opscontainer_, stmt, objtype) = { \
		.type = T_ ## stmt, \
		.nested = true, \
		.nestedOffset = offsetof(stmt, objectVarName), \
		.nestedType = objtype, \
		.ops = &opsvar, \
	}; \
	REGISTER_SECTION_POINTER(opscontainer, PP_CAT3(opscontainer_, stmt, objtype))

#define REGISTER_DISTRIBUTED_OPERATION_NESTED_NEW(stmt, objectVarName, objtype) \
	static DistributeObjectOps PP_CAT3(distops, stmt, objtype); \
	REGISTER_DISTRIBUTED_OPERATION_NESTED(stmt, objectVarName, objtype, \
										  PP_CAT3(distops, stmt, objtype)); \
	static DistributeObjectOps PP_CAT3(distops, stmt, objtype) =

const DistributeObjectOps * GetDistributeObjectOps(Node *node);

/* cluster.c - forward declarations */
extern List * PreprocessClusterStmt(Node *node, const char *clusterCommand);


/* call.c */
extern bool CallDistributedProcedureRemotely(CallStmt *callStmt, DestReceiver *dest);


/* collation.c - forward declarations */
extern char * CreateCollationDDL(Oid collationId);
extern List * CreateCollationDDLsIdempotent(Oid collationId);
extern ObjectAddress AlterCollationOwnerObjectAddress(Node *stmt, bool missing_ok);
extern List * PreprocessDropCollationStmt(Node *stmt, const char *queryString);
extern List * PreprocessAlterCollationOwnerStmt(Node *stmt, const char *queryString);
extern List * PreprocessAlterCollationSchemaStmt(Node *stmt, const char *queryString);
extern List * PreprocessRenameCollationStmt(Node *stmt, const char *queryString);
extern ObjectAddress RenameCollationStmtObjectAddress(Node *stmt, bool missing_ok);
extern ObjectAddress AlterCollationSchemaStmtObjectAddress(Node *stmt,
														   bool missing_ok);
extern List * PostprocessAlterCollationSchemaStmt(Node *stmt, const char *queryString);
extern char * GenerateBackupNameForCollationCollision(const ObjectAddress *address);
extern ObjectAddress DefineCollationStmtObjectAddress(Node *stmt, bool missing_ok);
extern List * PostprocessDefineCollationStmt(Node *stmt, const char *queryString);

/* extension.c - forward declarations */
extern bool IsCreateAlterExtensionUpdateCitusStmt(Node *parsetree);
extern void ErrorIfUnstableCreateOrAlterExtensionStmt(Node *parsetree);
extern List * PostprocessCreateExtensionStmt(Node *stmt, const char *queryString);
extern List * PreprocessDropExtensionStmt(Node *stmt, const char *queryString);
extern List * PreprocessAlterExtensionSchemaStmt(Node *stmt,
												 const char *queryString);
extern List * PostprocessAlterExtensionSchemaStmt(Node *stmt,
												  const char *queryString);
extern List * PreprocessAlterExtensionUpdateStmt(Node *stmt,
												 const char *queryString);
extern List * PreprocessAlterExtensionContentsStmt(Node *node,
												   const char *queryString);
extern List * CreateExtensionDDLCommand(const ObjectAddress *extensionAddress);
extern ObjectAddress AlterExtensionSchemaStmtObjectAddress(Node *stmt,
														   bool missing_ok);
extern ObjectAddress AlterExtensionUpdateStmtObjectAddress(Node *stmt,
														   bool missing_ok);


/* foreign_constraint.c - forward declarations */
extern bool ConstraintIsAForeignKeyToReferenceTable(char *constraintName,
													Oid leftRelationId);
extern void ErrorIfUnsupportedForeignConstraintExists(Relation relation,
													  char distributionMethod,
													  Var *distributionColumn,
													  uint32 colocationId);
extern bool ColumnAppearsInForeignKeyToReferenceTable(char *columnName, Oid
													  relationId);
extern List * GetTableForeignConstraintCommands(Oid relationId);
extern bool HasForeignKeyToReferenceTable(Oid relationId);
extern bool TableReferenced(Oid relationId);
extern bool TableReferencing(Oid relationId);
extern bool ConstraintIsAForeignKey(char *constraintName, Oid relationId);


/* function.c - forward declarations */
extern List * PreprocessCreateFunctionStmt(Node *stmt, const char *queryString);
extern List * PostprocessCreateFunctionStmt(Node *stmt,
											const char *queryString);
extern ObjectAddress CreateFunctionStmtObjectAddress(Node *stmt,
													 bool missing_ok);
extern ObjectAddress DefineAggregateStmtObjectAddress(Node *stmt,
													  bool missing_ok);
extern List * PreprocessAlterFunctionStmt(Node *stmt, const char *queryString);
extern ObjectAddress AlterFunctionStmtObjectAddress(Node *stmt,
													bool missing_ok);
extern List * PreprocessRenameFunctionStmt(Node *stmt, const char *queryString);
extern ObjectAddress RenameFunctionStmtObjectAddress(Node *stmt,
													 bool missing_ok);
extern List * PreprocessAlterFunctionOwnerStmt(Node *stmt, const char *queryString);
extern ObjectAddress AlterFunctionOwnerObjectAddress(Node *stmt,
													 bool missing_ok);
extern List * PreprocessAlterFunctionSchemaStmt(Node *stmt, const char *queryString);
extern ObjectAddress AlterFunctionSchemaStmtObjectAddress(Node *stmt,
														  bool missing_ok);
extern List * PostprocessAlterFunctionSchemaStmt(Node *stmt,
												 const char *queryString);
extern List * PreprocessDropFunctionStmt(Node *stmt, const char *queryString);
extern List * PreprocessAlterFunctionDependsStmt(Node *stmt,
												 const char *queryString);
extern ObjectAddress AlterFunctionDependsStmtObjectAddress(Node *stmt,
														   bool missing_ok);

/* index.c - forward declarations */
extern bool IsIndexRenameStmt(RenameStmt *renameStmt);
extern List * PreprocessIndexStmt(Node *createIndexStatement,
								  const char *createIndexCommand);
extern List * PreprocessReindexStmt(Node *ReindexStatement,
									const char *ReindexCommand);
extern List * PreprocessDropIndexStmt(Node *dropIndexStatement,
									  const char *dropIndexCommand);
extern List * PostprocessIndexStmt(Node *node,
								   const char *queryString);
extern void ErrorIfUnsupportedAlterIndexStmt(AlterTableStmt *alterTableStatement);

/* objectaddress.c - forward declarations */
extern ObjectAddress CreateExtensionStmtObjectAddress(Node *stmt, bool missing_ok);
extern ObjectAddress AlterExtensionStmtObjectAddress(Node *stmt, bool missing_ok);


/* policy.c -  forward declarations */
extern List * CreatePolicyCommands(Oid relationId);
extern void ErrorIfUnsupportedPolicy(Relation relation);
extern void ErrorIfUnsupportedPolicyExpr(Node *expr);
extern List * PreprocessCreatePolicyStmt(Node *node, const char *queryString);
extern List * PreprocessAlterPolicyStmt(Node *node, const char *queryString);
extern List * PreprocessDropPolicyStmt(Node *stmt, const char *queryString);
extern bool IsPolicyRenameStmt(RenameStmt *stmt);
extern void CreatePolicyEventExtendNames(CreatePolicyStmt *stmt, const char *schemaName,
										 uint64 shardId);
extern void AlterPolicyEventExtendNames(AlterPolicyStmt *stmt, const char *schemaName,
										uint64 shardId);
extern void RenamePolicyEventExtendNames(RenameStmt *stmt, const char *schemaName, uint64
										 shardId);
extern void DropPolicyEventExtendNames(DropStmt *stmt, const char *schemaName, uint64
									   shardId);


/* rename.c - forward declarations*/
extern List * PreprocessRenameStmt(Node *renameStmt, const char *renameCommand);
extern void ErrorIfUnsupportedRenameStmt(RenameStmt *renameStmt);
extern List * PreprocessRenameAttributeStmt(Node *stmt, const char *queryString);


/* role.c - forward declarations*/
extern List * PostprocessAlterRoleStmt(Node *stmt, const char *queryString);
extern List * GenerateAlterRoleIfExistsCommandAllRoles(void);


/* schema.c - forward declarations */
extern List * PreprocessDropSchemaStmt(Node *dropSchemaStatement,
									   const char *queryString);
extern List * PreprocessAlterObjectSchemaStmt(Node *alterObjectSchemaStmt,
											  const char *alterObjectSchemaCommand);
extern List * PreprocessGrantOnSchemaStmt(Node *node, const char *queryString);


/* sequence.c - forward declarations */
extern void ErrorIfUnsupportedSeqStmt(CreateSeqStmt *createSeqStmt);
extern void ErrorIfDistributedAlterSeqOwnedBy(AlterSeqStmt *alterSeqStmt);


/* subscription.c - forward declarations */
extern Node * ProcessCreateSubscriptionStmt(CreateSubscriptionStmt *createSubStmt);


/* table.c - forward declarations */
extern List * PreprocessDropTableStmt(Node *stmt, const char *queryString);
extern List * PostprocessCreateTableStmtPartitionOf(CreateStmt *createStatement,
													const char *queryString);
extern List * PostprocessAlterTableStmtAttachPartition(
	AlterTableStmt *alterTableStatement,
	const char *queryString);
extern List * PostprocessAlterTableSchemaStmt(Node *node, const char *queryString);
extern List * PreprocessAlterTableStmt(Node *node, const char *alterTableCommand);
extern List * PreprocessAlterTableMoveAllStmt(Node *node, const char *queryString);
extern List * PreprocessAlterTableSchemaStmt(Node *node, const char *queryString);
extern Node * WorkerProcessAlterTableStmt(AlterTableStmt *alterTableStatement,
										  const char *alterTableCommand);
extern bool IsAlterTableRenameStmt(RenameStmt *renameStmt);
extern void ErrorIfAlterDropsPartitionColumn(AlterTableStmt *alterTableStatement);
extern void PostprocessAlterTableStmt(AlterTableStmt *pStmt);
extern void ErrorUnsupportedAlterTableAddColumn(Oid relationId, AlterTableCmd *command,
												Constraint *constraint);
extern void ErrorIfUnsupportedConstraint(Relation relation, char distributionMethod,
										 Var *distributionColumn, uint32 colocationId);
extern ObjectAddress AlterTableSchemaStmtObjectAddress(Node *stmt,
													   bool missing_ok);


/* truncate.c - forward declarations */
extern void PostprocessTruncateStatement(TruncateStmt *truncateStatement);

/* type.c - forward declarations */
extern List * PreprocessCompositeTypeStmt(Node *stmt, const char *queryString);
extern List * PostprocessCompositeTypeStmt(Node *stmt, const char *queryString);
extern List * PreprocessAlterTypeStmt(Node *stmt, const char *queryString);
extern List * PreprocessCreateEnumStmt(Node *stmt, const char *queryString);
extern List * PostprocessCreateEnumStmt(Node *stmt, const char *queryString);
extern List * PreprocessAlterEnumStmt(Node *stmt, const char *queryString);
extern List * PostprocessAlterEnumStmt(Node *stmt, const char *queryString);
extern List * PreprocessDropTypeStmt(Node *stmt, const char *queryString);
extern List * PreprocessRenameTypeStmt(Node *stmt, const char *queryString);
extern List * PreprocessRenameTypeAttributeStmt(Node *stmt, const char *queryString);
extern List * PreprocessAlterTypeSchemaStmt(Node *stmt, const char *queryString);
extern List * PreprocessAlterTypeOwnerStmt(Node *stmt, const char *queryString);
extern List * PostprocessAlterTypeSchemaStmt(Node *stmt, const char *queryString);
extern Node * CreateTypeStmtByObjectAddress(const ObjectAddress *address);
extern ObjectAddress CompositeTypeStmtObjectAddress(Node *stmt, bool missing_ok);
extern ObjectAddress CreateEnumStmtObjectAddress(Node *stmt, bool missing_ok);
extern ObjectAddress AlterTypeStmtObjectAddress(Node *stmt, bool missing_ok);
extern ObjectAddress AlterEnumStmtObjectAddress(Node *stmt, bool missing_ok);
extern ObjectAddress RenameTypeStmtObjectAddress(Node *stmt, bool missing_ok);
extern ObjectAddress AlterTypeSchemaStmtObjectAddress(Node *stmt,
													  bool missing_ok);
extern ObjectAddress RenameTypeAttributeStmtObjectAddress(Node *stmt,
														  bool missing_ok);
extern ObjectAddress AlterTypeOwnerObjectAddress(Node *stmt, bool missing_ok);
extern List * CreateTypeDDLCommandsIdempotent(const ObjectAddress *typeAddress);
extern char * GenerateBackupNameForTypeCollision(const ObjectAddress *address);

/* function.c - forward declarations */
extern List * CreateFunctionDDLCommandsIdempotent(const ObjectAddress *functionAddress);
extern char * GetFunctionDDLCommand(const RegProcedure funcOid, bool useCreateOrReplace);
extern char * GenerateBackupNameForProcCollision(const ObjectAddress *address);
extern ObjectWithArgs * ObjectWithArgsFromOid(Oid funcOid);

/* vacuum.c - froward declarations */
extern void PostprocessVacuumStmt(VacuumStmt *vacuumStmt, const char *vacuumCommand);

extern bool ShouldPropagateSetCommand(VariableSetStmt *setStmt);
extern void PostprocessVariableSetStmt(VariableSetStmt *setStmt, const char *setCommand);

#endif /*CITUS_COMMANDS_H */
