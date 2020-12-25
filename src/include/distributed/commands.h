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

#define CITUS_TRUNCATE_TRIGGER_NAME "citus_truncate_trigger"

const DistributeObjectOps * GetDistributeObjectOps(Node *node);

/*
 * Flags that can be passed to GetForeignKeyOids to indicate
 * which foreign key constraint OIDs are to be extracted
 */
typedef enum ExtractForeignKeyConstraintsMode
{
	/* extract the foreign key OIDs where the table is the referencing one */
	INCLUDE_REFERENCING_CONSTRAINTS = 1 << 0,

	/* extract the foreign key OIDs the table is the referenced one */
	INCLUDE_REFERENCED_CONSTRAINTS = 1 << 1,

	/* exclude the self-referencing foreign keys */
	EXCLUDE_SELF_REFERENCES = 1 << 2,

	/* any combination of the 4 flags below is supported */
	/* include foreign keys when the other table is a distributed table*/
	INCLUDE_DISTRIBUTED_TABLES = 1 << 3,

	/* include foreign keys when the other table is a reference table*/
	INCLUDE_REFERENCE_TABLES = 1 << 4,

	/* include foreign keys when the other table is a citus local table*/
	INCLUDE_CITUS_LOCAL_TABLES = 1 << 5,

	/* include foreign keys when the other table is a Postgres local table*/
	INCLUDE_LOCAL_TABLES = 1 << 6,

	/* include foreign keys regardless of the other table's type */
	INCLUDE_ALL_TABLE_TYPES = INCLUDE_DISTRIBUTED_TABLES | INCLUDE_REFERENCE_TABLES |
							  INCLUDE_CITUS_LOCAL_TABLES | INCLUDE_LOCAL_TABLES
} ExtractForeignKeyConstraintMode;


/*
 * Flags that can be passed to GetForeignKeyIdsForColumn to
 * indicate whether relationId argument should match:
 *   - referencing relation or,
 *   - referenced relation,
 *  or we are searching for both sides.
 */
typedef enum SearchForeignKeyColumnFlags
{
	/* relationId argument should match referencing relation */
	SEARCH_REFERENCING_RELATION = 1 << 0,

	/* relationId argument should match referenced relation */
	SEARCH_REFERENCED_RELATION = 1 << 1,

	/* callers can also pass union of above flags */
} SearchForeignKeyColumnFlags;


/* cluster.c - forward declarations */
extern List * PreprocessClusterStmt(Node *node, const char *clusterCommand);

/* index.c */
typedef void (*PGIndexProcessor)(Form_pg_index, List **);


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
extern bool IsDropCitusExtensionStmt(Node *parsetree);
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
extern void PostprocessAlterExtensionCitusUpdateStmt(Node *node);
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
													  char referencingReplicationModel,
													  char distributionMethod,
													  Var *distributionColumn,
													  uint32 colocationId);
extern void ErrorOutForFKeyBetweenPostgresAndCitusLocalTable(Oid localTableId);
extern bool ColumnAppearsInForeignKey(char *columnName, Oid relationId);
extern bool ColumnAppearsInForeignKeyToReferenceTable(char *columnName, Oid
													  relationId);
extern List * GetReferencingForeignConstaintCommands(Oid relationOid);
extern bool HasForeignKeyToCitusLocalTable(Oid relationId);
extern bool HasForeignKeyToReferenceTable(Oid relationOid);
extern bool TableReferenced(Oid relationOid);
extern bool TableReferencing(Oid relationOid);
extern bool ConstraintIsAUniquenessConstraint(char *inputConstaintName, Oid relationId);
extern bool ConstraintIsAForeignKey(char *inputConstaintName, Oid relationOid);
extern bool ConstraintWithNameIsOfType(char *inputConstaintName, Oid relationId,
									   char targetConstraintType);
extern bool ConstraintWithIdIsOfType(Oid constraintId, char targetConstraintType);
extern void ErrorIfTableHasExternalForeignKeys(Oid relationId);
extern List * GetForeignKeyOids(Oid relationId, int flags);
extern Oid GetReferencedTableId(Oid foreignKeyId);


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


/* grant.c - forward declarations */
extern List * PreprocessGrantStmt(Node *node, const char *queryString);


/* index.c - forward declarations */
extern bool IsIndexRenameStmt(RenameStmt *renameStmt);
extern List * PreprocessIndexStmt(Node *createIndexStatement,
								  const char *createIndexCommand);
extern char * ChooseIndexName(const char *tabname, Oid namespaceId,
							  List *colnames, List *exclusionOpNames,
							  bool primary, bool isconstraint);
extern char * ChooseIndexNameAddition(List *colnames);
extern List * ChooseIndexColumnNames(List *indexElems);
extern List * PreprocessReindexStmt(Node *ReindexStatement,
									const char *ReindexCommand);
extern List * PreprocessDropIndexStmt(Node *dropIndexStatement,
									  const char *dropIndexCommand);
extern List * PostprocessIndexStmt(Node *node,
								   const char *queryString);
extern void ErrorIfUnsupportedAlterIndexStmt(AlterTableStmt *alterTableStatement);
extern void MarkIndexValid(IndexStmt *indexStmt);
extern List * ExecuteFunctionOnEachTableIndex(Oid relationId, PGIndexProcessor
											  pgIndexProcessor);

/* objectaddress.c - forward declarations */
extern ObjectAddress CreateExtensionStmtObjectAddress(Node *stmt, bool missing_ok);


/* policy.c -  forward declarations */
extern List * CreatePolicyCommands(Oid relationId);
extern void ErrorIfUnsupportedPolicy(Relation relation);
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
extern List * PreprocessAlterRoleSetStmt(Node *stmt, const char *queryString);
extern List * GenerateAlterRoleSetCommandForRole(Oid roleid);
extern ObjectAddress AlterRoleStmtObjectAddress(Node *node,
												bool missing_ok);
extern ObjectAddress AlterRoleSetStmtObjectAddress(Node *node,
												   bool missing_ok);
extern List * GenerateCreateOrAlterRoleCommand(Oid roleOid);

/* schema.c - forward declarations */
extern List * PreprocessDropSchemaStmt(Node *dropSchemaStatement,
									   const char *queryString);
extern List * PreprocessAlterObjectSchemaStmt(Node *alterObjectSchemaStmt,
											  const char *alterObjectSchemaCommand);
extern List * PreprocessGrantOnSchemaStmt(Node *node, const char *queryString);
extern List * PreprocessAlterSchemaRenameStmt(Node *node, const char *queryString);
extern ObjectAddress AlterSchemaRenameStmtObjectAddress(Node *node, bool missing_ok);

/* sequence.c - forward declarations */
extern void ErrorIfUnsupportedSeqStmt(CreateSeqStmt *createSeqStmt);
extern void ErrorIfDistributedAlterSeqOwnedBy(AlterSeqStmt *alterSeqStmt);

/* statistics.c - forward declarations */
extern List * PreprocessCreateStatisticsStmt(Node *node, const char *queryString);
extern List * PostprocessCreateStatisticsStmt(Node *node, const char *queryString);
extern ObjectAddress CreateStatisticsStmtObjectAddress(Node *node, bool missingOk);
extern List * PreprocessDropStatisticsStmt(Node *node, const char *queryString);
extern List * PreprocessAlterStatisticsRenameStmt(Node *node, const char *queryString);
extern List * PreprocessAlterStatisticsSchemaStmt(Node *node, const char *queryString);
extern List * PostprocessAlterStatisticsSchemaStmt(Node *node, const char *queryString);
extern ObjectAddress AlterStatisticsSchemaStmtObjectAddress(Node *node, bool missingOk);
extern List * PreprocessAlterStatisticsStmt(Node *node, const char *queryString);
extern List * PreprocessAlterStatisticsOwnerStmt(Node *node, const char *queryString);
extern List * GetExplicitStatisticsCommandList(Oid relationId);
extern List * GetExplicitStatisticsSchemaIdList(Oid relationId);

/* subscription.c - forward declarations */
extern Node * ProcessCreateSubscriptionStmt(CreateSubscriptionStmt *createSubStmt);


/* table.c - forward declarations */
extern List * PreprocessDropTableStmt(Node *stmt, const char *queryString);
extern void PostprocessCreateTableStmt(CreateStmt *createStatement,
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
										 char referencingReplicationModel,
										 Var *distributionColumn, uint32 colocationId);
extern ObjectAddress AlterTableSchemaStmtObjectAddress(Node *stmt,
													   bool missing_ok);
extern List * MakeNameListFromRangeVar(const RangeVar *rel);


/* truncate.c - forward declarations */
extern void PreprocessTruncateStatement(TruncateStmt *truncateStatement);

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

/* vacuum.c - forward declarations */
extern void PostprocessVacuumStmt(VacuumStmt *vacuumStmt, const char *vacuumCommand);

/* trigger.c - forward declarations */
extern List * GetExplicitTriggerCommandList(Oid relationId);
extern HeapTuple GetTriggerTupleById(Oid triggerId, bool missingOk);
extern List * GetExplicitTriggerIdList(Oid relationId);
extern Oid get_relation_trigger_oid_compat(HeapTuple heapTuple);
extern List * PostprocessCreateTriggerStmt(Node *node, const char *queryString);
extern ObjectAddress CreateTriggerStmtObjectAddress(Node *node, bool missingOk);
extern void CreateTriggerEventExtendNames(CreateTrigStmt *createTriggerStmt,
										  char *schemaName, uint64 shardId);
extern List * PostprocessAlterTriggerRenameStmt(Node *node, const char *queryString);
extern void AlterTriggerRenameEventExtendNames(RenameStmt *renameTriggerStmt,
											   char *schemaName, uint64 shardId);
extern List * PostprocessAlterTriggerDependsStmt(Node *node, const char *queryString);
extern void AlterTriggerDependsEventExtendNames(
	AlterObjectDependsStmt *alterTriggerDependsStmt,
	char *schemaName, uint64 shardId);
extern List * PreprocessDropTriggerStmt(Node *node, const char *queryString);
extern void ErrorOutForTriggerIfNotCitusLocalTable(Oid relationId);
extern void DropTriggerEventExtendNames(DropStmt *dropTriggerStmt, char *schemaName,
										uint64 shardId);
extern List * CitusLocalTableTriggerCommandDDLJob(Oid relationId, char *triggerName,
												  const char *queryString);
extern Oid GetTriggerFunctionId(Oid triggerId);

extern bool ShouldPropagateSetCommand(VariableSetStmt *setStmt);
extern void PostprocessVariableSetStmt(VariableSetStmt *setStmt, const char *setCommand);

#endif /*CITUS_COMMANDS_H */
