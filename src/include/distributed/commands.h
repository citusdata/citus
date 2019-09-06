/*-------------------------------------------------------------------------
 *
 * commands.h
 *    Declarations for public functions and variables used for all commands
 *    and DDL operations for citus. All declarations are grouped by the
 *    file that implements them.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_COMMANDS_H
#define CITUS_COMMANDS_H

#include "postgres.h"

#include "utils/rel.h"
#include "nodes/parsenodes.h"


/* cluster.c - forward declarations */
extern List * PlanClusterStmt(ClusterStmt *clusterStmt, const char *clusterCommand);


/* extension.c - forward declarations */
extern bool IsCitusExtensionStmt(Node *parsetree);
extern void ErrorIfUnstableCreateOrAlterExtensionStmt(Node *parsetree);


/* foreign_constraint.c - forward declarations */
extern bool ConstraintIsAForeignKeyToReferenceTable(char *constraintName,
													Oid leftRelationId);
extern void ErrorIfUnsupportedForeignConstraintExists(Relation relation, char
													  distributionMethod,
													  Var *distributionColumn, uint32
													  colocationId);
extern bool ColumnAppearsInForeignKeyToReferenceTable(char *columnName, Oid
													  relationId);
extern List * GetTableForeignConstraintCommands(Oid relationId);
extern bool HasForeignKeyToReferenceTable(Oid relationId);
extern bool TableReferenced(Oid relationId);
extern bool TableReferencing(Oid relationId);
extern bool ConstraintIsAForeignKey(char *constraintName, Oid relationId);


/* grant.c - forward declarations */
extern List * PlanGrantStmt(GrantStmt *grantStmt);


/* index.c - forward declarations */
extern bool IsIndexRenameStmt(RenameStmt *renameStmt);
extern List * PlanIndexStmt(IndexStmt *createIndexStatement,
							const char *createIndexCommand);
extern void ErrorIfReindexOnDistributedTable(ReindexStmt *ReindexStatement);
extern List * PlanDropIndexStmt(DropStmt *dropIndexStatement,
								const char *dropIndexCommand);
extern void PostProcessIndexStmt(IndexStmt *indexStmt);
extern void ErrorIfUnsupportedAlterIndexStmt(AlterTableStmt *alterTableStatement);


/* policy.c -  forward declarations */
extern List * CreatePolicyCommands(Oid relationId);
extern void ErrorIfUnsupportedPolicy(Relation relation);
extern void ErrorIfUnsupportedPolicyExpr(Node *expr);
extern List * PlanCreatePolicyStmt(CreatePolicyStmt *stmt);
extern List * PlanAlterPolicyStmt(AlterPolicyStmt *stmt);
extern List * PlanDropPolicyStmt(DropStmt *stmt, const char *queryString);
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
extern List * PlanRenameStmt(RenameStmt *renameStmt, const char *renameCommand);
extern void ErrorIfUnsupportedRenameStmt(RenameStmt *renameStmt);


/* schema.c - forward declarations */
extern void ProcessDropSchemaStmt(DropStmt *dropSchemaStatement);
extern List * PlanAlterTableSchemaStmt(AlterObjectSchemaStmt *stmt,
									   const char *queryString);
extern List * PlanAlterObjectSchemaStmt(AlterObjectSchemaStmt *alterObjectSchemaStmt,
										const char *alterObjectSchemaCommand);

extern void ProcessAlterObjectSchemaStmt(AlterObjectSchemaStmt *stmt,
										 const char *queryString);


/* sequence.c - forward declarations */
extern void ErrorIfUnsupportedSeqStmt(CreateSeqStmt *createSeqStmt);
extern void ErrorIfDistributedAlterSeqOwnedBy(AlterSeqStmt *alterSeqStmt);


/* subscription.c - forward declarations */
extern Node * ProcessCreateSubscriptionStmt(CreateSubscriptionStmt *createSubStmt);


/* table.c - forward declarations */
extern void ProcessDropTableStmt(DropStmt *dropTableStatement);
extern void ProcessCreateTableStmtPartitionOf(CreateStmt *createStatement);
extern void ProcessAlterTableStmtAttachPartition(AlterTableStmt *alterTableStatement);
extern List * PlanAlterTableStmt(AlterTableStmt *alterTableStatement,
								 const char *alterTableCommand);
extern Node * WorkerProcessAlterTableStmt(AlterTableStmt *alterTableStatement,
										  const char *alterTableCommand);
extern bool IsAlterTableRenameStmt(RenameStmt *renameStmt);
extern void ErrorIfAlterDropsPartitionColumn(AlterTableStmt *alterTableStatement);
extern void PostProcessAlterTableStmt(AlterTableStmt *pStmt);
extern void ErrorUnsupportedAlterTableAddColumn(Oid relationId, AlterTableCmd *command,
												Constraint *constraint);
extern void ErrorIfUnsupportedConstraint(Relation relation, char distributionMethod,
										 Var *distributionColumn, uint32 colocationId);


/* truncate.c - forward declarations */
extern void ProcessTruncateStatement(TruncateStmt *truncateStatement);

/* type.c - forward declarations */
extern List * PlanCompositeTypeStmt(CompositeTypeStmt *stmt, const char *queryString);
extern List * PlanAlterTypeStmt(AlterTableStmt *stmt, const char *queryString);
extern List * PlanCreateEnumStmt(CreateEnumStmt *createEnumStmt, const char *queryString);
extern List * PlanAlterEnumStmt(AlterEnumStmt *stmt, const char *queryString);
extern List * PlanDropTypeStmt(DropStmt *stmt, const char *queryString);
extern List * PlanRenameTypeStmt(RenameStmt *stmt, const char *queryString);
extern List * PlanRenameTypeAttributeStmt(RenameStmt *stmt, const char *queryString);
extern List * PlanAlterTypeSchemaStmt(AlterObjectSchemaStmt *stmt,
									  const char *queryString);
extern List * PlanAlterTypeOwnerStmt(AlterOwnerStmt *stmt, const char *queryString);
extern void ProcessAlterTypeSchemaStmt(AlterObjectSchemaStmt *stmt,
									   const char *queryString);
extern Node * CreateTypeStmtByObjectAddress(const ObjectAddress *address);
extern const ObjectAddress * CompositeTypeStmtObjectAddress(CompositeTypeStmt *stmt,
															bool missing_ok);
extern const ObjectAddress * CreateEnumStmtObjectAddress(CreateEnumStmt *stmt,
														 bool missing_ok);
extern const ObjectAddress * AlterTypeStmtObjectAddress(AlterTableStmt *stmt,
														bool missing_ok);
extern const ObjectAddress * AlterEnumStmtObjectAddress(AlterEnumStmt *stmt,
														bool missing_ok);
extern const ObjectAddress * RenameTypeStmtObjectAddress(RenameStmt *stmt,
														 bool missing_ok);
extern const ObjectAddress * AlterTypeSchemaStmtObjectAddress(AlterObjectSchemaStmt *stmt,
															  bool missing_ok);
extern const ObjectAddress * RenameTypeAttributeStmtObjectAddress(RenameStmt *stmt,
																  bool missing_ok);
extern const ObjectAddress * AlterTypeOwnerObjectAddress(AlterOwnerStmt *stmt,
														 bool missing_ok);
extern DropStmt * CompositeTypeStmtToDrop(CompositeTypeStmt *stmt);
extern DropStmt * CreateEnumStmtToDrop(CreateEnumStmt *stmt);
extern List * CreateTypeDDLCommandsIdempotent(const ObjectAddress *typeAddress);

/* vacuum.c - froward declarations */
extern void ProcessVacuumStmt(VacuumStmt *vacuumStmt, const char *vacuumCommand);

extern bool ShouldPropagateSetCommand(VariableSetStmt *setStmt);
extern void ProcessVariableSetStmt(VariableSetStmt *setStmt, const char *setCommand);

#endif /*CITUS_COMMANDS_H */
