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
#include "tcop/utility.h"
#include "utils/acl.h"


extern bool AddAllLocalTablesToMetadata;

/* controlled via GUC, should be accessed via EnableLocalReferenceForeignKeys() */
extern bool EnableLocalReferenceForeignKeys;

extern bool EnableUnsafeTriggers;

extern int MaxMatViewSizeToAutoRecreate;

extern void SwitchToSequentialAndLocalExecutionIfRelationNameTooLong(Oid relationId,
																	 char *
																	 finalRelationName);
extern void SwitchToSequentialAndLocalExecutionIfPartitionNameTooLong(Oid
																	  parentRelationId,
																	  Oid
																	  partitionRelationId);


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
 * markDistribued: true if the object will be distributed.
 *
 * preprocess/postprocess return a List of DDLJobs.
 */
typedef struct DistributeObjectOps
{
	char * (*deparse)(Node *);
	void (*qualify)(Node *);
	List * (*preprocess)(Node *, const char *, ProcessUtilityContext);
	List * (*postprocess)(Node *, const char *);
	List * (*address)(Node *, bool);
	bool markDistributed;

	/* fields used by common implementations, omitted for specialized implementations */
	ObjectType objectType;

	/*
	 * Points to the varriable that contains the GUC'd feature flag, when turned off the
	 * common propagation functions will not propagate the creation of the object.
	 */
	bool *featureFlag;
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
extern List * PreprocessClusterStmt(Node *node, const char *clusterCommand,
									ProcessUtilityContext processUtilityContext);

/* common.c - forward declarations*/
extern List * PostprocessCreateDistributedObjectFromCatalogStmt(Node *stmt,
																const char *queryString);
extern List * PreprocessAlterDistributedObjectStmt(Node *stmt, const char *queryString,
												   ProcessUtilityContext
												   processUtilityContext);
extern List * PostprocessAlterDistributedObjectStmt(Node *stmt, const char *queryString);
extern List * PreprocessDropDistributedObjectStmt(Node *node, const char *queryString,
												  ProcessUtilityContext
												  processUtilityContext);

/* index.c */
typedef void (*PGIndexProcessor)(Form_pg_index, List **, int);


/* call.c */
extern bool CallDistributedProcedureRemotely(CallStmt *callStmt, DestReceiver *dest);


/* collation.c - forward declarations */
extern char * CreateCollationDDL(Oid collationId);
extern List * CreateCollationDDLsIdempotent(Oid collationId);
extern List * AlterCollationOwnerObjectAddress(Node *stmt, bool missing_ok);
extern List * RenameCollationStmtObjectAddress(Node *stmt, bool missing_ok);
extern List * AlterCollationSchemaStmtObjectAddress(Node *stmt,
													bool missing_ok);
extern char * GenerateBackupNameForCollationCollision(const ObjectAddress *address);
extern List * DefineCollationStmtObjectAddress(Node *stmt, bool missing_ok);

/* database.c - forward declarations */
extern List * AlterDatabaseOwnerObjectAddress(Node *node, bool missing_ok);
extern List * DatabaseOwnerDDLCommands(const ObjectAddress *address);

/* domain.c - forward declarations */
extern List * CreateDomainStmtObjectAddress(Node *node, bool missing_ok);
extern List * AlterDomainStmtObjectAddress(Node *node, bool missing_ok);
extern List * DomainRenameConstraintStmtObjectAddress(Node *node,
													  bool missing_ok);
extern List * AlterDomainOwnerStmtObjectAddress(Node *node, bool missing_ok);
extern List * RenameDomainStmtObjectAddress(Node *node, bool missing_ok);
extern CreateDomainStmt * RecreateDomainStmt(Oid domainOid);
extern Oid get_constraint_typid(Oid conoid);

/* extension.c - forward declarations */
extern bool IsDropCitusExtensionStmt(Node *parsetree);
extern List * GetDependentFDWsToExtension(Oid extensionId);
extern bool IsCreateAlterExtensionUpdateCitusStmt(Node *parsetree);
extern void PreprocessCreateExtensionStmtForCitusColumnar(Node *parsetree);
extern void PreprocessAlterExtensionCitusStmtForCitusColumnar(Node *parsetree);
extern void PostprocessAlterExtensionCitusStmtForCitusColumnar(Node *parsetree);
extern bool ShouldMarkRelationDistributed(Oid relationId);
extern void ErrorIfUnstableCreateOrAlterExtensionStmt(Node *parsetree);
extern List * PostprocessCreateExtensionStmt(Node *stmt, const char *queryString);
extern List * PreprocessDropExtensionStmt(Node *stmt, const char *queryString,
										  ProcessUtilityContext processUtilityContext);
extern List * PreprocessAlterExtensionSchemaStmt(Node *stmt,
												 const char *queryString,
												 ProcessUtilityContext
												 processUtilityContext);
extern List * PostprocessAlterExtensionSchemaStmt(Node *stmt,
												  const char *queryString);
extern List * PreprocessAlterExtensionUpdateStmt(Node *stmt,
												 const char *queryString,
												 ProcessUtilityContext
												 processUtilityContext);
extern void PostprocessAlterExtensionCitusUpdateStmt(Node *node);
extern List * PreprocessAlterExtensionContentsStmt(Node *node,
												   const char *queryString,
												   ProcessUtilityContext
												   processUtilityContext);
extern List * CreateExtensionDDLCommand(const ObjectAddress *extensionAddress);
extern List * AlterExtensionSchemaStmtObjectAddress(Node *stmt,
													bool missing_ok);
extern List * AlterExtensionUpdateStmtObjectAddress(Node *stmt,
													bool missing_ok);
extern void CreateExtensionWithVersion(char *extname, char *extVersion);
extern void AlterExtensionUpdateStmt(char *extname, char *extVersion);
extern int GetExtensionVersionNumber(char *extVersion);

/* foreign_constraint.c - forward declarations */
extern bool ConstraintIsAForeignKeyToReferenceTable(char *constraintName,
													Oid leftRelationId);
extern void ErrorIfUnsupportedForeignConstraintExists(Relation relation,
													  char referencingReplicationModel,
													  char distributionMethod,
													  Var *distributionColumn,
													  uint32 colocationId);
extern void ErrorOutForFKeyBetweenPostgresAndCitusLocalTable(Oid localTableId);
extern bool ColumnReferencedByAnyForeignKey(char *columnName, Oid relationId);
extern bool ColumnAppearsInForeignKey(char *columnName, Oid relationId);
extern bool ColumnAppearsInForeignKeyToReferenceTable(char *columnName, Oid
													  relationId);
extern List * GetReferencingForeignConstaintCommands(Oid relationOid);
extern List * GetForeignConstraintToReferenceTablesCommands(Oid relationId);
extern List * GetForeignConstraintToDistributedTablesCommands(Oid relationId);
extern List * GetForeignConstraintFromDistributedTablesCommands(Oid relationId);
extern List * GetForeignConstraintCommandsInternal(Oid relationId, int flags);
extern bool AnyForeignKeyDependsOnIndex(Oid indexId);
extern bool HasForeignKeyWithLocalTable(Oid relationId);
extern bool HasForeignKeyToCitusLocalTable(Oid relationId);
extern bool HasForeignKeyToReferenceTable(Oid relationOid);
extern List * GetForeignKeysFromLocalTables(Oid relationId);
extern bool TableReferenced(Oid relationOid);
extern bool TableReferencing(Oid relationOid);
extern bool ConstraintIsAUniquenessConstraint(char *inputConstaintName, Oid relationId);
extern bool ConstraintIsAForeignKey(char *inputConstaintName, Oid relationOid);
extern bool ConstraintWithNameIsOfType(char *inputConstaintName, Oid relationId,
									   char targetConstraintType);
extern bool ConstraintWithIdIsOfType(Oid constraintId, char targetConstraintType);
extern bool TableHasExternalForeignKeys(Oid relationId);
extern List * GetForeignKeyOids(Oid relationId, int flags);
extern Oid GetReferencedTableId(Oid foreignKeyId);
extern Oid GetReferencingTableId(Oid foreignKeyId);
extern bool RelationInvolvedInAnyNonInheritedForeignKeys(Oid relationId);


/* foreign_data_wrapper.c - forward declarations */
extern List * PreprocessGrantOnFDWStmt(Node *node, const char *queryString,
									   ProcessUtilityContext processUtilityContext);
extern Acl * GetPrivilegesForFDW(Oid FDWOid);


/* foreign_server.c - forward declarations */
extern List * PreprocessGrantOnForeignServerStmt(Node *node, const char *queryString,
												 ProcessUtilityContext
												 processUtilityContext);
extern List * CreateForeignServerStmtObjectAddress(Node *node, bool missing_ok);
extern List * AlterForeignServerStmtObjectAddress(Node *node, bool missing_ok);
extern List * RenameForeignServerStmtObjectAddress(Node *node, bool missing_ok);
extern List * AlterForeignServerOwnerStmtObjectAddress(Node *node, bool
													   missing_ok);
extern List * GetForeignServerCreateDDLCommand(Oid serverId);


/* foreign_table.c - forward declarations */
extern List * PreprocessAlterForeignTableSchemaStmt(Node *node, const char *queryString,
													ProcessUtilityContext
													processUtilityContext);


/* function.c - forward declarations */
extern List * PreprocessCreateFunctionStmt(Node *stmt, const char *queryString,
										   ProcessUtilityContext processUtilityContext);
extern List * PostprocessCreateFunctionStmt(Node *stmt,
											const char *queryString);
extern List * CreateFunctionStmtObjectAddress(Node *stmt,
											  bool missing_ok);
extern List * DefineAggregateStmtObjectAddress(Node *stmt,
											   bool missing_ok);
extern List * PreprocessAlterFunctionStmt(Node *stmt, const char *queryString,
										  ProcessUtilityContext processUtilityContext);
extern List * AlterFunctionStmtObjectAddress(Node *stmt,
											 bool missing_ok);
extern List * RenameFunctionStmtObjectAddress(Node *stmt,
											  bool missing_ok);
extern List * AlterFunctionOwnerObjectAddress(Node *stmt,
											  bool missing_ok);
extern List * AlterFunctionSchemaStmtObjectAddress(Node *stmt,
												   bool missing_ok);
extern List * PreprocessAlterFunctionDependsStmt(Node *stmt,
												 const char *queryString,
												 ProcessUtilityContext
												 processUtilityContext);
extern List * AlterFunctionDependsStmtObjectAddress(Node *stmt,
													bool missing_ok);
extern List * PreprocessGrantOnFunctionStmt(Node *node, const char *queryString,
											ProcessUtilityContext processUtilityContext);
extern List * PostprocessGrantOnFunctionStmt(Node *node, const char *queryString);


/* grant.c - forward declarations */
extern List * PreprocessGrantStmt(Node *node, const char *queryString,
								  ProcessUtilityContext processUtilityContext);
extern void deparsePrivileges(StringInfo privsString, GrantStmt *grantStmt);
extern void deparseGrantees(StringInfo granteesString, GrantStmt *grantStmt);


/* index.c - forward declarations */
extern bool IsIndexRenameStmt(RenameStmt *renameStmt);
extern List * PreprocessIndexStmt(Node *createIndexStatement,
								  const char *createIndexCommand,
								  ProcessUtilityContext processUtilityContext);
extern char * ChooseIndexName(const char *tabname, Oid namespaceId,
							  List *colnames, List *exclusionOpNames,
							  bool primary, bool isconstraint);
extern char * ChooseIndexNameAddition(List *colnames);
extern List * ChooseIndexColumnNames(List *indexElems);
extern LOCKMODE GetCreateIndexRelationLockMode(IndexStmt *createIndexStatement);
extern List * PreprocessReindexStmt(Node *ReindexStatement,
									const char *ReindexCommand,
									ProcessUtilityContext processUtilityContext);
extern List * PreprocessDropIndexStmt(Node *dropIndexStatement,
									  const char *dropIndexCommand,
									  ProcessUtilityContext processUtilityContext);
extern List * PostprocessIndexStmt(Node *node,
								   const char *queryString);
extern void ErrorIfUnsupportedAlterIndexStmt(AlterTableStmt *alterTableStatement);
extern void MarkIndexValid(IndexStmt *indexStmt);
extern List * ExecuteFunctionOnEachTableIndex(Oid relationId, PGIndexProcessor
											  pgIndexProcessor, int flags);
extern bool IsReindexWithParam_compat(ReindexStmt *stmt, char *paramName);

/* objectaddress.c - forward declarations */
extern List * CreateExtensionStmtObjectAddress(Node *stmt, bool missing_ok);


/* policy.c -  forward declarations */
extern List * CreatePolicyCommands(Oid relationId);
extern void ErrorIfUnsupportedPolicy(Relation relation);
extern void ErrorIfUnsupportedPolicyExpr(Node *expr);
extern List * PostprocessCreatePolicyStmt(Node *node, const char *queryString);
extern List * PreprocessAlterPolicyStmt(Node *node, const char *queryString,
										ProcessUtilityContext processUtilityContext);
extern List * PreprocessDropPolicyStmt(Node *stmt, const char *queryString,
									   ProcessUtilityContext processUtilityContext);
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
extern List * PreprocessRenameStmt(Node *renameStmt, const char *renameCommand,
								   ProcessUtilityContext processUtilityContext);
extern void ErrorIfUnsupportedRenameStmt(RenameStmt *renameStmt);
extern List * PreprocessRenameAttributeStmt(Node *stmt, const char *queryString,
											ProcessUtilityContext processUtilityContext);


/* role.c - forward declarations*/
extern List * PostprocessAlterRoleStmt(Node *stmt, const char *queryString);
extern List * PreprocessAlterRoleSetStmt(Node *stmt, const char *queryString,
										 ProcessUtilityContext processUtilityContext);
extern List * GenerateAlterRoleSetCommandForRole(Oid roleid);
extern List * AlterRoleStmtObjectAddress(Node *node,
										 bool missing_ok);
extern List * AlterRoleSetStmtObjectAddress(Node *node,
											bool missing_ok);
extern List * PreprocessCreateRoleStmt(Node *stmt, const char *queryString,
									   ProcessUtilityContext processUtilityContext);
extern List * PreprocessDropRoleStmt(Node *stmt, const char *queryString,
									 ProcessUtilityContext processUtilityContext);
extern List * PreprocessGrantRoleStmt(Node *stmt, const char *queryString,
									  ProcessUtilityContext processUtilityContext);
extern List * PostprocessGrantRoleStmt(Node *stmt, const char *queryString);
extern List * GenerateCreateOrAlterRoleCommand(Oid roleOid);
extern List * CreateRoleStmtObjectAddress(Node *stmt, bool missing_ok);
extern void UnmarkRolesDistributed(List *roles);
extern List * FilterDistributedRoles(List *roles);

/* schema.c - forward declarations */
extern List * PreprocessCreateSchemaStmt(Node *node, const char *queryString,
										 ProcessUtilityContext processUtilityContext);
extern List * PreprocessDropSchemaStmt(Node *dropSchemaStatement,
									   const char *queryString,
									   ProcessUtilityContext processUtilityContext);
extern List * PreprocessAlterObjectSchemaStmt(Node *alterObjectSchemaStmt,
											  const char *alterObjectSchemaCommand);
extern List * PreprocessGrantOnSchemaStmt(Node *node, const char *queryString,
										  ProcessUtilityContext processUtilityContext);
extern List * CreateSchemaStmtObjectAddress(Node *node, bool missing_ok);
extern List * AlterSchemaRenameStmtObjectAddress(Node *node, bool missing_ok);

/* sequence.c - forward declarations */
extern List * PreprocessAlterSequenceStmt(Node *node, const char *queryString,
										  ProcessUtilityContext processUtilityContext);
extern List * PreprocessAlterSequenceSchemaStmt(Node *node, const char *queryString,
												ProcessUtilityContext
												processUtilityContext);
extern List * PostprocessAlterSequenceSchemaStmt(Node *node, const char *queryString);
extern List * PreprocessAlterSequenceOwnerStmt(Node *node, const char *queryString,
											   ProcessUtilityContext processUtilityContext);
extern List * PostprocessAlterSequenceOwnerStmt(Node *node, const char *queryString);
extern List * PreprocessDropSequenceStmt(Node *node, const char *queryString,
										 ProcessUtilityContext processUtilityContext);
extern List * PreprocessRenameSequenceStmt(Node *node, const char *queryString,
										   ProcessUtilityContext processUtilityContext);
extern List * PreprocessGrantOnSequenceStmt(Node *node, const char *queryString,
											ProcessUtilityContext processUtilityContext);
extern List * PostprocessGrantOnSequenceStmt(Node *node, const char *queryString);
extern List * AlterSequenceStmtObjectAddress(Node *node, bool missing_ok);
extern List * AlterSequenceSchemaStmtObjectAddress(Node *node, bool missing_ok);
extern List * AlterSequenceOwnerStmtObjectAddress(Node *node, bool missing_ok);
extern List * RenameSequenceStmtObjectAddress(Node *node, bool missing_ok);
extern void ErrorIfUnsupportedSeqStmt(CreateSeqStmt *createSeqStmt);
extern void ErrorIfDistributedAlterSeqOwnedBy(AlterSeqStmt *alterSeqStmt);
extern char * GenerateBackupNameForSequenceCollision(const ObjectAddress *address);
extern void RenameExistingSequenceWithDifferentTypeIfExists(RangeVar *sequence,
															Oid desiredSeqTypeId);

/* statistics.c - forward declarations */
extern List * PreprocessCreateStatisticsStmt(Node *node, const char *queryString,
											 ProcessUtilityContext processUtilityContext);
extern List * PostprocessCreateStatisticsStmt(Node *node, const char *queryString);
extern List * CreateStatisticsStmtObjectAddress(Node *node, bool missingOk);
extern List * PreprocessDropStatisticsStmt(Node *node, const char *queryString,
										   ProcessUtilityContext processUtilityContext);
extern List * PreprocessAlterStatisticsRenameStmt(Node *node, const char *queryString,
												  ProcessUtilityContext
												  processUtilityContext);
extern List * PreprocessAlterStatisticsSchemaStmt(Node *node, const char *queryString,
												  ProcessUtilityContext
												  processUtilityContext);
extern List * PostprocessAlterStatisticsSchemaStmt(Node *node, const char *queryString);
extern List * AlterStatisticsSchemaStmtObjectAddress(Node *node, bool missingOk);
extern List * PreprocessAlterStatisticsStmt(Node *node, const char *queryString,
											ProcessUtilityContext processUtilityContext);
extern List * PreprocessAlterStatisticsOwnerStmt(Node *node, const char *queryString,
												 ProcessUtilityContext
												 processUtilityContext);
extern List * PostprocessAlterStatisticsOwnerStmt(Node *node, const char *queryString);
extern List * GetExplicitStatisticsCommandList(Oid relationId);
extern List * GetExplicitStatisticsSchemaIdList(Oid relationId);
extern List * GetAlterIndexStatisticsCommands(Oid indexOid);

/* subscription.c - forward declarations */
extern Node * ProcessCreateSubscriptionStmt(CreateSubscriptionStmt *createSubStmt);


/* table.c - forward declarations */
extern List * PreprocessDropTableStmt(Node *stmt, const char *queryString,
									  ProcessUtilityContext processUtilityContext);
extern void PostprocessCreateTableStmt(CreateStmt *createStatement,
									   const char *queryString);
extern bool ShouldEnableLocalReferenceForeignKeys(void);
extern List * PreprocessAlterTableStmtAttachPartition(AlterTableStmt *alterTableStatement,
													  const char *queryString);
extern List * PostprocessAlterTableSchemaStmt(Node *node, const char *queryString);
extern List * PreprocessAlterTableStmt(Node *node, const char *alterTableCommand,
									   ProcessUtilityContext processUtilityContext);
extern List * PreprocessAlterTableMoveAllStmt(Node *node, const char *queryString,
											  ProcessUtilityContext processUtilityContext);
extern List * PreprocessAlterTableSchemaStmt(Node *node, const char *queryString,
											 ProcessUtilityContext processUtilityContext);
extern Node * SkipForeignKeyValidationIfConstraintIsFkey(AlterTableStmt *alterTableStmt,
														 bool processLocalRelation);
extern bool IsAlterTableRenameStmt(RenameStmt *renameStmt);
extern void ErrorIfAlterDropsPartitionColumn(AlterTableStmt *alterTableStatement);
extern void PostprocessAlterTableStmt(AlterTableStmt *pStmt);
extern void FixAlterTableStmtIndexNames(AlterTableStmt *pStmt);
extern void ErrorUnsupportedAlterTableAddColumn(Oid relationId, AlterTableCmd *command,
												Constraint *constraint);
extern void ErrorIfUnsupportedConstraint(Relation relation, char distributionMethod,
										 char referencingReplicationModel,
										 Var *distributionColumn, uint32 colocationId);
extern List * AlterTableSchemaStmtObjectAddress(Node *stmt,
												bool missing_ok);
extern List * MakeNameListFromRangeVar(const RangeVar *rel);
extern Oid GetSequenceOid(Oid relationId, AttrNumber attnum);
extern bool ConstrTypeUsesIndex(ConstrType constrType);


/* text_search.c - forward declarations */
extern List * GetCreateTextSearchConfigStatements(const ObjectAddress *address);
extern List * GetCreateTextSearchDictionaryStatements(const ObjectAddress *address);
extern List * CreateTextSearchConfigDDLCommandsIdempotent(const ObjectAddress *address);
extern List * CreateTextSearchDictDDLCommandsIdempotent(const ObjectAddress *address);
extern List * CreateTextSearchConfigurationObjectAddress(Node *node,
														 bool missing_ok);
extern List * CreateTextSearchDictObjectAddress(Node *node,
												bool missing_ok);
extern List * RenameTextSearchConfigurationStmtObjectAddress(Node *node,
															 bool missing_ok);
extern List * RenameTextSearchDictionaryStmtObjectAddress(Node *node,
														  bool missing_ok);
extern List * AlterTextSearchConfigurationStmtObjectAddress(Node *node,
															bool missing_ok);
extern List * AlterTextSearchDictionaryStmtObjectAddress(Node *node,
														 bool missing_ok);
extern List * AlterTextSearchConfigurationSchemaStmtObjectAddress(Node *node,
																  bool missing_ok);
extern List * AlterTextSearchDictionarySchemaStmtObjectAddress(Node *node,
															   bool missing_ok);
extern List * TextSearchConfigurationCommentObjectAddress(Node *node,
														  bool missing_ok);
extern List * TextSearchDictCommentObjectAddress(Node *node,
												 bool missing_ok);
extern List * AlterTextSearchConfigurationOwnerObjectAddress(Node *node,
															 bool missing_ok);
extern List * AlterTextSearchDictOwnerObjectAddress(Node *node,
													bool missing_ok);
extern char * GenerateBackupNameForTextSearchConfiguration(const ObjectAddress *address);
extern char * GenerateBackupNameForTextSearchDict(const ObjectAddress *address);
extern List * get_ts_config_namelist(Oid tsconfigOid);

/* truncate.c - forward declarations */
extern void PreprocessTruncateStatement(TruncateStmt *truncateStatement);

/* type.c - forward declarations */
extern List * PreprocessRenameTypeAttributeStmt(Node *stmt, const char *queryString,
												ProcessUtilityContext
												processUtilityContext);
extern Node * CreateTypeStmtByObjectAddress(const ObjectAddress *address);
extern List * CompositeTypeStmtObjectAddress(Node *stmt, bool missing_ok);
extern List * CreateEnumStmtObjectAddress(Node *stmt, bool missing_ok);
extern List * AlterTypeStmtObjectAddress(Node *stmt, bool missing_ok);
extern List * AlterEnumStmtObjectAddress(Node *stmt, bool missing_ok);
extern List * RenameTypeStmtObjectAddress(Node *stmt, bool missing_ok);
extern List * AlterTypeSchemaStmtObjectAddress(Node *stmt,
											   bool missing_ok);
extern List * RenameTypeAttributeStmtObjectAddress(Node *stmt,
												   bool missing_ok);
extern List * AlterTypeOwnerObjectAddress(Node *stmt, bool missing_ok);
extern List * CreateTypeDDLCommandsIdempotent(const ObjectAddress *typeAddress);
extern char * GenerateBackupNameForTypeCollision(const ObjectAddress *address);

/* function.c - forward declarations */
extern List * CreateFunctionDDLCommandsIdempotent(const ObjectAddress *functionAddress);
extern char * GetFunctionDDLCommand(const RegProcedure funcOid, bool useCreateOrReplace);
extern char * GenerateBackupNameForProcCollision(const ObjectAddress *address);
extern ObjectWithArgs * ObjectWithArgsFromOid(Oid funcOid);
extern void UpdateFunctionDistributionInfo(const ObjectAddress *distAddress,
										   int *distribution_argument_index,
										   int *colocationId,
										   bool *forceDelegation);

/* vacuum.c - forward declarations */
extern List * PostprocessVacuumStmt(Node *node, const char *vacuumCommand);

/* view.c - forward declarations */
extern List * PreprocessViewStmt(Node *node, const char *queryString,
								 ProcessUtilityContext processUtilityContext);
extern List * PostprocessViewStmt(Node *node, const char *queryString);
extern List * ViewStmtObjectAddress(Node *node, bool missing_ok);
extern List * AlterViewStmtObjectAddress(Node *node, bool missing_ok);
extern List * PreprocessDropViewStmt(Node *node, const char *queryString,
									 ProcessUtilityContext processUtilityContext);
extern char * CreateViewDDLCommand(Oid viewOid);
extern List * GetViewCreationCommandsOfTable(Oid relationId);
extern List * GetViewCreationTableDDLCommandsOfTable(Oid relationId);
extern char * AlterViewOwnerCommand(Oid viewOid);
extern char * DeparseViewStmt(Node *node);
extern char * DeparseDropViewStmt(Node *node);
extern bool IsViewDistributed(Oid viewOid);
extern List * CreateViewDDLCommandsIdempotent(Oid viewOid);
extern List * PreprocessAlterViewStmt(Node *node, const char *queryString,
									  ProcessUtilityContext processUtilityContext);
extern List * PostprocessAlterViewStmt(Node *node, const char *queryString);
extern List * PreprocessRenameViewStmt(Node *node, const char *queryString,
									   ProcessUtilityContext processUtilityContext);
extern List * RenameViewStmtObjectAddress(Node *node, bool missing_ok);
extern List * PreprocessAlterViewSchemaStmt(Node *node, const char *queryString,
											ProcessUtilityContext processUtilityContext);
extern List * PostprocessAlterViewSchemaStmt(Node *node, const char *queryString);
extern List * AlterViewSchemaStmtObjectAddress(Node *node, bool missing_ok);
extern bool IsViewRenameStmt(RenameStmt *renameStmt);

/* trigger.c - forward declarations */
extern List * GetExplicitTriggerCommandList(Oid relationId);
extern HeapTuple GetTriggerTupleById(Oid triggerId, bool missingOk);
extern List * GetExplicitTriggerIdList(Oid relationId);
extern List * PostprocessCreateTriggerStmt(Node *node, const char *queryString);
extern List * CreateTriggerStmtObjectAddress(Node *node, bool missingOk);
extern void CreateTriggerEventExtendNames(CreateTrigStmt *createTriggerStmt,
										  char *schemaName, uint64 shardId);
extern List * PostprocessAlterTriggerRenameStmt(Node *node, const char *queryString);
extern void AlterTriggerRenameEventExtendNames(RenameStmt *renameTriggerStmt,
											   char *schemaName, uint64 shardId);
extern List * PostprocessAlterTriggerDependsStmt(Node *node, const char *queryString);
extern List * PreprocessAlterTriggerDependsStmt(Node *node, const char *queryString,
												ProcessUtilityContext
												processUtilityContext);
extern void AlterTriggerDependsEventExtendNames(
	AlterObjectDependsStmt *alterTriggerDependsStmt,
	char *schemaName, uint64 shardId);
extern void ErrorOutForTriggerIfNotSupported(Oid relationId);
extern List * PreprocessDropTriggerStmt(Node *node, const char *queryString,
										ProcessUtilityContext processUtilityContext);
extern void DropTriggerEventExtendNames(DropStmt *dropTriggerStmt, char *schemaName,
										uint64 shardId);
extern List * CitusCreateTriggerCommandDDLJob(Oid relationId, char *triggerName,
											  const char *queryString);
extern Oid GetTriggerFunctionId(Oid triggerId);

/* cascade_table_operation_for_connected_relations.c */

/*
 * Flags that can be passed to CascadeOperationForFkeyConnectedRelations, and
 * CascadeOperationForRelationIdList to specify
 * citus table function to be executed in cascading mode.
 */
typedef enum CascadeOperationType
{
	INVALID_OPERATION = 1 << 0,

	/* execute UndistributeTable on each relation */
	CASCADE_FKEY_UNDISTRIBUTE_TABLE = 1 << 1,

	/* execute CreateCitusLocalTable on each relation, with autoConverted = false */
	CASCADE_USER_ADD_LOCAL_TABLE_TO_METADATA = 1 << 2,

	/* execute CreateCitusLocalTable on each relation, with autoConverted = true */
	CASCADE_AUTO_ADD_LOCAL_TABLE_TO_METADATA = 1 << 3,
} CascadeOperationType;

extern void CascadeOperationForFkeyConnectedRelations(Oid relationId,
													  LOCKMODE relLockMode,
													  CascadeOperationType
													  cascadeOperationType);
extern void CascadeOperationForRelationIdList(List *relationIdList, LOCKMODE lockMode,
											  CascadeOperationType cascadeOperationType);
extern void ErrorIfAnyPartitionRelationInvolvedInNonInheritedFKey(List *relationIdList);
extern bool RelationIdListHasReferenceTable(List *relationIdList);
extern List * GetFKeyCreationCommandsForRelationIdList(List *relationIdList);
extern void DropRelationForeignKeys(Oid relationId, int flags);
extern void SetLocalEnableLocalReferenceForeignKeys(bool state);
extern void ExecuteAndLogUtilityCommandListInTableTypeConversionViaSPI(
	List *utilityCommandList);
extern void ExecuteAndLogUtilityCommandList(List *ddlCommandList);
extern void ExecuteAndLogUtilityCommand(const char *commandString);
extern void ExecuteForeignKeyCreateCommandList(List *ddlCommandList,
											   bool skip_validation);

/* create_citus_local_table.c */
extern void CreateCitusLocalTable(Oid relationId, bool cascadeViaForeignKeys,
								  bool autoConverted);
extern List * GetExplicitIndexOidList(Oid relationId);

extern bool ShouldPropagateSetCommand(VariableSetStmt *setStmt);
extern void PostprocessVariableSetStmt(VariableSetStmt *setStmt, const char *setCommand);

extern void CreateCitusLocalTablePartitionOf(CreateStmt *createStatement,
											 Oid relationId, Oid parentRelationId);
extern void UpdateAutoConvertedForConnectedRelations(List *relationId, bool
													 autoConverted);

#endif /*CITUS_COMMANDS_H */
