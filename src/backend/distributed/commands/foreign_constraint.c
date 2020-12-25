/*-------------------------------------------------------------------------
 *
 * foreign_constraint.c
 *
 * This file contains functions to create, alter and drop foreign
 * constraints on distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_constraint.h"
#if (PG_VERSION_NUM >= PG_VERSION_12)
#include "access/genam.h"
#endif
#include "catalog/pg_type.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/listutils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/multi_join_order.h"
#include "distributed/namespace_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/version_compat.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"


#define BehaviorIsRestrictOrNoAction(x) \
	((x) == FKCONSTR_ACTION_NOACTION || (x) == FKCONSTR_ACTION_RESTRICT)


typedef bool (*CheckRelationFunc)(Oid);


/* Local functions forward declarations */
static void EnsureReferencingTableNotReplicated(Oid referencingTableId);
static void EnsureSupportedFKeyOnDistKey(Form_pg_constraint constraintForm);
static void EnsureSupportedFKeyBetweenCitusLocalAndRefTable(Form_pg_constraint
															constraintForm,
															char
															referencingReplicationModel,
															char
															referencedReplicationModel);
static bool HeapTupleOfForeignConstraintIncludesColumn(HeapTuple heapTuple,
													   Oid relationId,
													   int pgConstraintKey,
													   char *columnName);
static Oid FindForeignKeyOidWithName(List *foreignKeyOids, const
									 char *inputConstraintName);
static void ForeignConstraintFindDistKeys(HeapTuple pgConstraintTuple,
										  Var *referencingDistColumn,
										  Var *referencedDistColumn,
										  int *referencingAttrIndex,
										  int *referencedAttrIndex);
static List * GetForeignKeyIdsForColumn(char *columnName, Oid relationId,
										int searchForeignKeyColumnFlags);
static List * GetForeignConstraintCommandsInternal(Oid relationId, int flags);
static Oid get_relation_constraint_oid_compat(HeapTuple heapTuple);
static bool IsTableTypeIncluded(Oid relationId, int flags);

/*
 * ConstraintIsAForeignKeyToReferenceTable checks if the given constraint is a
 * foreign key constraint from the given relation to any reference table.
 */
bool
ConstraintIsAForeignKeyToReferenceTable(char *inputConstaintName, Oid relationId)
{
	int flags = INCLUDE_REFERENCING_CONSTRAINTS | INCLUDE_REFERENCE_TABLES;
	List *foreignKeyOids = GetForeignKeyOids(relationId, flags);

	Oid foreignKeyOid = FindForeignKeyOidWithName(foreignKeyOids, inputConstaintName);

	return OidIsValid(foreignKeyOid);
}


/*
 * ErrorIfUnsupportedForeignConstraintExists runs checks related to foreign
 * constraints and errors out if it is not possible to create one of the
 * foreign constraint in distributed environment.
 *
 * To support foreign constraints, we require that;
 * - If referencing and referenced tables are hash-distributed
 *		- Referencing and referenced tables are co-located.
 *      - Foreign constraint is defined over distribution column.
 *		- ON DELETE/UPDATE SET NULL, ON DELETE/UPDATE SET DEFAULT and
 *        ON UPDATE CASCADE options
 *        are not used.
 *      - Replication factors of referencing and referenced table are 1.
 * - If referenced table is a reference table
 *      - ON DELETE/UPDATE SET NULL, ON DELETE/UPDATE SET DEFAULT and
 *        ON UPDATE CASCADE options are not used on the distribution key
 *        of the referencing column.
 * - If referencing table is a reference table, error out if the referenced
 *   table is a distributed table.
 * - If referencing table is a reference table and referenced table is a
 *   citus local table:
 *      - ON DELETE/UPDATE SET NULL, ON DELETE/UPDATE SET DEFAULT and
 *        ON CASCADE options are not used.
 * - If referencing or referenced table is distributed table, then the
 *   other table is not a citus local table.
 */
void
ErrorIfUnsupportedForeignConstraintExists(Relation relation, char referencingDistMethod,
										  char referencingReplicationModel,
										  Var *referencingDistKey,
										  uint32 referencingColocationId)
{
	Oid referencingTableId = relation->rd_id;

	int flags = INCLUDE_REFERENCING_CONSTRAINTS | INCLUDE_ALL_TABLE_TYPES;
	List *foreignKeyOids = GetForeignKeyOids(referencingTableId, flags);

	Oid foreignKeyOid = InvalidOid;
	foreach_oid(foreignKeyOid, foreignKeyOids)
	{
		HeapTuple heapTuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(foreignKeyOid));

		Assert(HeapTupleIsValid(heapTuple));

		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		int referencingAttrIndex = -1;

		Var *referencedDistKey = NULL;
		int referencedAttrIndex = -1;
		uint32 referencedColocationId = INVALID_COLOCATION_ID;

		Oid referencedTableId = constraintForm->confrelid;
		bool referencedIsCitus = IsCitusTable(referencedTableId);

		bool selfReferencingTable = (referencingTableId == referencedTableId);

		if (!referencedIsCitus && !selfReferencingTable)
		{
			if (IsCitusLocalTableByDistParams(referencingDistMethod,
											  referencingReplicationModel))
			{
				ErrorOutForFKeyBetweenPostgresAndCitusLocalTable(referencedTableId);
			}

			char *referencedTableName = get_rel_name(referencedTableId);

			ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							errmsg("referenced table \"%s\" must be a distributed table"
								   " or a reference table",
								   referencedTableName),
							errdetail("To enforce foreign keys, the referencing and "
									  "referenced rows need to be stored on the same "
									  "node."),
							errhint("You could use SELECT create_reference_table('%s') "
									"to replicate the referenced table to all nodes",
									referencedTableName)));
		}

		/* set referenced table related variables here if table is referencing itself */
		char referencedDistMethod = 0;
		char referencedReplicationModel = REPLICATION_MODEL_INVALID;
		if (!selfReferencingTable)
		{
			referencedDistMethod = PartitionMethod(referencedTableId);
			referencedDistKey = IsCitusTableType(referencedTableId,
												 CITUS_TABLE_WITH_NO_DIST_KEY) ?
								NULL :
								DistPartitionKey(referencedTableId);
			referencedColocationId = TableColocationId(referencedTableId);
			referencedReplicationModel = TableReplicationModel(referencedTableId);
		}
		else
		{
			referencedDistMethod = referencingDistMethod;
			referencedDistKey = referencingDistKey;
			referencedColocationId = referencingColocationId;
			referencedReplicationModel = referencingReplicationModel;
		}

		bool referencingIsCitusLocalOrRefTable =
			(referencingDistMethod == DISTRIBUTE_BY_NONE);
		bool referencedIsCitusLocalOrRefTable =
			(referencedDistMethod == DISTRIBUTE_BY_NONE);
		if (referencingIsCitusLocalOrRefTable && referencedIsCitusLocalOrRefTable)
		{
			EnsureSupportedFKeyBetweenCitusLocalAndRefTable(constraintForm,
															referencingReplicationModel,
															referencedReplicationModel);

			ReleaseSysCache(heapTuple);
			continue;
		}

		/*
		 * Foreign keys from citus local tables or reference tables to distributed
		 * tables are not supported.
		 */
		if (referencingIsCitusLocalOrRefTable && !referencedIsCitusLocalOrRefTable)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create foreign key constraint "
								   "since foreign keys from reference tables "
								   "to distributed tables are not supported"),
							errdetail("A reference table can only have foreign "
									  "keys to other reference tables or citus "
									  "local tables")));
		}

		/*
		 * To enforce foreign constraints, tables must be co-located unless a
		 * reference table is referenced.
		 */
		bool referencedIsReferenceTable =
			(referencedReplicationModel == REPLICATION_MODEL_2PC);
		if (referencingColocationId == INVALID_COLOCATION_ID ||
			(referencingColocationId != referencedColocationId &&
			 !referencedIsReferenceTable))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create foreign key constraint since "
								   "relations are not colocated or not referencing "
								   "a reference table"),
							errdetail(
								"A distributed table can only have foreign keys "
								"if it is referencing another colocated hash "
								"distributed table or a reference table")));
		}

		ForeignConstraintFindDistKeys(heapTuple,
									  referencingDistKey,
									  referencedDistKey,
									  &referencingAttrIndex,
									  &referencedAttrIndex);
		bool referencingColumnsIncludeDistKey = (referencingAttrIndex != -1);
		bool foreignConstraintOnDistKey =
			(referencingColumnsIncludeDistKey && referencingAttrIndex ==
			 referencedAttrIndex);

		/*
		 * If columns in the foreign key includes the distribution key from the
		 * referencing side, we do not allow update/delete operations through
		 * foreign key constraints (e.g. ... ON UPDATE SET NULL)
		 */
		if (referencingColumnsIncludeDistKey)
		{
			EnsureSupportedFKeyOnDistKey(constraintForm);
		}

		/*
		 * if tables are hash-distributed and colocated, we need to make sure that
		 * the distribution key is included in foreign constraint.
		 */
		if (!referencedIsCitusLocalOrRefTable && !foreignConstraintOnDistKey)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create foreign key constraint"),
							errdetail("Foreign keys are supported in two cases, "
									  "either in between two colocated tables including "
									  "partition column in the same ordinal in the both "
									  "tables or from distributed to reference tables")));
		}

		/*
		 * We do not allow to create foreign constraints if shard replication factor is
		 * greater than 1. Because in our current design, multiple replicas may cause
		 * locking problems and inconsistent shard contents.
		 *
		 * Note that we allow referenced table to be a reference table (e.g., not a
		 * single replicated table). This is allowed since (a) we are sure that
		 * placements always be in the same state (b) executors are aware of reference
		 * tables and handle concurrency related issues accordingly.
		 */
		EnsureReferencingTableNotReplicated(referencingTableId);

		ReleaseSysCache(heapTuple);
	}
}


/*
 * EnsureSupportedFKeyBetweenCitusLocalAndRefTable is a helper function that
 * takes a foreign key constraint form for a foreign key between two citus
 * tables that are either citus local table or reference table and errors
 * out if it it an unsupported foreign key from a reference table to a citus
 * local table according to given replication model parameters.
 */
static void
EnsureSupportedFKeyBetweenCitusLocalAndRefTable(Form_pg_constraint fKeyConstraintForm,
												char referencingReplicationModel,
												char referencedReplicationModel)
{
	bool referencingIsReferenceTable =
		(referencingReplicationModel == REPLICATION_MODEL_2PC);
	bool referencedIsCitusLocalTable =
		(referencedReplicationModel != REPLICATION_MODEL_2PC);
	if (referencingIsReferenceTable && referencedIsCitusLocalTable)
	{
		/*
		 * We only support RESTRICT and NO ACTION behaviors for the
		 * foreign keys from reference tables to citus local tables.
		 * This is because, we can't cascade dml operations from citus
		 * local tables's coordinator placement to the remote placements
		 * of the reference table.
		 * Note that for the foreign keys from citus local tables to
		 * reference tables, we support all foreign key behaviors.
		 */
		if (!(BehaviorIsRestrictOrNoAction(fKeyConstraintForm->confdeltype) &&
			  BehaviorIsRestrictOrNoAction(fKeyConstraintForm->confupdtype)))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot define foreign key constraint, "
								   "foreign keys from reference tables to "
								   "citus local tables can only be defined "
								   "with NO ACTION or RESTRICT behaviors")));
		}
	}
}


/*
 * EnsureSupportedFKeyOnDistKey errors out if given foreign key constraint form
 * implies an unsupported ON DELETE/UPDATE behavior assuming the referencing column
 * is the distribution column of the referencing distributed table.
 */
static void
EnsureSupportedFKeyOnDistKey(Form_pg_constraint fKeyConstraintForm)
{
	/*
	 * ON DELETE SET NULL and ON DELETE SET DEFAULT is not supported. Because we do
	 * not want to set partition column to NULL or default value.
	 */
	if (fKeyConstraintForm->confdeltype == FKCONSTR_ACTION_SETNULL ||
		fKeyConstraintForm->confdeltype == FKCONSTR_ACTION_SETDEFAULT)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot create foreign key constraint"),
						errdetail("SET NULL or SET DEFAULT is not supported "
								  "in ON DELETE operation when distribution "
								  "key is included in the foreign key constraint")));
	}

	/*
	 * ON UPDATE SET NULL, ON UPDATE SET DEFAULT and UPDATE CASCADE is not supported.
	 * Because we do not want to set partition column to NULL or default value. Also
	 * cascading update operation would require re-partitioning. Updating partition
	 * column value is not allowed anyway even outside of foreign key concept.
	 */
	if (fKeyConstraintForm->confupdtype == FKCONSTR_ACTION_SETNULL ||
		fKeyConstraintForm->confupdtype == FKCONSTR_ACTION_SETDEFAULT ||
		fKeyConstraintForm->confupdtype == FKCONSTR_ACTION_CASCADE)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot create foreign key constraint"),
						errdetail("SET NULL, SET DEFAULT or CASCADE is not "
								  "supported in ON UPDATE operation when "
								  "distribution key included in the foreign "
								  "constraint.")));
	}
}


/*
 * EnsureReferencingTableNotReplicated takes referencingTableId for the
 * referencing table of the foreign key and errors out if it's not a single
 * replicated table.
 */
static void
EnsureReferencingTableNotReplicated(Oid referencingTableId)
{
	bool referencingNotReplicated = true;
	bool referencingIsCitus = IsCitusTable(referencingTableId);

	if (referencingIsCitus)
	{
		/* ALTER TABLE command is applied over single replicated table */
		referencingNotReplicated = SingleReplicatedTable(referencingTableId);
	}
	else
	{
		/* Creating single replicated table with foreign constraint */
		referencingNotReplicated = !DistributedTableReplicationIsEnabled();
	}

	if (!referencingNotReplicated)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot create foreign key constraint"),
						errdetail("Citus Community Edition currently supports "
								  "foreign key constraints only for "
								  "\"citus.shard_replication_factor = 1\"."),
						errhint("Please change \"citus.shard_replication_factor to "
								"1\". To learn more about using foreign keys with "
								"other replication factors, please contact us at "
								"https://citusdata.com/about/contact_us.")));
	}
}


/*
 * ErrorOutForFKeyBetweenPostgresAndCitusLocalTable is a helper function to
 * error out for foreign keys between postgres local tables and citus local
 * tables.
 */
void
ErrorOutForFKeyBetweenPostgresAndCitusLocalTable(Oid localTableId)
{
	char *localTableName = get_rel_name(localTableId);
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cannot create foreign key constraint as \"%s\" is "
						   "a postgres local table", localTableName),
					errhint("first create a citus local table from the postgres "
							"local table using SELECT create_citus_local_table('%s') "
							"and execute the ALTER TABLE command to create the "
							"foreign key to citus local table", localTableName)));
}


/*
 * ForeignConstraintFindDistKeys finds the index of the given distribution columns
 * in the given foreign key constraint and returns them in referencingAttrIndex
 * and referencedAttrIndex. If one of them is not found, it returns -1 instead.
 */
static void
ForeignConstraintFindDistKeys(HeapTuple pgConstraintTuple,
							  Var *referencingDistColumn,
							  Var *referencedDistColumn,
							  int *referencingAttrIndex,
							  int *referencedAttrIndex)
{
	Datum *referencingColumnArray = NULL;
	int referencingColumnCount = 0;
	Datum *referencedColumnArray = NULL;
	int referencedColumnCount = 0;
	bool isNull = false;

	*referencedAttrIndex = -1;
	*referencedAttrIndex = -1;

	/*
	 * Column attributes are not available in Form_pg_constraint, therefore we need
	 * to find them in the system catalog. After finding them, we iterate over column
	 * attributes together because partition column must be at the same place in both
	 * referencing and referenced side of the foreign key constraint.
	 */
	Datum referencingColumnsDatum = SysCacheGetAttr(CONSTROID, pgConstraintTuple,
													Anum_pg_constraint_conkey, &isNull);
	Datum referencedColumnsDatum = SysCacheGetAttr(CONSTROID, pgConstraintTuple,
												   Anum_pg_constraint_confkey, &isNull);

	deconstruct_array(DatumGetArrayTypeP(referencingColumnsDatum), INT2OID, 2, true,
					  's', &referencingColumnArray, NULL, &referencingColumnCount);
	deconstruct_array(DatumGetArrayTypeP(referencedColumnsDatum), INT2OID, 2, true,
					  's', &referencedColumnArray, NULL, &referencedColumnCount);

	Assert(referencingColumnCount == referencedColumnCount);

	for (int attrIdx = 0; attrIdx < referencingColumnCount; ++attrIdx)
	{
		AttrNumber referencingAttrNo = DatumGetInt16(referencingColumnArray[attrIdx]);
		AttrNumber referencedAttrNo = DatumGetInt16(referencedColumnArray[attrIdx]);

		if (referencedDistColumn != NULL &&
			referencedDistColumn->varattno == referencedAttrNo)
		{
			*referencedAttrIndex = attrIdx;
		}

		if (referencingDistColumn != NULL &&
			referencingDistColumn->varattno == referencingAttrNo)
		{
			*referencingAttrIndex = attrIdx;
		}
	}
}


/*
 * ColumnAppearsInForeignKey returns true if there is a foreign key constraint
 * from/to given column.
 */
bool
ColumnAppearsInForeignKey(char *columnName, Oid relationId)
{
	int searchForeignKeyColumnFlags = SEARCH_REFERENCING_RELATION |
									  SEARCH_REFERENCED_RELATION;
	List *foreignKeyIdsColumnAppeared =
		GetForeignKeyIdsForColumn(columnName, relationId, searchForeignKeyColumnFlags);
	return list_length(foreignKeyIdsColumnAppeared) > 0;
}


/*
 * ColumnAppearsInForeignKeyToReferenceTable checks if there is a foreign key
 * constraint from/to any reference table on the given column.
 */
bool
ColumnAppearsInForeignKeyToReferenceTable(char *columnName, Oid relationId)
{
	int searchForeignKeyColumnFlags = SEARCH_REFERENCING_RELATION |
									  SEARCH_REFERENCED_RELATION;
	List *foreignKeyIdsColumnAppeared =
		GetForeignKeyIdsForColumn(columnName, relationId, searchForeignKeyColumnFlags);

	Oid foreignKeyId = InvalidOid;
	foreach_oid(foreignKeyId, foreignKeyIdsColumnAppeared)
	{
		Oid referencedTableId = GetReferencedTableId(foreignKeyId);
		if (IsCitusTableType(referencedTableId, REFERENCE_TABLE))
		{
			return true;
		}
	}

	return false;
}


/*
 * GetForeignKeyIdsForColumn takes columnName and relationId for the owning
 * relation, and returns a list of OIDs for foreign constraints that the column
 * with columnName is involved according to "searchForeignKeyColumnFlags" argument.
 * See SearchForeignKeyColumnFlags enum definition for usage.
 */
static List *
GetForeignKeyIdsForColumn(char *columnName, Oid relationId,
						  int searchForeignKeyColumnFlags)
{
	bool searchReferencing = searchForeignKeyColumnFlags & SEARCH_REFERENCING_RELATION;
	bool searchReferenced = searchForeignKeyColumnFlags & SEARCH_REFERENCED_RELATION;

	/* at least one of them should be true */
	Assert(searchReferencing || searchReferenced);

	List *foreignKeyIdsColumnAppeared = NIL;

	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	Relation pgConstraint = table_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_contype, BTEqualStrategyNumber,
				F_CHAREQ, CharGetDatum(CONSTRAINT_FOREIGN));

	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint, InvalidOid, false,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		int pgConstraintKey = 0;
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		Oid referencedTableId = constraintForm->confrelid;
		Oid referencingTableId = constraintForm->conrelid;

		if (referencedTableId == relationId && searchReferenced)
		{
			pgConstraintKey = Anum_pg_constraint_confkey;
		}
		else if (referencingTableId == relationId && searchReferencing)
		{
			pgConstraintKey = Anum_pg_constraint_conkey;
		}
		else
		{
			/*
			 * If the constraint is not from/to the given relation, we should simply
			 * skip.
			 */
			heapTuple = systable_getnext(scanDescriptor);
			continue;
		}

		if (HeapTupleOfForeignConstraintIncludesColumn(heapTuple, relationId,
													   pgConstraintKey, columnName))
		{
			Oid foreignKeyOid = get_relation_constraint_oid_compat(heapTuple);
			foreignKeyIdsColumnAppeared = lappend_oid(foreignKeyIdsColumnAppeared,
													  foreignKeyOid);
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	table_close(pgConstraint, NoLock);

	return foreignKeyIdsColumnAppeared;
}


/*
 * GetReferencingForeignConstaintCommands takes in a relationId, and
 * returns the list of foreign constraint commands needed to reconstruct
 * foreign key constraints that the table is involved in as the "referencing"
 * one.
 */
List *
GetReferencingForeignConstaintCommands(Oid relationId)
{
	int flags = INCLUDE_REFERENCING_CONSTRAINTS | INCLUDE_ALL_TABLE_TYPES;
	return GetForeignConstraintCommandsInternal(relationId, flags);
}


/*
 * GetForeignConstraintCommandsInternal is a wrapper function to get the
 * DDL commands to recreate the foreign key constraints returned by
 * GetForeignKeyOids. See more details at the underlying function.
 */
static List *
GetForeignConstraintCommandsInternal(Oid relationId, int flags)
{
	List *foreignKeyOids = GetForeignKeyOids(relationId, flags);

	List *foreignKeyCommands = NIL;

	PushOverrideEmptySearchPath(CurrentMemoryContext);

	Oid foreignKeyOid = InvalidOid;
	foreach_oid(foreignKeyOid, foreignKeyOids)
	{
		char *statementDef = pg_get_constraintdef_command(foreignKeyOid);

		foreignKeyCommands = lappend(foreignKeyCommands, statementDef);
	}

	/* revert back to original search_path */
	PopOverrideSearchPath();

	return foreignKeyCommands;
}


/*
 * get_relation_constraint_oid_compat returns OID of the constraint represented
 * by the constraintForm, which is passed as an heapTuple. OID of the contraint
 * is already stored in the constraintForm struct if major PostgreSQL version is
 * 12. However, in the older versions, we should utilize HeapTupleGetOid to deduce
 * that OID with no cost.
 */
static Oid
get_relation_constraint_oid_compat(HeapTuple heapTuple)
{
	Assert(heapTuple != NULL);

	Oid constraintOid = InvalidOid;

#if PG_VERSION_NUM >= PG_VERSION_12
	Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);
	constraintOid = constraintForm->oid;
#else
	constraintOid = HeapTupleGetOid(heapTuple);
#endif

	return constraintOid;
}


/*
 * HasForeignKeyToCitusLocalTable returns true if any of the foreign key constraints
 * on the relation with relationId references to a citus local table.
 */
bool
HasForeignKeyToCitusLocalTable(Oid relationId)
{
	int flags = INCLUDE_REFERENCING_CONSTRAINTS | INCLUDE_CITUS_LOCAL_TABLES;
	List *foreignKeyOidList = GetForeignKeyOids(relationId, flags);
	return list_length(foreignKeyOidList) > 0;
}


/*
 * HasForeignKeyToReferenceTable returns true if any of the foreign key
 * constraints on the relation with relationId references to a reference
 * table.
 */
bool
HasForeignKeyToReferenceTable(Oid relationId)
{
	int flags = INCLUDE_REFERENCING_CONSTRAINTS | INCLUDE_REFERENCE_TABLES;
	List *foreignKeyOids = GetForeignKeyOids(relationId, flags);

	return list_length(foreignKeyOids) > 0;
}


/*
 * TableReferenced function checks whether given table is referenced by another table
 * via foreign constraints. If it is referenced, this function returns true.
 */
bool
TableReferenced(Oid relationId)
{
	int flags = INCLUDE_REFERENCED_CONSTRAINTS | INCLUDE_ALL_TABLE_TYPES;
	List *foreignKeyOids = GetForeignKeyOids(relationId, flags);

	return list_length(foreignKeyOids) > 0;
}


/*
 * HeapTupleOfForeignConstraintIncludesColumn fetches the columns from the foreign
 * constraint and checks if the given column name matches one of them.
 */
static bool
HeapTupleOfForeignConstraintIncludesColumn(HeapTuple heapTuple, Oid relationId,
										   int pgConstraintKey, char *columnName)
{
	Datum *columnArray = NULL;
	int columnCount = 0;
	bool isNull = false;

	Datum columnsDatum = SysCacheGetAttr(CONSTROID, heapTuple, pgConstraintKey, &isNull);
	deconstruct_array(DatumGetArrayTypeP(columnsDatum), INT2OID, 2, true,
					  's', &columnArray, NULL, &columnCount);

	for (int attrIdx = 0; attrIdx < columnCount; ++attrIdx)
	{
		AttrNumber attrNo = DatumGetInt16(columnArray[attrIdx]);

		char *colName = get_attname(relationId, attrNo, false);
		if (strncmp(colName, columnName, NAMEDATALEN) == 0)
		{
			return true;
		}
	}

	return false;
}


/*
 * TableReferencing function checks whether given table is referencing to another
 * table via foreign key constraints. If it is referencing, this function returns
 * true.
 */
bool
TableReferencing(Oid relationId)
{
	int flags = INCLUDE_REFERENCING_CONSTRAINTS | INCLUDE_ALL_TABLE_TYPES;
	List *foreignKeyOids = GetForeignKeyOids(relationId, flags);

	return list_length(foreignKeyOids) > 0;
}


/*
 * ConstraintWithNameIsOfType is a wrapper around ConstraintWithNameIsOfType that returns true
 * if given constraint name identifies a uniqueness constraint, i.e:
 *   - primary key constraint, or
 *   - unique constraint
 */
bool
ConstraintIsAUniquenessConstraint(char *inputConstaintName, Oid relationId)
{
	bool isUniqueConstraint = ConstraintWithNameIsOfType(inputConstaintName, relationId,
														 CONSTRAINT_UNIQUE);
	bool isPrimaryConstraint = ConstraintWithNameIsOfType(inputConstaintName, relationId,
														  CONSTRAINT_PRIMARY);
	return isUniqueConstraint || isPrimaryConstraint;
}


/*
 * ConstraintIsAForeignKey is a wrapper around ConstraintWithNameIsOfType that returns true
 * if given constraint name identifies a foreign key constraint.
 */
bool
ConstraintIsAForeignKey(char *inputConstaintName, Oid relationId)
{
	return ConstraintWithNameIsOfType(inputConstaintName, relationId, CONSTRAINT_FOREIGN);
}


/*
 * ConstraintWithNameIsOfType is a wrapper around get_relation_constraint_oid that
 * returns true if given constraint name identifies a valid constraint defined
 * on relation with relationId and it's type matches the input constraint type.
 */
bool
ConstraintWithNameIsOfType(char *inputConstaintName, Oid relationId,
						   char targetConstraintType)
{
	bool missingOk = true;
	Oid constraintId =
		get_relation_constraint_oid(relationId, inputConstaintName, missingOk);
	return ConstraintWithIdIsOfType(constraintId, targetConstraintType);
}


/*
 * ConstraintWithIdIsOfType returns true if constraint with constraintId exists
 * and is of type targetConstraintType.
 */
bool
ConstraintWithIdIsOfType(Oid constraintId, char targetConstraintType)
{
	HeapTuple heapTuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraintId));
	if (!HeapTupleIsValid(heapTuple))
	{
		/* no such constraint */
		return false;
	}

	Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);
	char constraintType = constraintForm->contype;
	bool constraintTypeMatches = (constraintType == targetConstraintType);

	ReleaseSysCache(heapTuple);

	return constraintTypeMatches;
}


/*
 * FindForeignKeyOidWithName searches the foreign key constraint with
 * inputConstraintName in the given list of foreign key constraint OIDs.
 * Returns the OID of the matching constraint. If there no matching constraint
 * in the given list, then returns InvalidOid.
 */
static Oid
FindForeignKeyOidWithName(List *foreignKeyOids, const char *inputConstraintName)
{
	Oid foreignKeyOid = InvalidOid;
	foreach_oid(foreignKeyOid, foreignKeyOids)
	{
		char *constraintName = get_constraint_name(foreignKeyOid);

		Assert(constraintName != NULL);

		if (strncmp(constraintName, inputConstraintName, NAMEDATALEN) == 0)
		{
			return foreignKeyOid;
		}
	}

	return InvalidOid;
}


/*
 * ErrorIfTableHasExternalForeignKeys errors out if the relation with relationId
 * is involved in a foreign key relationship other than the self-referencing ones.
 */
void
ErrorIfTableHasExternalForeignKeys(Oid relationId)
{
	int flags = (INCLUDE_REFERENCING_CONSTRAINTS | EXCLUDE_SELF_REFERENCES |
				 INCLUDE_ALL_TABLE_TYPES);
	List *foreignKeyIdsTableReferencing = GetForeignKeyOids(relationId, flags);

	flags = (INCLUDE_REFERENCED_CONSTRAINTS | EXCLUDE_SELF_REFERENCES |
			 INCLUDE_ALL_TABLE_TYPES);
	List *foreignKeyIdsTableReferenced = GetForeignKeyOids(relationId, flags);

	List *foreignKeysWithOtherTables = list_concat(foreignKeyIdsTableReferencing,
												   foreignKeyIdsTableReferenced);

	if (list_length(foreignKeysWithOtherTables) == 0)
	{
		return;
	}

	const char *relationName = get_rel_name(relationId);
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("relation \"%s\" is involved in a foreign key relationship "
						   "with another table", relationName),
					errhint("Drop foreign keys with other tables and re-define them "
							"with ALTER TABLE commands after the current operation "
							"is done.")));
}


/*
 * GetForeignKeyOids takes in a relationId, and returns a list of OIDs for
 * foreign constraints that the relation with relationId is involved according
 * to "flags" argument. See ExtractForeignKeyConstraintsMode enum definition
 * for usage of the flags.
 */
List *
GetForeignKeyOids(Oid relationId, int flags)
{
	AttrNumber pgConstraintTargetAttrNumber = InvalidAttrNumber;

	bool extractReferencing = (flags & INCLUDE_REFERENCING_CONSTRAINTS);
	bool extractReferenced = (flags & INCLUDE_REFERENCED_CONSTRAINTS);

	/*
	 * Only one of them should be passed at a time since the way we scan
	 * pg_constraint differs for those columns. Anum_pg_constraint_conrelid
	 * supports index scan while Anum_pg_constraint_confrelid does not.
	 */
	Assert(!(extractReferencing && extractReferenced));
	Assert(extractReferencing || extractReferenced);

	bool useIndex = false;
	Oid indexOid = InvalidOid;

	if (extractReferencing)
	{
		pgConstraintTargetAttrNumber = Anum_pg_constraint_conrelid;

		useIndex = true;
		indexOid = ConstraintRelidTypidNameIndexId;
	}
	else if (extractReferenced)
	{
		pgConstraintTargetAttrNumber = Anum_pg_constraint_confrelid;
	}

	bool excludeSelfReference = (flags & EXCLUDE_SELF_REFERENCES);

	List *foreignKeyOids = NIL;

	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	Relation pgConstraint = table_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&scanKey[0], pgConstraintTargetAttrNumber,
				BTEqualStrategyNumber, F_OIDEQ, relationId);
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint, indexOid, useIndex,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		if (constraintForm->contype != CONSTRAINT_FOREIGN)
		{
			heapTuple = systable_getnext(scanDescriptor);
			continue;
		}

		bool inheritedConstraint = OidIsValid(constraintForm->conparentid);
		if (inheritedConstraint)
		{
			/*
			 * We only consider the constraints that are explicitly created on
			 * the table as we already process the constraints from parent tables
			 * implicitly when a command is issued
			 */
			heapTuple = systable_getnext(scanDescriptor);
			continue;
		}

		Oid constraintId = get_relation_constraint_oid_compat(heapTuple);

		bool isSelfReference = (constraintForm->conrelid == constraintForm->confrelid);
		if (excludeSelfReference && isSelfReference)
		{
			heapTuple = systable_getnext(scanDescriptor);
			continue;
		}

		Oid otherTableId = InvalidOid;
		if (extractReferencing)
		{
			otherTableId = constraintForm->confrelid;
		}
		else if (extractReferenced)
		{
			otherTableId = constraintForm->conrelid;
		}

		if (!IsTableTypeIncluded(otherTableId, flags))
		{
			heapTuple = systable_getnext(scanDescriptor);
			continue;
		}

		foreignKeyOids = lappend_oid(foreignKeyOids, constraintId);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);

	/*
	 * Do not release AccessShareLock yet to prevent modifications to be done
	 * on pg_constraint to make sure that caller will process valid foreign key
	 * constraints through the transaction.
	 */
	table_close(pgConstraint, NoLock);

	return foreignKeyOids;
}


/*
 * GetReferencedTableId returns OID of the referenced relation for the foreign
 * key with foreignKeyId. If there is no such foreign key, then this function
 * returns InvalidOid.
 */
Oid
GetReferencedTableId(Oid foreignKeyId)
{
	HeapTuple heapTuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(foreignKeyId));
	if (!HeapTupleIsValid(heapTuple))
	{
		/* no such foreign key */
		return InvalidOid;
	}

	Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);
	Oid referencedTableId = constraintForm->confrelid;

	ReleaseSysCache(heapTuple);

	return referencedTableId;
}


/*
 * IsTableTypeIncluded returns true if type of the table with relationId (distributed,
 * reference, Citus local or Postgres local) is included in the flags, false if not
 */
static bool
IsTableTypeIncluded(Oid relationId, int flags)
{
	if (!IsCitusTable(relationId))
	{
		return (flags & INCLUDE_LOCAL_TABLES) != 0;
	}
	else if (IsCitusTableType(relationId, DISTRIBUTED_TABLE))
	{
		return (flags & INCLUDE_DISTRIBUTED_TABLES) != 0;
	}
	else if (IsCitusTableType(relationId, REFERENCE_TABLE))
	{
		return (flags & INCLUDE_REFERENCE_TABLES) != 0;
	}
	else if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		return (flags & INCLUDE_CITUS_LOCAL_TABLES) != 0;
	}
	return false;
}
