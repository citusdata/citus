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
#include "distributed/master_protocol.h"
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

/*
 * Flags that can be passed to GetForeignKeyOids to indicate
 * which foreign key constraint OIDs are to be extracted
 */
typedef enum ExtractForeignKeyConstrainstMode
{
	/* extract the foreign key OIDs where the table is the referencing one */
	INCLUDE_REFERENCING_CONSTRAINTS = 1 << 0,

	/* extract the foreign key OIDs the table is the referenced one */
	INCLUDE_REFERENCED_CONSTRAINTS = 1 << 1,

	/* exclude the self-referencing foreign keys */
	EXCLUDE_SELF_REFERENCES = 1 << 2
} ExtractForeignKeyConstraintMode;

/* Local functions forward declarations */
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
static List * GetForeignConstraintCommandsInternal(Oid relationId, int flags);
static Oid get_relation_constraint_oid_compat(HeapTuple heapTuple);
static List * GetForeignKeyOidsToReferenceTables(Oid relationId);
static List * GetForeignKeyOids(Oid relationId, int flags);

/*
 * ConstraintIsAForeignKeyToReferenceTable checks if the given constraint is a
 * foreign key constraint from the given relation to any reference table.
 */
bool
ConstraintIsAForeignKeyToReferenceTable(char *inputConstaintName, Oid relationId)
{
	List *foreignKeyOids = GetForeignKeyOidsToReferenceTables(relationId);

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
 *   table is not a reference table.
 */
void
ErrorIfUnsupportedForeignConstraintExists(Relation relation, char referencingDistMethod,
										  Var *referencingDistKey,
										  uint32 referencingColocationId)
{
	Oid referencingTableId = relation->rd_id;
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
		referencingNotReplicated = (ShardReplicationFactor == 1);
	}

	int flags = INCLUDE_REFERENCING_CONSTRAINTS;
	List *foreignKeyOids = GetForeignKeyOids(referencingTableId, flags);

	Oid foreignKeyOid = InvalidOid;
	foreach_oid(foreignKeyOid, foreignKeyOids)
	{
		HeapTuple heapTuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(foreignKeyOid));

		Assert(HeapTupleIsValid(heapTuple));

		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		int referencingAttrIndex = -1;

		char referencedDistMethod = 0;
		Var *referencedDistKey = NULL;
		int referencedAttrIndex = -1;
		uint32 referencedColocationId = INVALID_COLOCATION_ID;

		Oid referencedTableId = constraintForm->confrelid;
		bool referencedIsCitus = IsCitusTable(referencedTableId);

		bool selfReferencingTable = (referencingTableId == referencedTableId);

		if (!referencedIsCitus && !selfReferencingTable)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							errmsg("cannot create foreign key constraint"),
							errdetail("Referenced table must be a distributed table"
									  " or a reference table.")));
		}

		/* set referenced table related variables here if table is referencing itself */

		if (!selfReferencingTable)
		{
			referencedDistMethod = PartitionMethod(referencedTableId);
			referencedDistKey = (referencedDistMethod == DISTRIBUTE_BY_NONE) ?
								NULL :
								DistPartitionKey(referencedTableId);
			referencedColocationId = TableColocationId(referencedTableId);
		}
		else
		{
			referencedDistMethod = referencingDistMethod;
			referencedDistKey = referencingDistKey;
			referencedColocationId = referencingColocationId;
		}

		bool referencingIsReferenceTable = (referencingDistMethod == DISTRIBUTE_BY_NONE);
		bool referencedIsReferenceTable = (referencedDistMethod == DISTRIBUTE_BY_NONE);

		/*
		 * We support foreign keys between reference tables. No more checks
		 * are necessary.
		 */
		if (referencingIsReferenceTable && referencedIsReferenceTable)
		{
			ReleaseSysCache(heapTuple);
			continue;
		}

		/*
		 * Foreign keys from reference tables to distributed tables are not
		 * supported.
		 */
		if (referencingIsReferenceTable && !referencedIsReferenceTable)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create foreign key constraint "
								   "since foreign keys from reference tables "
								   "to distributed tables are not supported"),
							errdetail("A reference table can only have reference "
									  "keys to other reference tables")));
		}

		/*
		 * To enforce foreign constraints, tables must be co-located unless a
		 * reference table is referenced.
		 */
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
			/*
			 * ON DELETE SET NULL and ON DELETE SET DEFAULT is not supported. Because we do
			 * not want to set partition column to NULL or default value.
			 */
			if (constraintForm->confdeltype == FKCONSTR_ACTION_SETNULL ||
				constraintForm->confdeltype == FKCONSTR_ACTION_SETDEFAULT)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot create foreign key constraint"),
								errdetail("SET NULL or SET DEFAULT is not supported"
										  " in ON DELETE operation when distribution "
										  "key is included in the foreign key constraint")));
			}

			/*
			 * ON UPDATE SET NULL, ON UPDATE SET DEFAULT and UPDATE CASCADE is not supported.
			 * Because we do not want to set partition column to NULL or default value. Also
			 * cascading update operation would require re-partitioning. Updating partition
			 * column value is not allowed anyway even outside of foreign key concept.
			 */
			if (constraintForm->confupdtype == FKCONSTR_ACTION_SETNULL ||
				constraintForm->confupdtype == FKCONSTR_ACTION_SETDEFAULT ||
				constraintForm->confupdtype == FKCONSTR_ACTION_CASCADE)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot create foreign key constraint"),
								errdetail("SET NULL, SET DEFAULT or CASCADE is not "
										  "supported in ON UPDATE operation  when "
										  "distribution key included in the foreign "
										  "constraint.")));
			}
		}

		/*
		 * if tables are hash-distributed and colocated, we need to make sure that
		 * the distribution key is included in foreign constraint.
		 */
		if (!referencedIsReferenceTable && !foreignConstraintOnDistKey)
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

		ReleaseSysCache(heapTuple);
	}
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
 * ColumnAppearsInForeignKeyToReferenceTable checks if there is a foreign key
 * constraint from/to any reference table on the given column.
 */
bool
ColumnAppearsInForeignKeyToReferenceTable(char *columnName, Oid relationId)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool foreignKeyToReferenceTableIncludesGivenColumn = false;

	Relation pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);

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

		if (referencedTableId == relationId)
		{
			pgConstraintKey = Anum_pg_constraint_confkey;
		}
		else if (referencingTableId == relationId)
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

		/*
		 * We check if the referenced table is a reference table. There cannot be
		 * any foreign constraint from a distributed table to a local table.
		 */
		Assert(IsCitusTable(referencedTableId));
		if (PartitionMethod(referencedTableId) != DISTRIBUTE_BY_NONE)
		{
			heapTuple = systable_getnext(scanDescriptor);
			continue;
		}

		if (HeapTupleOfForeignConstraintIncludesColumn(heapTuple, relationId,
													   pgConstraintKey, columnName))
		{
			foreignKeyToReferenceTableIncludesGivenColumn = true;
			break;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, NoLock);

	return foreignKeyToReferenceTableIncludesGivenColumn;
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
	int flags = INCLUDE_REFERENCING_CONSTRAINTS;
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
 * HasForeignKeyToReferenceTable function returns true if any of the foreign
 * key constraints on the relation with relationId references to a reference
 * table.
 */
bool
HasForeignKeyToReferenceTable(Oid relationId)
{
	List *foreignKeyOids = GetForeignKeyOidsToReferenceTables(relationId);

	return list_length(foreignKeyOids) > 0;
}


/*
 * GetForeignKeyOidsToReferenceTables function returns list of OIDs for the
 * foreign key constraints on the given relationId that are referencing to
 * reference tables.
 */
static List *
GetForeignKeyOidsToReferenceTables(Oid relationId)
{
	int flags = INCLUDE_REFERENCING_CONSTRAINTS;
	List *foreignKeyOids = GetForeignKeyOids(relationId, flags);

	List *fkeyOidsToReferenceTables = NIL;

	Oid foreignKeyOid = InvalidOid;
	foreach_oid(foreignKeyOid, foreignKeyOids)
	{
		HeapTuple heapTuple =
			SearchSysCache1(CONSTROID, ObjectIdGetDatum(foreignKeyOid));

		Assert(HeapTupleIsValid(heapTuple));

		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		Oid referencedTableOid = constraintForm->confrelid;

		if (IsReferenceTable(referencedTableOid))
		{
			fkeyOidsToReferenceTables = lappend_oid(fkeyOidsToReferenceTables,
													foreignKeyOid);
		}

		ReleaseSysCache(heapTuple);
	}

	return fkeyOidsToReferenceTables;
}


/*
 * TableReferenced function checks whether given table is referenced by another table
 * via foreign constraints. If it is referenced, this function returns true.
 */
bool
TableReferenced(Oid relationId)
{
	int flags = INCLUDE_REFERENCED_CONSTRAINTS;
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
	int flags = INCLUDE_REFERENCING_CONSTRAINTS;
	List *foreignKeyOids = GetForeignKeyOids(relationId, flags);

	return list_length(foreignKeyOids) > 0;
}


/*
 * ConstraintIsAForeignKey returns true if the given constraint name
 * is a foreign key defined on the relation.
 */
bool
ConstraintIsAForeignKey(char *inputConstaintName, Oid relationId)
{
	int flags = INCLUDE_REFERENCING_CONSTRAINTS;
	List *foreignKeyOids = GetForeignKeyOids(relationId, flags);

	Oid foreignKeyOid = FindForeignKeyOidWithName(foreignKeyOids, inputConstaintName);

	return OidIsValid(foreignKeyOid);
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
 * GetForeignKeyOids takes in a relationId, and returns a list of OIDs for
 * foreign constraints that the relation with relationId is involved according
 * to "flags" argument. See ExtractForeignKeyConstrainstMode enum definition
 * for usage of the flags.
 */
static List *
GetForeignKeyOids(Oid relationId, int flags)
{
	AttrNumber pgConstraintTargetAttrNumber = InvalidAttrNumber;

	bool extractReferencing PG_USED_FOR_ASSERTS_ONLY =
		(flags & INCLUDE_REFERENCING_CONSTRAINTS);
	bool extractReferenced PG_USED_FOR_ASSERTS_ONLY =
		(flags & INCLUDE_REFERENCED_CONSTRAINTS);

	/*
	 * Only one of them should be passed at a time since the way we scan
	 * pg_constraint differs for those columns. Anum_pg_constraint_conrelid
	 * supports index scan while Anum_pg_constraint_confrelid does not.
	 */
	Assert(!(extractReferencing && extractReferenced));
	Assert(extractReferencing || extractReferenced);

	bool useIndex = false;
	Oid indexOid = InvalidOid;

	if (flags & INCLUDE_REFERENCING_CONSTRAINTS)
	{
		pgConstraintTargetAttrNumber = Anum_pg_constraint_conrelid;

		useIndex = true;
		indexOid = ConstraintRelidTypidNameIndexId;
	}
	else if (flags & INCLUDE_REFERENCED_CONSTRAINTS)
	{
		pgConstraintTargetAttrNumber = Anum_pg_constraint_confrelid;
	}

	bool excludeSelfReference = (flags & EXCLUDE_SELF_REFERENCES);

	List *foreignKeyOids = NIL;

	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	Relation pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);
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

		foreignKeyOids = lappend_oid(foreignKeyOids, constraintId);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);

	/*
	 * Do not release AccessShareLock yet to prevent modifications to be done
	 * on pg_constraint to make sure that caller will process valid foreign key
	 * constraints through the transaction.
	 */
	heap_close(pgConstraint, NoLock);

	return foreignKeyOids;
}
