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

#include "miscadmin.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"

#include "pg_version_constants.h"

#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/sequence.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/multi_join_order.h"
#include "distributed/namespace_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/utils/array_type.h"
#include "distributed/version_compat.h"


#define BehaviorIsRestrictOrNoAction(x) \
	((x) == FKCONSTR_ACTION_NOACTION || (x) == FKCONSTR_ACTION_RESTRICT)


#define USE_CREATE_REFERENCE_TABLE_HINT \
	"You could use SELECT create_reference_table('%s') " \
	"to replicate the referenced table to all nodes or " \
	"consider dropping the foreign key"


typedef bool (*CheckRelationFunc)(Oid);


/* Local functions forward declarations */
static void EnsureReferencingTableNotReplicated(Oid referencingTableId);
static void EnsureSupportedFKeyOnDistKey(Form_pg_constraint constraintForm);
static bool ForeignKeySetsNextValColumnToDefault(HeapTuple pgConstraintTuple);
static List * ForeignKeyGetDefaultingAttrs(HeapTuple pgConstraintTuple);
static void EnsureSupportedFKeyBetweenCitusLocalAndRefTable(Form_pg_constraint
															constraintForm,
															char
															referencingReplicationModel,
															char
															referencedReplicationModel,
															Oid referencedTableId);
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
static List * GetForeignKeysWithLocalTables(Oid relationId);
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
 * EnsureNoFKeyFromTableType ensures that given relation is not referenced by any table specified
 * by table type flag.
 */
void
EnsureNoFKeyFromTableType(Oid relationId, int tableTypeFlag)
{
	int flags = INCLUDE_REFERENCED_CONSTRAINTS | EXCLUDE_SELF_REFERENCES |
				tableTypeFlag;
	List *referencedFKeyOids = GetForeignKeyOids(relationId, flags);

	if (list_length(referencedFKeyOids) > 0)
	{
		Oid referencingFKeyOid = linitial_oid(referencedFKeyOids);
		Oid referencingTableId = GetReferencingTableId(referencingFKeyOid);

		char *referencingRelName = get_rel_name(referencingTableId);
		char *referencedRelName = get_rel_name(relationId);
		char *referencingTableTypeName = GetTableTypeName(referencingTableId);

		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("relation %s is referenced by a foreign key from %s",
							   referencedRelName, referencingRelName),
						errdetail(
							"foreign keys from a %s to a distributed table are not supported.",
							referencingTableTypeName)));
	}
}


/*
 * EnsureNoFKeyToTableType ensures that given relation is not referencing any table specified
 * by table type flag.
 */
void
EnsureNoFKeyToTableType(Oid relationId, int tableTypeFlag)
{
	int flags = INCLUDE_REFERENCING_CONSTRAINTS | EXCLUDE_SELF_REFERENCES |
				tableTypeFlag;
	List *referencingFKeyOids = GetForeignKeyOids(relationId, flags);

	if (list_length(referencingFKeyOids) > 0)
	{
		Oid referencedFKeyOid = linitial_oid(referencingFKeyOids);
		Oid referencedTableId = GetReferencedTableId(referencedFKeyOid);

		char *referencedRelName = get_rel_name(referencedTableId);
		char *referencingRelName = get_rel_name(relationId);
		char *referencedTableTypeName = GetTableTypeName(referencedTableId);

		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("relation %s is referenced by a foreign key from %s",
							   referencedRelName, referencingRelName),
						errdetail(
							"foreign keys from a distributed table to a %s are not supported.",
							referencedTableTypeName)));
	}
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
	foreach_declared_oid(foreignKeyOid, foreignKeyOids)
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
											  referencingReplicationModel,
											  referencingColocationId))
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
							errhint(USE_CREATE_REFERENCE_TABLE_HINT,
									referencedTableName)));
		}

		/* set referenced table related variables here if table is referencing itself */
		char referencedDistMethod = 0;
		char referencedReplicationModel = REPLICATION_MODEL_INVALID;
		if (!selfReferencingTable)
		{
			referencedDistMethod = PartitionMethod(referencedTableId);
			referencedDistKey = !HasDistributionKey(referencedTableId) ?
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

		/*
		 * Given that we drop DEFAULT nextval('sequence') expressions from
		 * shard relation columns, allowing ON DELETE/UPDATE SET DEFAULT
		 * on such columns causes inserting NULL values to referencing relation
		 * as a result of a delete/update operation on referenced relation.
		 *
		 * For this reason, we disallow ON DELETE/UPDATE SET DEFAULT actions
		 * on columns that default to sequences.
		 */
		if (ForeignKeySetsNextValColumnToDefault(heapTuple))
		{
			ereport(ERROR, (errmsg("cannot create foreign key constraint "
								   "since Citus does not support ON DELETE "
								   "/ UPDATE SET DEFAULT actions on the "
								   "columns that default to sequences")));
		}

		bool referencingIsCitusLocalOrRefTable =
			IsCitusLocalTableByDistParams(referencingDistMethod,
										  referencingReplicationModel,
										  referencingColocationId) ||
			IsReferenceTableByDistParams(referencingDistMethod,
										 referencingReplicationModel);
		bool referencedIsCitusLocalOrRefTable =
			IsCitusLocalTableByDistParams(referencedDistMethod,
										  referencedReplicationModel,
										  referencedColocationId) ||
			IsReferenceTableByDistParams(referencedDistMethod,
										 referencedReplicationModel);
		if (referencingIsCitusLocalOrRefTable && referencedIsCitusLocalOrRefTable)
		{
			EnsureSupportedFKeyBetweenCitusLocalAndRefTable(constraintForm,
															referencingReplicationModel,
															referencedReplicationModel,
															referencedTableId);

			ReleaseSysCache(heapTuple);
			continue;
		}

		/*
		 * Foreign keys from citus local tables or reference tables to distributed
		 * tables are not supported.
		 *
		 * We could support foreign keys from references tables to single-shard
		 * tables but this doesn't seem useful a lot. However, if we decide supporting
		 * this, then we need to expand relation access tracking check for the single-shard
		 * tables too.
		 */
		if (referencingIsCitusLocalOrRefTable && !referencedIsCitusLocalOrRefTable)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create foreign key constraint "
								   "since foreign keys from reference tables "
								   "and local tables to distributed tables "
								   "are not supported"),
							errdetail("Reference tables and local tables "
									  "can only have foreign keys to reference "
									  "tables and local tables")));
		}

		/*
		 * To enforce foreign constraints, tables must be co-located unless a
		 * reference table is referenced.
		 */
		bool referencedIsReferenceTable =
			IsReferenceTableByDistParams(referencedDistMethod,
										 referencedReplicationModel);
		if (!referencedIsReferenceTable && (
				referencingColocationId == INVALID_COLOCATION_ID ||
				referencingColocationId != referencedColocationId))
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
		bool referencedIsSingleShardTable =
			IsSingleShardTableByDistParams(referencedDistMethod,
										   referencedReplicationModel,
										   referencedColocationId);
		if (!referencedIsCitusLocalOrRefTable && !referencedIsSingleShardTable &&
			!foreignConstraintOnDistKey)
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
 * ForeignKeySetsNextValColumnToDefault returns true if at least one of the
 * columns specified in ON DELETE / UPDATE SET DEFAULT clauses default to
 * nextval().
 */
static bool
ForeignKeySetsNextValColumnToDefault(HeapTuple pgConstraintTuple)
{
	Form_pg_constraint pgConstraintForm =
		(Form_pg_constraint) GETSTRUCT(pgConstraintTuple);

	List *setDefaultAttrs = ForeignKeyGetDefaultingAttrs(pgConstraintTuple);
	AttrNumber setDefaultAttr = InvalidAttrNumber;
	foreach_declared_int(setDefaultAttr, setDefaultAttrs)
	{
		if (ColumnDefaultsToNextVal(pgConstraintForm->conrelid, setDefaultAttr))
		{
			return true;
		}
	}

	return false;
}


/*
 * ForeignKeyGetDefaultingAttrs returns a list of AttrNumbers
 * might be set to default ON DELETE or ON UPDATE.
 *
 * For example; if the foreign key has SET DEFAULT clause for
 * both actions, then returns a superset of the attributes that
 * might be set to DEFAULT on either of those actions.
 */
static List *
ForeignKeyGetDefaultingAttrs(HeapTuple pgConstraintTuple)
{
	bool isNull = false;
	Datum referencingColumnsDatum = SysCacheGetAttr(CONSTROID, pgConstraintTuple,
													Anum_pg_constraint_conkey, &isNull);
	if (isNull)
	{
		ereport(ERROR, (errmsg("got NULL conkey from catalog")));
	}

	List *referencingColumns =
		IntegerArrayTypeToList(DatumGetArrayTypeP(referencingColumnsDatum));

	Form_pg_constraint pgConstraintForm =
		(Form_pg_constraint) GETSTRUCT(pgConstraintTuple);
	if (pgConstraintForm->confupdtype == FKCONSTR_ACTION_SETDEFAULT)
	{
		/*
		 * Postgres doesn't allow specifying SET DEFAULT for a subset of
		 * the referencing columns for ON UPDATE action, so in that case
		 * we return all referencing columns regardless of what ON DELETE
		 * action says.
		 */
		return referencingColumns;
	}

	if (pgConstraintForm->confdeltype != FKCONSTR_ACTION_SETDEFAULT)
	{
		return NIL;
	}

	List *onDeleteSetDefColumnList = NIL;
	Datum onDeleteSetDefColumnsDatum = SysCacheGetAttr(CONSTROID, pgConstraintTuple,
													   Anum_pg_constraint_confdelsetcols,
													   &isNull);

	/*
	 * confdelsetcols being NULL means that "ON DELETE SET DEFAULT" doesn't
	 * specify which subset of columns should be set to DEFAULT, so fetching
	 * NULL from the catalog is also possible.
	 */
	if (!isNull)
	{
		onDeleteSetDefColumnList =
			IntegerArrayTypeToList(DatumGetArrayTypeP(onDeleteSetDefColumnsDatum));
	}

	if (list_length(onDeleteSetDefColumnList) == 0)
	{
		/*
		 * That means that all referencing columns need to be set to
		 * DEFAULT.
		 */
		return referencingColumns;
	}
	else
	{
		return onDeleteSetDefColumnList;
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
												char referencedReplicationModel,
												Oid referencedTableId)
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
			char *referencedTableName = get_rel_name(referencedTableId);
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot define foreign key constraint, "
								   "foreign keys from reference tables to "
								   "local tables can only be defined "
								   "with NO ACTION or RESTRICT behaviors"),
							errhint(USE_CREATE_REFERENCE_TABLE_HINT,
									referencedTableName)));
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
						errdetail("Citus currently supports foreign key constraints "
								  "only for \"citus.shard_replication_factor = 1\"."),
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
					errhint("first add local table to citus metadata "
							"by using SELECT citus_add_local_table_to_metadata('%s') "
							"and execute the ALTER TABLE command to create the "
							"foreign key to local table", localTableName)));
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
 * from/to given column. False otherwise.
 */
bool
ColumnAppearsInForeignKey(char *columnName, Oid relationId)
{
	int searchForeignKeyColumnFlags = SEARCH_REFERENCING_RELATION |
									  SEARCH_REFERENCED_RELATION;
	List *foreignKeysColumnAppeared =
		GetForeignKeyIdsForColumn(columnName, relationId, searchForeignKeyColumnFlags);
	return list_length(foreignKeysColumnAppeared) > 0;
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
	foreach_declared_oid(foreignKeyId, foreignKeyIdsColumnAppeared)
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
			foreignKeyIdsColumnAppeared = lappend_oid(foreignKeyIdsColumnAppeared,
													  constraintForm->oid);
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
 * GetForeignConstraintToReferenceTablesCommands takes in a relationId, and
 * returns the list of foreign constraint commands needed to reconstruct
 * foreign key constraints that the table is involved in as the "referencing"
 * one and the "referenced" table is a reference table.
 */
List *
GetForeignConstraintToReferenceTablesCommands(Oid relationId)
{
	int flags = INCLUDE_REFERENCING_CONSTRAINTS | INCLUDE_REFERENCE_TABLES;
	return GetForeignConstraintCommandsInternal(relationId, flags);
}


/*
 * GetForeignConstraintToReferenceTablesCommands takes in a relationId, and
 * returns the list of foreign constraint commands needed to reconstruct
 * foreign key constraints that the table is involved in as the "referenced"
 * one and the "referencing" table is a reference table.
 */
List *
GetForeignConstraintFromOtherReferenceTablesCommands(Oid relationId)
{
	int flags = INCLUDE_REFERENCED_CONSTRAINTS |
				EXCLUDE_SELF_REFERENCES |
				INCLUDE_REFERENCE_TABLES;
	return GetForeignConstraintCommandsInternal(relationId, flags);
}


/*
 * GetForeignConstraintToDistributedTablesCommands takes in a relationId, and
 * returns the list of foreign constraint commands needed to reconstruct
 * foreign key constraints that the table is involved in as the "referencing"
 * one and the "referenced" table is a distributed table.
 */
List *
GetForeignConstraintToDistributedTablesCommands(Oid relationId)
{
	int flags = INCLUDE_REFERENCING_CONSTRAINTS | INCLUDE_DISTRIBUTED_TABLES;
	return GetForeignConstraintCommandsInternal(relationId, flags);
}


/*
 * GetForeignConstraintFromDistributedTablesCommands takes in a relationId, and
 * returns the list of foreign constraint commands needed to reconstruct
 * foreign key constraints that the table is involved in as the "referenced"
 * one and the "referencing" table is a distributed table.
 */
List *
GetForeignConstraintFromDistributedTablesCommands(Oid relationId)
{
	int flags = INCLUDE_REFERENCED_CONSTRAINTS | INCLUDE_DISTRIBUTED_TABLES;
	return GetForeignConstraintCommandsInternal(relationId, flags);
}


/*
 * GetForeignConstraintCommandsInternal is a wrapper function to get the
 * DDL commands to recreate the foreign key constraints returned by
 * GetForeignKeyOids. See more details at the underlying function.
 */
List *
GetForeignConstraintCommandsInternal(Oid relationId, int flags)
{
	List *foreignKeyOids = GetForeignKeyOids(relationId, flags);

	List *foreignKeyCommands = NIL;

	int saveNestLevel = PushEmptySearchPath();

	Oid foreignKeyOid = InvalidOid;
	foreach_declared_oid(foreignKeyOid, foreignKeyOids)
	{
		char *statementDef = pg_get_constraintdef_command(foreignKeyOid);

		foreignKeyCommands = lappend(foreignKeyCommands, statementDef);
	}

	/* revert back to original search_path */
	PopEmptySearchPath(saveNestLevel);

	return foreignKeyCommands;
}


/*
 * GetFKeyCreationCommandsRelationInvolvedWithTableType returns a list of DDL
 * commands to recreate the foreign keys that relation with relationId is involved
 * with given table type.
 */
List *
GetFKeyCreationCommandsRelationInvolvedWithTableType(Oid relationId, int tableTypeFlag)
{
	int referencingFKeysFlag = INCLUDE_REFERENCING_CONSTRAINTS |
							   tableTypeFlag;
	List *referencingFKeyCreationCommands =
		GetForeignConstraintCommandsInternal(relationId, referencingFKeysFlag);

	/* already captured self referencing foreign keys, so use EXCLUDE_SELF_REFERENCES */
	int referencedFKeysFlag = INCLUDE_REFERENCED_CONSTRAINTS |
							  EXCLUDE_SELF_REFERENCES |
							  tableTypeFlag;
	List *referencedFKeyCreationCommands =
		GetForeignConstraintCommandsInternal(relationId, referencedFKeysFlag);
	return list_concat(referencingFKeyCreationCommands, referencedFKeyCreationCommands);
}


/*
 * DropFKeysRelationInvolvedWithTableType drops foreign keys that relation
 * with relationId is involved with given table type.
 */
void
DropFKeysRelationInvolvedWithTableType(Oid relationId, int tableTypeFlag)
{
	int referencingFKeysFlag = INCLUDE_REFERENCING_CONSTRAINTS |
							   tableTypeFlag;
	DropRelationForeignKeys(relationId, referencingFKeysFlag);

	/* already captured self referencing foreign keys, so use EXCLUDE_SELF_REFERENCES */
	int referencedFKeysFlag = INCLUDE_REFERENCED_CONSTRAINTS |
							  EXCLUDE_SELF_REFERENCES |
							  tableTypeFlag;
	DropRelationForeignKeys(relationId, referencedFKeysFlag);
}


/*
 * HasForeignKeyWithLocalTable returns true if relation has foreign key
 * relationship with a local table.
 */
bool
HasForeignKeyWithLocalTable(Oid relationId)
{
	List *foreignKeysWithLocalTables = GetForeignKeysWithLocalTables(relationId);
	return list_length(foreignKeysWithLocalTables) > 0;
}


/*
 * GetForeignKeysWithLocalTables returns a list of foreign keys for foreign key
 * relationships that relation has with local tables.
 */
static List *
GetForeignKeysWithLocalTables(Oid relationId)
{
	int referencingFKeysFlag = INCLUDE_REFERENCING_CONSTRAINTS |
							   INCLUDE_LOCAL_TABLES;
	List *referencingFKeyList = GetForeignKeyOids(relationId, referencingFKeysFlag);

	/* already captured self referencing foreign keys, so use EXCLUDE_SELF_REFERENCES */
	int referencedFKeysFlag = INCLUDE_REFERENCED_CONSTRAINTS |
							  EXCLUDE_SELF_REFERENCES |
							  INCLUDE_LOCAL_TABLES;
	List *referencedFKeyList = GetForeignKeyOids(relationId, referencedFKeysFlag);
	return list_concat(referencingFKeyList, referencedFKeyList);
}


/*
 * GetForeignKeysFromLocalTables returns a list of foreign keys where the referencing
 * relation is a local table.
 */
List *
GetForeignKeysFromLocalTables(Oid relationId)
{
	int referencedFKeysFlag = INCLUDE_REFERENCED_CONSTRAINTS |
							  INCLUDE_LOCAL_TABLES;
	List *referencingFKeyList = GetForeignKeyOids(relationId, referencedFKeysFlag);

	return referencingFKeyList;
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
 * ConstraintIsAUniquenessConstraint is a wrapper around ConstraintWithNameIsOfType
 * that returns true if given constraint name identifies a uniqueness constraint, i.e:
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
 * on relation with relationId and its type matches the input constraint type.
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
	foreach_declared_oid(foreignKeyOid, foreignKeyOids)
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
 * TableHasExternalForeignKeys returns true if the relation with relationId is
 * involved in a foreign key relationship other than the self-referencing ones.
 */
bool
TableHasExternalForeignKeys(Oid relationId)
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
		return false;
	}

	return true;
}


/*
 * ForeignConstraintMatchesFlags is a function with logic that's very specific
 * to GetForeignKeyOids. There's no reason to use it in any other context.
 */
static bool
ForeignConstraintMatchesFlags(Form_pg_constraint constraintForm,
							  int flags)
{
	if (constraintForm->contype != CONSTRAINT_FOREIGN)
	{
		return false;
	}

	bool inheritedConstraint = OidIsValid(constraintForm->conparentid);
	if (inheritedConstraint)
	{
		/*
		 * We only consider the constraints that are explicitly created on
		 * the table as we already process the constraints from parent tables
		 * implicitly when a command is issued
		 */
		return false;
	}

	bool excludeSelfReference = (flags & EXCLUDE_SELF_REFERENCES);
	bool isSelfReference = (constraintForm->conrelid == constraintForm->confrelid);
	if (excludeSelfReference && isSelfReference)
	{
		return false;
	}

	Oid otherTableId = InvalidOid;
	if (flags & INCLUDE_REFERENCING_CONSTRAINTS)
	{
		otherTableId = constraintForm->confrelid;
	}
	else
	{
		otherTableId = constraintForm->conrelid;
	}

	return IsTableTypeIncluded(otherTableId, flags);
}


/*
 * GetForeignKeyOidsForReferencedTable returns a list of foreign key OIDs that
 * reference the relationId and match the given flags.
 *
 * This is separated from GetForeignKeyOids because we need to scan pg_depend
 * instead of pg_constraint directly. The reason for this is that there is no
 * index on the confrelid of pg_constraint, so searching by that column
 * requires a seqscan.
 */
static List *
GetForeignKeyOidsForReferencedTable(Oid relationId, int flags)
{
	HTAB *foreignKeyOidsSet = CreateSimpleHashSetWithName(
		Oid, "ReferencingForeignKeyOidsSet");
	List *foreignKeyOidsList = NIL;
	ScanKeyData key[2];
	HeapTuple dependTup;
	Relation depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));
	SysScanDesc scan = systable_beginscan(depRel, DependReferenceIndexId, true,
										  NULL, lengthof(key), key);
	while (HeapTupleIsValid(dependTup = systable_getnext(scan)))
	{
		Form_pg_depend deprec = (Form_pg_depend) GETSTRUCT(dependTup);

		if (deprec->classid != ConstraintRelationId ||
			deprec->deptype != DEPENDENCY_NORMAL ||
			hash_search(foreignKeyOidsSet, &deprec->objid, HASH_FIND, NULL))
		{
			continue;
		}


		HeapTuple constraintTup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(
													  deprec->objid));
		if (!HeapTupleIsValid(constraintTup)) /* can happen during DROP TABLE */
		{
			continue;
		}

		Form_pg_constraint constraint = (Form_pg_constraint) GETSTRUCT(constraintTup);
		if (constraint->confrelid == relationId &&
			ForeignConstraintMatchesFlags(constraint, flags))
		{
			foreignKeyOidsList = lappend_oid(foreignKeyOidsList, constraint->oid);
			hash_search(foreignKeyOidsSet, &constraint->oid, HASH_ENTER, NULL);
		}
		ReleaseSysCache(constraintTup);
	}
	systable_endscan(scan);
	table_close(depRel, AccessShareLock);
	return foreignKeyOidsList;
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
	bool extractReferencing PG_USED_FOR_ASSERTS_ONLY = (flags &
														INCLUDE_REFERENCING_CONSTRAINTS);
	bool extractReferenced = (flags & INCLUDE_REFERENCED_CONSTRAINTS);

	/*
	 * Only one of them should be passed at a time since the way we scan
	 * pg_constraint differs for those columns. Anum_pg_constraint_conrelid
	 * supports index scan while Anum_pg_constraint_confrelid does not.
	 */
	Assert(!(extractReferencing && extractReferenced));
	Assert(extractReferencing || extractReferenced);

	if (extractReferenced)
	{
		return GetForeignKeyOidsForReferencedTable(relationId, flags);
	}

	List *foreignKeyOids = NIL;

	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	Relation pgConstraint = table_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&scanKey[0], Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relationId));

	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint,
													ConstraintRelidTypidNameIndexId, true,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple;
	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		if (ForeignConstraintMatchesFlags(constraintForm, flags))
		{
			foreignKeyOids = lappend_oid(foreignKeyOids, constraintForm->oid);
		}
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
 * GetReferencingTableId returns OID of the referencing relation for the foreign
 * key with foreignKeyId. If there is no such foreign key, then this function
 * returns InvalidOid.
 */
Oid
GetReferencingTableId(Oid foreignKeyId)
{
	HeapTuple heapTuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(foreignKeyId));
	if (!HeapTupleIsValid(heapTuple))
	{
		/* no such foreign key */
		return InvalidOid;
	}

	Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);
	Oid referencingTableId = constraintForm->conrelid;

	ReleaseSysCache(heapTuple);

	return referencingTableId;
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
	else if (IsCitusTableType(relationId, SINGLE_SHARD_DISTRIBUTED))
	{
		return (flags & INCLUDE_SINGLE_SHARD_TABLES) != 0;
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


/*
 * RelationInvolvedInAnyNonInheritedForeignKeys returns true if relation involved
 * in a foreign key that is not inherited from its parent relation.
 */
bool
RelationInvolvedInAnyNonInheritedForeignKeys(Oid relationId)
{
	List *referencingForeignKeys = GetForeignKeyOids(relationId,
													 INCLUDE_REFERENCING_CONSTRAINTS |
													 INCLUDE_ALL_TABLE_TYPES);

	/*
	 * We already capture self-referencing foreign keys above, so use
	 * EXCLUDE_SELF_REFERENCES here
	 */
	List *referencedForeignKeys = GetForeignKeyOids(relationId,
													INCLUDE_REFERENCED_CONSTRAINTS |
													EXCLUDE_SELF_REFERENCES |
													INCLUDE_ALL_TABLE_TYPES);
	List *foreignKeysRelationInvolved = list_concat(referencingForeignKeys,
													referencedForeignKeys);
	Oid foreignKeyId = InvalidOid;
	foreach_declared_oid(foreignKeyId, foreignKeysRelationInvolved)
	{
		HeapTuple heapTuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(foreignKeyId));
		if (!HeapTupleIsValid(heapTuple))
		{
			/* not possible but be on the safe side */
			continue;
		}

		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);
		Oid parentConstraintId = constraintForm->conparentid;
		if (!OidIsValid(parentConstraintId))
		{
			return true;
		}
	}

	return false;
}
