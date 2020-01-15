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

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_constraint.h"
#if (PG_VERSION_NUM >= 120000)
#include "access/genam.h"
#endif
#include "catalog/pg_type.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/master_protocol.h"
#include "distributed/multi_join_order.h"
#include "distributed/version_compat.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"

/* Local functions forward declarations */
static bool HeapTupleOfForeignConstraintIncludesColumn(HeapTuple heapTuple, Oid
													   relationId, int pgConstraintKey,
													   char *columnName);
static void ForeignConstraintFindDistKeys(HeapTuple pgConstraintTuple,
										  Var *referencingDistColumn,
										  Var *referencedDistColumn,
										  int *referencingAttrIndex,
										  int *referencedAttrIndex);
static Oid GetCoordinatorLocalTableHavingFKeyWithReferenceTable(Oid referenceTableOid);

/*
 * ConstraintIsAForeignKeyToReferenceTable checks if the given constraint is a
 * foreign key constraint from the given relation to a reference table. It does
 * that by scanning pg_constraint for foreign key constraints.
 */
bool
ConstraintIsAForeignKeyToReferenceTable(char *constraintName, Oid relationId)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool foreignKeyToReferenceTable = false;


	Relation pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_contype, BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(CONSTRAINT_FOREIGN));
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint, InvalidOid, false,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);
		char *tupleConstraintName = (constraintForm->conname).data;

		if (strncmp(constraintName, tupleConstraintName, NAMEDATALEN) != 0 ||
			constraintForm->conrelid != relationId)
		{
			heapTuple = systable_getnext(scanDescriptor);
			continue;
		}

		Oid referencedTableId = constraintForm->confrelid;

		Assert(IsDistributedTable(referencedTableId));

		if (PartitionMethod(referencedTableId) == DISTRIBUTE_BY_NONE)
		{
			foreignKeyToReferenceTable = true;
			break;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, AccessShareLock);

	return foreignKeyToReferenceTable;
}


/*
 * ErrorIfUnsupportedForeignConstraintExists runs checks related to foreign constraints and
 * errors out if it is not possible to create one of the foreign constraint in distributed
 * environment.
 *
 * To support foreign constraints in this function, we require that;
 * - If referencing and referenced tables are hash-distributed
 *		- Referencing and referenced tables are co-located.
 *      - Foreign constraint is defined over distribution column.
 *		- ON DELETE/UPDATE SET NULL, ON DELETE/UPDATE SET DEFAULT and ON UPDATE CASCADE options
 *        are not used.
 *      - Replication factors of referencing and referenced table are 1.
 * - If referenced table is a reference table
 *      - ON DELETE/UPDATE SET NULL, ON DELETE/UPDATE SET DEFAULT and ON UPDATE CASCADE options
 *        are not used on the distribution key of the referencing column.
 * - If referencing table is a reference table, error out if the referenced table is not a
 *   a reference table.
 *
 * Note that checks performed in this functions are only done via PostprocessAlterTableStmt
 * function. There is another case ,allowed by Citus, but error'ed in this function, creating
 * foreign key constraint between a reference table and a coordinator local table. The rationale
 * behind it is to allow defining foreign keys between coordinator local tables and reference
 * tables only via ALTER TABLE ADD CONSTRAINT ... commands. This function, prevents upgrading
 * "a local table involved in a foreign key constraint with another local table" to a reference
 * table.
 * See also ErrorIfUnsupportedAlterAddDropFKeyBetweenReferecenceAndLocalTable and its usage
 */
void
ErrorIfUnsupportedForeignConstraintExists(Relation relation, char distributionMethod,
										  Var *distributionColumn,
										  uint32 colocationId)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	Oid relationOid = relation->rd_id;
	bool relationIsDistributed = IsDistributedTable(relationOid);
	bool relationIsReferenceTable = (distributionMethod == DISTRIBUTE_BY_NONE);
	bool relationNotReplicated = true;

	if (relationIsDistributed)
	{
		/* ALTER TABLE command is applied over single replicated table */
		relationNotReplicated = SingleReplicatedTable(relationOid);
	}
	else
	{
		/* Creating single replicated table with foreign constraint */
		relationNotReplicated = (ShardReplicationFactor == 1);
	}

	Relation pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&scanKey[0], Anum_pg_constraint_contype, BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(CONSTRAINT_FOREIGN));
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint,
													InvalidOid,
													false, NULL,
													scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);

	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		if (constraintForm->conrelid == relationOid)
		{
			/* alias variables from the referencing table perspective */
			Oid referencingTableId = relationOid;
			char referencingDistMethod = distributionMethod;
			Var *referencingDistKey = distributionColumn;
			uint32 referencingColocationId = colocationId;

			bool referencingIsDistributed = relationIsDistributed;
			bool referencingIsReferenceTable = relationIsReferenceTable;
			bool referencingNotReplicated = relationNotReplicated;

			char referencedDistMethod = 0;
			Var *referencedDistKey = NULL;
			uint32 referencedColocationId = INVALID_COLOCATION_ID;

			Oid referencedTableId = constraintForm->confrelid;
			bool referencedIsDistributed = IsDistributedTable(referencedTableId);
			int referencedAttrIndex = -1;

			bool selfReferencingTable = (referencingTableId == referencedTableId);

			/* set referenced table related variables here if table is referencing itself */

			if (!selfReferencingTable)
			{
				if (referencedIsDistributed)
				{
					referencedDistMethod = PartitionMethod(referencedTableId);
					referencedDistKey = (referencedDistMethod == DISTRIBUTE_BY_NONE) ?
										NULL :
										DistPartitionKey(referencedTableId);
					referencedColocationId = TableColocationId(referencedTableId);
				}
			}
			else
			{
				referencedDistMethod = referencingDistMethod;
				referencedDistKey = referencingDistKey;
				referencedColocationId = referencingColocationId;
			}

			bool referencedIsReferenceTable = (referencedDistMethod ==
											   DISTRIBUTE_BY_NONE);

			/* foreign key constraint between reference tables */
			if (referencingIsReferenceTable && referencedIsReferenceTable)
			{
				/*
				 * We support foreign keys between reference tables. No more checks
				 * are necessary.
				 */
				heapTuple = systable_getnext(scanDescriptor);
				continue;
			}

			/* foreign key constraint from a reference table to a local table */
			if (referencingIsReferenceTable && !referencedIsDistributed)
			{
				/*
				 * Upgrading "a local table having a foreign key constraint to another
				 * local table" to a reference table is not supported.
				 */
				ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								errmsg("cannot create foreign key constraint"),
								errdetail(
									"Local table having a foreign key constraint to another "
									"local table cannot be upgraded to a reference table"),
								errhint(
									"To define foreign key constraint from a reference table to a "
									"local table, use ALTER TABLE ADD CONSTRAINT ... command after "
									"creating the reference table without a foreing key")));
			}

			/* distributed table to local table */
			if (!referencingIsReferenceTable && referencingIsDistributed &&
				!referencedIsDistributed)
			{
				ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								errmsg("cannot create foreign key constraint"),
								errdetail(
									"Foreign keys from distributed tables to local tables are not supported")));
			}


			if (referencingIsReferenceTable && referencedIsDistributed &&
				!referencedIsReferenceTable)
			{
				/*
				 * Foreign keys from reference tables to distributed tables are not
				 * supported.
				 */
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot create foreign key constraint "
									   "since foreign keys from reference tables "
									   "to distributed tables are not supported"),
								errdetail("A reference table can only have reference "
										  "keys to other reference tables or a local table "
										  "in coordinator")));
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

			int referencingAttrIndex = -1;

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
								errhint(
									"Please change \"citus.shard_replication_factor to "
									"1\". To learn more about using foreign keys with "
									"other replication factors, please contact us at "
									"https://citusdata.com/about/contact_us.")));
			}
		}
		else if (constraintForm->confrelid == relationOid)
		{
			/* alias variables from the referenced table perspective */
			bool referencedIsReferenceTable = relationIsReferenceTable;

			Oid referencingTableId = constraintForm->conrelid;
			bool referencingIsDistributed = IsDistributedTable(referencingTableId);

			/* foreign key constraint from a local table to a reference table */
			if (!referencingIsDistributed && referencedIsReferenceTable)
			{
				/*
				 * Upgrading "a local table referenced by another local table"
				 * to a reference table is supported, but not encouraged.
				 */
				ereport(WARNING, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								  errmsg(
									  "should not create foreign key constraint in this way"),
								  errdetail(
									  "Local table referenced by another local table should not "
									  "be upgraded to a reference table"),
								  errhint(
									  "To define foreign key constraint from a local table to a "
									  "reference table properly, use ALTER TABLE ADD CONSTRAINT ... "
									  "command after creating the reference table without a foreing key")));
			}
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, AccessShareLock);
}


/*
 * ForeignConstraintFindDistKeys finds the index of the given distribution columns
 * in the given foreig key constraint and returns them in referencingAttrIndex
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
 * ErrorIfUnsupportedAlterAddDropFKeyBetweenReferecenceAndLocalTable runs checks related to ALTER
 * ADD / DROP foreign key constraints between local tables and reference tables.
 *
 * If constraint is not NULL, then we perform checks for ADD command, otherwise we check for DROP.
 */
void
ErrorIfUnsupportedAlterAddDropFKeyBetweenReferecenceAndLocalTable(Oid referencingTableOid,
																  Oid
																  referencedTableOid,
																  Constraint *constraint)
{
	bool referencingIsDistributed = IsDistributedTable(referencingTableOid);
	char referencingDistMethod = 0;
	bool referencingIsReferenceTable = false;

	bool referencedIsDistributed = IsDistributedTable(referencedTableOid);
	char referencedDistMethod = 0;
	bool referencedIsReferenceTable = false;

	if (referencingIsDistributed)
	{
		referencingDistMethod = PartitionMethod(referencingTableOid);
		referencingIsReferenceTable = (referencingDistMethod == DISTRIBUTE_BY_NONE);
	}

	if (referencedIsDistributed)
	{
		referencedDistMethod = PartitionMethod(referencedTableOid);
		referencedIsReferenceTable = (referencedDistMethod == DISTRIBUTE_BY_NONE);
	}

	/* reference table <> local table */
	if ((referencingIsReferenceTable && !referencedIsDistributed) ||
		(!referencingIsDistributed && referencedIsReferenceTable))
	{
		/* constraint is not NULL when the command is ALTER TABLE ADD constraint */
		if (constraint != NULL)
		{
			/*
			 * "ALTER TABLE reference_table ADD CONSTRAINT fkey FOREIGN KEY REFERENCES
			 * local_table ON DELETE (UPDATE) CASCADE" commands are not supported
			 */
			bool onDeleteOrOnUpdateCascade = (constraint->fk_upd_action ==
											  FKCONSTR_ACTION_CASCADE ||
											  constraint->fk_del_action ==
											  FKCONSTR_ACTION_CASCADE);

			bool fromReferenceTableToLocalTable = referencingIsReferenceTable &&
												  !referencedIsDistributed;

			if (fromReferenceTableToLocalTable && onDeleteOrOnUpdateCascade)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg(
									"cannot create foreign key constraint"),
								errdetail(
									"Foreign key constraints from reference tables to coordinator "
									"local tables cannot enforce ON DELETE / UPDATE CASCADE behaviour")));
			}
		}

		/* ALTER TABLE ADD / DROP fkey constraint */

		/*
		 * For now, we do not allow ALTER TABLE ADD/DROP fkey commands in multi
		 * statement transactions
		 */
		if (IsMultiStatementTransaction())
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg(
								"cannot ADD/DROP foreign key constraint between coordinator "
								"local tables and reference tables in a transaction block, "
								"udf block, or distributed CTE subquery")));
		}

		/*
		 * Check if we are in the coordinator and coordinator can have reference
		 * table replicas
		 */
		if (!CanUseCoordinatorLocalTablesWithReferenceTables())
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							errmsg(
								"cannot ADD/DROP foreign key constraint"),
							errdetail(
								"Referenced table must be a distributed table"
								" or a reference table or a local table in coordinator."),
							errhint(
								"To ADD/DROP foreign constraint between reference tables "
								"and coordinator local tables, consider adding coordinator "
								"to pg_dist_node as well.")));
		}
	}
}


/*
 * ErrorIfCoordinatorHasLocalTableHavingFKeyWithReferenceTable errors out if we
 * if coordinator has reference table replica for given referance table and if
 * it is involved in a foreign key constraint with a coordinator local table.
 */
void
ErrorIfCoordinatorHasLocalTableHavingFKeyWithReferenceTable(Oid referenceTableOid)
{
	Oid localTableOid = GetCoordinatorLocalTableHavingFKeyWithReferenceTable(
		referenceTableOid);

	/*
	 * There is a coordinator local table involved in a foreign key constraint
	 * with reference table with referenceTableOid
	 */
	if (OidIsValid(localTableOid))
	{
		const char *localTableName = get_rel_name(localTableOid);
		const char *referenceTableName = get_rel_name(referenceTableOid);

		ereport(ERROR, (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
						errmsg(
							"cannot remove reference table placement from coordinator"),
						errdetail(
							"Local table \"%s\" is involved in a foreign key constraint with "
							"refence table \"%s\", which has replica in coordinator",
							localTableName, referenceTableName),
						errhint(
							"DROP foreign key constraint between them or drop referenced "
							"table with DROP ... CASCADE.")));
	}
}


/*
 * GetCoordinatorLocalTableHavingFKeyWithReferenceTable returns OID of the
 * local table that is involved in a foreign key constraint with the reference
 * table with referenceTableOid.
 * It does that by scanning pg_constraint for foreign key constraints.
 *
 * If there does not exist such a foreign key constraint, returns InvalidOid.
 * If there exists more than one such a foreign key constraint, we return the
 * first local table we ecounter while scanning pg_constraint
 */
static Oid
GetCoordinatorLocalTableHavingFKeyWithReferenceTable(Oid referenceTableOid)
{
	Oid referenceTableShardOid = GetOnlyShardOidOfReferenceTable(referenceTableOid);

	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	Relation pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&scanKey[0], Anum_pg_constraint_contype, BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(CONSTRAINT_FOREIGN));
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint,
													InvalidOid,
													false, NULL,
													scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);

	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		if (constraintForm->confrelid == referenceTableShardOid)
		{
			Oid referencingTableId = constraintForm->conrelid;

			if (!IsDistributedTable(referencingTableId))
			{
				return referencingTableId;
			}
		}
		else if (constraintForm->conrelid == referenceTableShardOid)
		{
			Oid referencedTableId = constraintForm->confrelid;

			if (!IsDistributedTable(referencedTableId))
			{
				return referencedTableId;
			}
		}

		/*
		 * If this is not such a relation from/to the given relation, we should
		 * simply skip.
		 */
		heapTuple = systable_getnext(scanDescriptor);
		continue;
	}

	/* clean up scan and close system catalog */

	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, AccessShareLock);

	return InvalidOid;
}


/*
 * ColumnAppearsInForeignKeyToReferenceTable checks if there is a foreign key
 * constraint from/to a reference table on the given column. We iterate
 * pg_constraint to fetch the constraint on the given relationId and find
 * if any of the constraints includes the given column.
 */
bool
ColumnAppearsInForeignKeyToReferenceTable(char *columnName, Oid relationId)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool foreignKeyToReferenceTableIncludesGivenColumn = false;

	Relation pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_contype, BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(CONSTRAINT_FOREIGN));

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
		Assert(IsDistributedTable(referencedTableId));
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
	heap_close(pgConstraint, AccessShareLock);

	return foreignKeyToReferenceTableIncludesGivenColumn;
}


/*
 * GetTableForeignConstraints takes in a relationId, and returns the list of foreign
 * constraint commands needed to reconstruct foreign constraints of that table.
 */
List *
GetTableForeignConstraintCommands(Oid relationId)
{
	List *tableForeignConstraints = NIL;

	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	/*
	 * Set search_path to NIL so that all objects outside of pg_catalog will be
	 * schema-prefixed. pg_catalog will be added automatically when we call
	 * PushOverrideSearchPath(), since we set addCatalog to true;
	 */
	OverrideSearchPath *overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = true;
	PushOverrideSearchPath(overridePath);

	/* open system catalog and scan all constraints that belong to this table */
	Relation pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&scanKey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
				relationId);
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint,
													ConstraintRelidTypidNameIndexId,
													true, NULL,
													scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		bool inheritedConstraint = OidIsValid(constraintForm->conparentid);

		if (!inheritedConstraint && constraintForm->contype == CONSTRAINT_FOREIGN)
		{
			Oid constraintId = get_relation_constraint_oid(relationId,
														   constraintForm->conname.data,
														   true);
			char *statementDef = pg_get_constraintdef_command(constraintId);

			tableForeignConstraints = lappend(tableForeignConstraints, statementDef);
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, AccessShareLock);

	/* revert back to original search_path */
	PopOverrideSearchPath();

	return tableForeignConstraints;
}


/*
 * HasForeignKeyToReferenceTable function scans the pgConstraint table to
 * fetch all of the constraints on the given relationId and see if at least one
 * of them is a foreign key referencing to a reference table.
 */
bool
HasForeignKeyToReferenceTable(Oid relationId)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool hasForeignKeyToReferenceTable = false;

	Relation pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&scanKey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
				relationId);
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint,
													ConstraintRelidTypidNameIndexId,
													true, NULL,
													scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		if (constraintForm->contype != CONSTRAINT_FOREIGN)
		{
			heapTuple = systable_getnext(scanDescriptor);
			continue;
		}

		Oid referencedTableId = constraintForm->confrelid;

		if (!IsDistributedTable(referencedTableId))
		{
			/* TODO: This line should be already here ?? */
			heapTuple = systable_getnext(scanDescriptor);
			continue;
		}

		if (PartitionMethod(referencedTableId) == DISTRIBUTE_BY_NONE)
		{
			hasForeignKeyToReferenceTable = true;
			break;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, NoLock);
	return hasForeignKeyToReferenceTable;
}


/*
 * TableReferenced function checks whether given table is referenced by another table
 * via foreign constraints. If it is referenced, this function returns true. To check
 * that, this function searches for the given relation in the pg_constraint system
 * catalog table. However since there are no indexes for the column we search for,
 * this function performs sequential search. So call this function with caution.
 */
bool
TableReferenced(Oid relationId)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	Oid scanIndexId = InvalidOid;
	bool useIndex = false;

	Relation pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_confrelid, BTEqualStrategyNumber, F_OIDEQ,
				relationId);
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint, scanIndexId, useIndex,
													NULL,
													scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);

	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		if (constraintForm->contype == CONSTRAINT_FOREIGN)
		{
			systable_endscan(scanDescriptor);
			heap_close(pgConstraint, NoLock);

			return true;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */

	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, NoLock);

	return false;
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
 * TableReferencing function checks whether given table is referencing by another table
 * via foreign constraints. If it is referencing, this function returns true. To check
 * that, this function searches given relation at pg_constraints system catalog. However
 * since there is no index for the column we searched, this function performs sequential
 * search, therefore call this function with caution.
 */
bool
TableReferencing(Oid relationId)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	Oid scanIndexId = InvalidOid;
	bool useIndex = false;

	Relation pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
				relationId);
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint, scanIndexId, useIndex,
													NULL,
													scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);

		if (constraintForm->contype == CONSTRAINT_FOREIGN)
		{
			systable_endscan(scanDescriptor);
			heap_close(pgConstraint, NoLock);

			return true;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, NoLock);

	return false;
}


/*
 * ConstraintIsAForeignKey returns true if the given constraint name
 * is a foreign key to defined on the relation.
 */
bool
ConstraintIsAForeignKey(char *constraintNameInput, Oid relationId)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	Relation pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_contype, BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(CONSTRAINT_FOREIGN));
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint, InvalidOid, false,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);
		char *constraintName = (constraintForm->conname).data;

		if (strncmp(constraintName, constraintNameInput, NAMEDATALEN) == 0 &&
			constraintForm->conrelid == relationId)
		{
			systable_endscan(scanDescriptor);
			heap_close(pgConstraint, AccessShareLock);

			return true;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, AccessShareLock);

	return false;
}
