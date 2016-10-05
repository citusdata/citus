/*-------------------------------------------------------------------------
 *
 * create_distributed_relation.c
 *	  Routines relation to the creation of distributed relations.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_trigger.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "commands/sequence.h"
#include "commands/trigger.h"
#include "distributed/colocation_utils.h"
#include "distributed/distribution_column.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/pg_dist_colocation.h"
#include "distributed/pg_dist_partition.h"
#include "executor/spi.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/inval.h"


/* local function forward declarations */
static void ConvertToDistributedTable(Oid relationId,
									  text *distributionColumnText,
									  Oid distributionMethodOid, uint32 colocationId);
static char LookupDistributionMethod(Oid distributionMethodOid);
static void RecordDistributedRelationDependencies(Oid distributedRelationId,
												  Node *distributionKey);
static Oid SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
									int16 supportFunctionNumber);
static bool LocalTableEmpty(Oid tableId);
static void ErrorIfNotSupportedConstraint(Relation relation, char distributionMethod,
										  Var *distributionColumn);
static void InsertPgDistPartition(Oid relationId, char distributionMethod,
								  Node *distributionKey, uint32 colocationId);
static void CreateTruncateTrigger(Oid relationId);
static uint32 ColocationId(int shardCount, int replicationFactor,
						   Oid distributionColumnType);
static uint32 CreateColocationGroup(int shardCount, int replicationFactor,
									Oid distributionColumnType);
static uint32 GetNextColocationId(void);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_create_distributed_table);
PG_FUNCTION_INFO_V1(create_distributed_table);


/*
 * master_create_distributed_table accepts a table, distribution column and
 * method and performs the corresponding catalog changes.
 */
Datum
master_create_distributed_table(PG_FUNCTION_ARGS)
{
	Oid distributedRelationId = PG_GETARG_OID(0);
	text *distributionColumnText = PG_GETARG_TEXT_P(1);
	Oid distributionMethodOid = PG_GETARG_OID(2);

	ConvertToDistributedTable(distributedRelationId, distributionColumnText,
							  distributionMethodOid, INVALID_COLOCATION_ID);
	PG_RETURN_VOID();
}


/*
 * create_distributed_table accepts a table, distribution column and
 * distribution method, then it creates a distributed table.
 */
Datum
create_distributed_table(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	text *distributionColumnText = PG_GETARG_TEXT_P(1);
	Oid distributionMethodOid = PG_GETARG_OID(2);

	Relation distributedRelation = NULL;
	Relation pgDistColocation = NULL;
	Node *distributionKey = NULL;
	Var *distributionColumn = NULL;
	char *distributionColumnName = NULL;
	int distributionColumnType = 0;
	uint32 colocationId = INVALID_COLOCATION_ID;

	/* if distribution method is not hash, just create partition metadata */
	char distributionMethod = LookupDistributionMethod(distributionMethodOid);
	if (distributionMethod != DISTRIBUTE_BY_HASH)
	{
		ConvertToDistributedTable(relationId, distributionColumnText,
								  distributionMethodOid, INVALID_COLOCATION_ID);
		PG_RETURN_VOID();
	}

	/* get distribution column type */
	distributionColumnName = text_to_cstring(distributionColumnText);
	distributedRelation = relation_open(relationId, AccessShareLock);
	distributionKey = BuildDistributionKeyFromColumnName(distributedRelation,
														 distributionColumnName);
	distributionColumn = (Var *) distributionKey;
	distributionColumnType = distributionColumn->vartype;

	/*
	 * Get an exclusive lock on the colocation system catalog. Therefore, we
	 * can be sure that there will no modifications on the table until this
	 * transaction is committed.
	 */
	pgDistColocation = heap_open(DistColocationRelationId(), ExclusiveLock);

	/* check for existing colocations */
	colocationId = ColocationId(ShardCount, ShardReplicationFactor,
								distributionColumnType);

	/*
	 * If there is a colocation group for the current configuration, get a
	 * colocated table from the group and use its shards as a reference to
	 * create new shards. Otherwise, create a new colocation group and create
	 * shards with the default round robin algorithm.
	 */
	if (colocationId != INVALID_COLOCATION_ID)
	{
		char *relationName = get_rel_name(relationId);

		Oid colocatedTableId = ColocatedTableId(colocationId);
		ConvertToDistributedTable(relationId, distributionColumnText,
								  distributionMethodOid, colocationId);

		CreateColocatedShards(relationId, colocatedTableId);
		ereport(DEBUG2, (errmsg("table %s is added to colocation group: %d",
								relationName, colocationId)));
	}
	else
	{
		colocationId = CreateColocationGroup(ShardCount, ShardReplicationFactor,
											 distributionColumnType);
		ConvertToDistributedTable(relationId, distributionColumnText,
								  distributionMethodOid, colocationId);

		/* use the default way to create shards */
		CreateShardsWithRoundRobinPolicy(relationId, ShardCount, ShardReplicationFactor);
	}

	heap_close(pgDistColocation, NoLock);
	relation_close(distributedRelation, NoLock);

	PG_RETURN_VOID();
}


/*
 * ConvertToDistributedTable converts the given regular PostgreSQL table into a
 * distributed table. First, it checks if the given table can be distributed,
 * then it creates related tuple in pg_dist_partition.
 *
 * XXX: We should perform more checks here to see if this table is fit for
 * partitioning. At a minimum, we should validate the following: (i) this node
 * runs as the master node, (ii) table does not make use of the inheritance
 * mechanism, (iii) table does not own columns that are sequences, and (iv)
 * table does not have collated columns.
 */
static void
ConvertToDistributedTable(Oid relationId, text *distributionColumnText,
						  Oid distributionMethodOid, uint32 colocationId)
{
	Relation relation = NULL;
	TupleDesc relationDesc = NULL;
	char *relationName = NULL;
	char relationKind = 0;

	char distributionMethod = LookupDistributionMethod(distributionMethodOid);
	char *distributionColumnName = text_to_cstring(distributionColumnText);
	Node *distributionKey = NULL;
	Var *distributionColumn = NULL;

	/*
	 * Lock target relation with an access exclusive lock - there's no way to
	 * make sense of this table until we've committed, and we don't want
	 * multiple backends manipulating this relation.
	 */
	relation = relation_open(relationId, AccessExclusiveLock);
	relationDesc = RelationGetDescr(relation);
	relationName = RelationGetRelationName(relation);

	EnsureTableOwner(relationId);

	/* check that the relation is not already distributed */
	if (IsDistributedTable(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("table \"%s\" is already distributed",
							   relationName)));
	}

	/* verify target relation does not use WITH (OIDS) PostgreSQL feature */
	if (relationDesc->tdhasoid)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute relation: %s", relationName),
						errdetail("Distributed relations must not specify the WITH "
								  "(OIDS) option in their definitions.")));
	}

	/* verify target relation is either regular or foreign table */
	relationKind = relation->rd_rel->relkind;
	if (relationKind != RELKIND_RELATION && relationKind != RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("cannot distribute relation: %s",
							   relationName),
						errdetail("Distributed relations must be regular or "
								  "foreign tables.")));
	}

	/* check that the relation does not contain any rows */
	if (!LocalTableEmpty(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("cannot distribute relation \"%s\"",
							   relationName),
						errdetail("Relation \"%s\" contains data.",
								  relationName),
						errhint("Empty your table before distributing it.")));
	}

	distributionKey = BuildDistributionKeyFromColumnName(relation,
														 distributionColumnName);

	/* the distribution key should always be a Var for now */
	Assert(IsA(distributionKey, Var));
	distributionColumn = (Var *) distributionKey;

	/* check for support function needed by specified partition method */
	if (distributionMethod == DISTRIBUTE_BY_HASH)
	{
		Oid hashSupportFunction = SupportFunctionForColumn(distributionColumn,
														   HASH_AM_OID, HASHPROC);
		if (hashSupportFunction == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
							errmsg("could not identify a hash function for type %s",
								   format_type_be(distributionColumn->vartype)),
							errdatatype(distributionColumn->vartype),
							errdetail("Partition column types must have a hash function "
									  "defined to use hash partitioning.")));
		}
	}
	else if (distributionMethod == DISTRIBUTE_BY_RANGE)
	{
		Oid btreeSupportFunction = SupportFunctionForColumn(distributionColumn,
															BTREE_AM_OID, BTORDER_PROC);
		if (btreeSupportFunction == InvalidOid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("could not identify a comparison function for type %s",
							format_type_be(distributionColumn->vartype)),
					 errdatatype(distributionColumn->vartype),
					 errdetail("Partition column types must have a comparison function "
							   "defined to use range partitioning.")));
		}
	}

	ErrorIfNotSupportedConstraint(relation, distributionMethod, distributionColumn);

	InsertPgDistPartition(relationId, distributionMethod, distributionKey, colocationId);

	relation_close(relation, NoLock);

	/*
	 * PostgreSQL supports truncate trigger for regular relations only.
	 * Truncate on foreign tables is not supported.
	 */
	if (relationKind == RELKIND_RELATION)
	{
		CreateTruncateTrigger(relationId);
	}
}


/*
 * InsertPgDistPartition inserts a new tuple into pg_dist_partition.
 */
static void
InsertPgDistPartition(Oid relationId, char distributionMethod, Node *distributionKey,
					  uint32 colocationId)
{
	Relation pgDistPartition = NULL;
	const char replicationModel = 'c';
	char *distributionKeyString = NULL;

	HeapTuple newTuple = NULL;
	Datum newValues[Natts_pg_dist_partition];
	bool newNulls[Natts_pg_dist_partition];

	/* open system catalog and insert new tuple */
	pgDistPartition = heap_open(DistPartitionRelationId(), RowExclusiveLock);

	distributionKeyString = nodeToString(distributionKey);

	/* form new tuple for pg_dist_partition */
	memset(newValues, 0, sizeof(newValues));
	memset(newNulls, false, sizeof(newNulls));

	newValues[Anum_pg_dist_partition_logicalrelid - 1] =
		ObjectIdGetDatum(relationId);
	newValues[Anum_pg_dist_partition_partmethod - 1] =
		CharGetDatum(distributionMethod);
	newValues[Anum_pg_dist_partition_partkey - 1] =
		CStringGetTextDatum(distributionKeyString);
	newValues[Anum_pg_dist_partition_colocationid - 1] = colocationId;
	newValues[Anum_pg_dist_partition_repmodel - 1] = CharGetDatum(replicationModel);

	newTuple = heap_form_tuple(RelationGetDescr(pgDistPartition), newValues, newNulls);

	/* finally insert tuple, build index entries & register cache invalidation */
	simple_heap_insert(pgDistPartition, newTuple);
	CatalogUpdateIndexes(pgDistPartition, newTuple);
	CitusInvalidateRelcacheByRelid(relationId);

	RecordDistributedRelationDependencies(relationId, distributionKey);

	CommandCounterIncrement();
	heap_close(pgDistPartition, NoLock);
}


/*
 * ErrorIfNotSupportedConstraint run checks related to unique index / exclude
 * constraints.
 *
 * Forbid UNIQUE, PRIMARY KEY, or EXCLUDE constraints on append partitioned tables,
 * since currently there is no way of enforcing uniqueness for overlapping shards.
 *
 * Similarly, do not allow such constraints if they do not include partition column.
 * This check is important for two reasons. First, currently Citus does not enforce
 * uniqueness constraint on multiple shards.
 * Second, INSERT INTO .. ON CONFLICT (i.e., UPSERT) queries can be executed with no
 * further check for constraints.
 */
static void
ErrorIfNotSupportedConstraint(Relation relation, char distributionMethod,
							  Var *distributionColumn)
{
	char *relationName = RelationGetRelationName(relation);
	List *indexOidList = RelationGetIndexList(relation);
	ListCell *indexOidCell = NULL;

	foreach(indexOidCell, indexOidList)
	{
		Oid indexOid = lfirst_oid(indexOidCell);
		Relation indexDesc = index_open(indexOid, RowExclusiveLock);
		IndexInfo *indexInfo = NULL;
		AttrNumber *attributeNumberArray = NULL;
		bool hasDistributionColumn = false;
		int attributeCount = 0;
		int attributeIndex = 0;

		/* extract index key information from the index's pg_index info */
		indexInfo = BuildIndexInfo(indexDesc);

		/* only check unique indexes and exclusion constraints. */
		if (indexInfo->ii_Unique == false && indexInfo->ii_ExclusionOps == NULL)
		{
			index_close(indexDesc, NoLock);
			continue;
		}

		/*
		 * Citus cannot enforce uniqueness/exclusion constraints with overlapping shards.
		 * Thus, emit a warning for unique indexes and exclusion constraints on
		 * append partitioned tables.
		 */
		if (distributionMethod == DISTRIBUTE_BY_APPEND)
		{
			ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							  errmsg("table \"%s\" has a UNIQUE or EXCLUDE constraint",
									 relationName),
							  errdetail("UNIQUE constraints, EXCLUDE constraints, "
										"and PRIMARY KEYs on "
										"append-partitioned tables cannot be enforced."),
							  errhint("Consider using hash partitioning.")));
		}

		attributeCount = indexInfo->ii_NumIndexAttrs;
		attributeNumberArray = indexInfo->ii_KeyAttrNumbers;

		for (attributeIndex = 0; attributeIndex < attributeCount; attributeIndex++)
		{
			AttrNumber attributeNumber = attributeNumberArray[attributeIndex];
			bool uniqueConstraint = false;
			bool exclusionConstraintWithEquality = false;

			if (distributionColumn->varattno != attributeNumber)
			{
				continue;
			}

			uniqueConstraint = indexInfo->ii_Unique;
			exclusionConstraintWithEquality = (indexInfo->ii_ExclusionOps != NULL &&
											   OperatorImplementsEquality(
												   indexInfo->ii_ExclusionOps[
													   attributeIndex]));

			if (uniqueConstraint || exclusionConstraintWithEquality)
			{
				hasDistributionColumn = true;
				break;
			}
		}

		if (!hasDistributionColumn)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot distribute relation: \"%s\"",
								   relationName),
							errdetail("Distributed relations cannot have UNIQUE, "
									  "EXCLUDE, or PRIMARY KEY constraints that do not "
									  "include the partition column (with an equality "
									  "operator if EXCLUDE).")));
		}

		index_close(indexDesc, NoLock);
	}
}


/*
 * RecordDistributedRelationDependencies creates the dependency entries
 * necessary for a distributed relation in addition to the preexisting ones
 * for a normal relation.
 *
 * We create one dependency from the (now distributed) relation to the citus
 * extension to prevent the extension from being dropped while distributed
 * tables exist. Furthermore a dependency from pg_dist_partition's
 * distribution clause to the underlying columns is created, but it's marked
 * as being owned by the relation itself. That means the entire table can be
 * dropped, but the column itself can't. Neither can the type of the
 * distribution column be changed (c.f. ATExecAlterColumnType).
 */
static void
RecordDistributedRelationDependencies(Oid distributedRelationId, Node *distributionKey)
{
	ObjectAddress relationAddr = { 0, 0, 0 };
	ObjectAddress citusExtensionAddr = { 0, 0, 0 };

	relationAddr.classId = RelationRelationId;
	relationAddr.objectId = distributedRelationId;
	relationAddr.objectSubId = 0;

	citusExtensionAddr.classId = ExtensionRelationId;
	citusExtensionAddr.objectId = get_extension_oid("citus", false);
	citusExtensionAddr.objectSubId = 0;

	/* dependency from table entry to extension */
	recordDependencyOn(&relationAddr, &citusExtensionAddr, DEPENDENCY_NORMAL);

	/* make sure the distribution key column/expression does not just go away */
	recordDependencyOnSingleRelExpr(&relationAddr, distributionKey, distributedRelationId,
									DEPENDENCY_NORMAL, DEPENDENCY_NORMAL);
}


/*
 * LookupDistributionMethod maps the oids of citus.distribution_type enum
 * values to pg_dist_partition.partmethod values.
 *
 * The passed in oid has to belong to a value of citus.distribution_type.
 */
static char
LookupDistributionMethod(Oid distributionMethodOid)
{
	HeapTuple enumTuple = NULL;
	Form_pg_enum enumForm = NULL;
	char distributionMethod = 0;
	const char *enumLabel = NULL;

	enumTuple = SearchSysCache1(ENUMOID, ObjectIdGetDatum(distributionMethodOid));
	if (!HeapTupleIsValid(enumTuple))
	{
		ereport(ERROR, (errmsg("invalid internal value for enum: %u",
							   distributionMethodOid)));
	}

	enumForm = (Form_pg_enum) GETSTRUCT(enumTuple);
	enumLabel = NameStr(enumForm->enumlabel);

	if (strncmp(enumLabel, "append", NAMEDATALEN) == 0)
	{
		distributionMethod = DISTRIBUTE_BY_APPEND;
	}
	else if (strncmp(enumLabel, "hash", NAMEDATALEN) == 0)
	{
		distributionMethod = DISTRIBUTE_BY_HASH;
	}
	else if (strncmp(enumLabel, "range", NAMEDATALEN) == 0)
	{
		distributionMethod = DISTRIBUTE_BY_RANGE;
	}
	else
	{
		ereport(ERROR, (errmsg("invalid label for enum: %s", enumLabel)));
	}

	ReleaseSysCache(enumTuple);

	return distributionMethod;
}


/*
 *	SupportFunctionForColumn locates a support function given a column, an access method,
 *	and and id of a support function. This function returns InvalidOid if there is no
 *	support function for the operator class family of the column, but if the data type
 *	of the column has no default operator class whatsoever, this function errors out.
 */
static Oid
SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
						 int16 supportFunctionNumber)
{
	Oid operatorFamilyId = InvalidOid;
	Oid supportFunctionOid = InvalidOid;
	Oid operatorClassInputType = InvalidOid;
	Oid columnOid = partitionColumn->vartype;
	Oid operatorClassId = GetDefaultOpClass(columnOid, accessMethodId);

	/* currently only support using the default operator class */
	if (operatorClassId == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("data type %s has no default operator class for specified"
							   " partition method", format_type_be(columnOid)),
						errdatatype(columnOid),
						errdetail("Partition column types must have a default operator"
								  " class defined.")));
	}

	operatorFamilyId = get_opclass_family(operatorClassId);
	operatorClassInputType = get_opclass_input_type(operatorClassId);
	supportFunctionOid = get_opfamily_proc(operatorFamilyId, operatorClassInputType,
										   operatorClassInputType,
										   supportFunctionNumber);

	return supportFunctionOid;
}


/*
 * LocalTableEmpty function checks whether given local table contains any row and
 * returns false if there is any data. This function is only for local tables and
 * should not be called for distributed tables.
 */
static bool
LocalTableEmpty(Oid tableId)
{
	Oid schemaId = get_rel_namespace(tableId);
	char *schemaName = get_namespace_name(schemaId);
	char *tableName = get_rel_name(tableId);
	char *tableQualifiedName = quote_qualified_identifier(schemaName, tableName);

	int spiConnectionResult = 0;
	int spiQueryResult = 0;
	StringInfo selectExistQueryString = makeStringInfo();

	HeapTuple tuple = NULL;
	Datum hasDataDatum = 0;
	bool localTableEmpty = false;
	bool columnNull = false;
	bool readOnly = true;

	int rowId = 0;
	int attributeId = 1;

	AssertArg(!IsDistributedTable(tableId));

	spiConnectionResult = SPI_connect();
	if (spiConnectionResult != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	appendStringInfo(selectExistQueryString, SELECT_EXIST_QUERY, tableQualifiedName);

	spiQueryResult = SPI_execute(selectExistQueryString->data, readOnly, 0);
	if (spiQueryResult != SPI_OK_SELECT)
	{
		ereport(ERROR, (errmsg("execution was not successful \"%s\"",
							   selectExistQueryString->data)));
	}

	/* we expect that SELECT EXISTS query will return single value in a single row */
	Assert(SPI_processed == 1);

	tuple = SPI_tuptable->vals[rowId];
	hasDataDatum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, attributeId, &columnNull);
	localTableEmpty = !DatumGetBool(hasDataDatum);

	SPI_finish();

	return localTableEmpty;
}


/*
 * CreateTruncateTrigger creates a truncate trigger on table identified by relationId
 * and assigns citus_truncate_trigger() as handler.
 */
static void
CreateTruncateTrigger(Oid relationId)
{
	CreateTrigStmt *trigger = NULL;
	StringInfo triggerName = makeStringInfo();
	bool internal = true;

	appendStringInfo(triggerName, "truncate_trigger");

	trigger = makeNode(CreateTrigStmt);
	trigger->trigname = triggerName->data;
	trigger->relation = NULL;
	trigger->funcname = SystemFuncName("citus_truncate_trigger");
	trigger->args = NIL;
	trigger->row = false;
	trigger->timing = TRIGGER_TYPE_BEFORE;
	trigger->events = TRIGGER_TYPE_TRUNCATE;
	trigger->columns = NIL;
	trigger->whenClause = NULL;
	trigger->isconstraint = false;

	CreateTrigger(trigger, NULL, relationId, InvalidOid, InvalidOid, InvalidOid,
				  internal);
}


/*
 * ColocationId searches pg_dist_colocation for shard count, replication factor
 * and distribution column type. If a matching entry is found, it returns the
 * colocation id, otherwise it returns INVALID_COLOCATION_ID.
 */
static uint32
ColocationId(int shardCount, int replicationFactor, Oid distributionColumnType)
{
	uint32 colocationId = INVALID_COLOCATION_ID;
	HeapTuple colocationTuple = NULL;
	SysScanDesc scanDescriptor;
	ScanKeyData scanKey[3];
	int scanKeyCount = 3;
	bool indexOK = true;

	/* acquire a lock, so that no one can do this concurrently */
	Relation pgDistColocation = heap_open(DistColocationRelationId(), AccessShareLock);

	/* set scan arguments */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_colocation_shardcount,
				BTEqualStrategyNumber, F_INT4EQ, UInt32GetDatum(shardCount));
	ScanKeyInit(&scanKey[1], Anum_pg_dist_colocation_replicationfactor,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(replicationFactor));
	ScanKeyInit(&scanKey[2], Anum_pg_dist_colocation_distributioncolumntype,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(distributionColumnType));

	scanDescriptor = systable_beginscan(pgDistColocation,
										DistColocationConfigurationIndexId(),
										indexOK, NULL, scanKeyCount, scanKey);

	colocationTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(colocationTuple))
	{
		Form_pg_dist_colocation colocationForm =
			(Form_pg_dist_colocation) GETSTRUCT(colocationTuple);

		colocationId = colocationForm->colocationid;
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistColocation, AccessShareLock);

	return colocationId;
}


/*
 * CreateColocationGroup creates a new colocation id and writes it into
 * pg_dist_colocation with the given configuration.
 */
static uint32
CreateColocationGroup(int shardCount, int replicationFactor, Oid distributionColumnType)
{
	uint32 colocationId = INVALID_COLOCATION_ID;
	Relation pgDistColocation = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[Natts_pg_dist_colocation];
	bool isNulls[Natts_pg_dist_colocation];

	/* form new colocation tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	colocationId = GetNextColocationId();

	values[Anum_pg_dist_colocation_colocationid - 1] = UInt32GetDatum(colocationId);
	values[Anum_pg_dist_colocation_shardcount - 1] = UInt32GetDatum(shardCount);
	values[Anum_pg_dist_colocation_replicationfactor - 1] =
		UInt32GetDatum(replicationFactor);
	values[Anum_pg_dist_colocation_distributioncolumntype - 1] =
		ObjectIdGetDatum(distributionColumnType);

	/* open colocation relation and insert the new tuple */
	pgDistColocation = heap_open(DistColocationRelationId(), RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgDistColocation);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(pgDistColocation, heapTuple);
	CatalogUpdateIndexes(pgDistColocation, heapTuple);

	/* increment the counter so that next command can see the row */
	CommandCounterIncrement();
	heap_close(pgDistColocation, NoLock);

	return colocationId;
}


/*
 * GetNextColocationId allocates and returns a unique colocationId for the
 * colocation group to be created. This allocation occurs both in shared memory
 * and in write ahead logs; writing to logs avoids the risk of having
 * colocationId collisions.
 *
 * Please note that the caller is still responsible for finalizing colocationId
 * with the master node. Further note that this function relies on an internal
 * sequence created in initdb to generate unique identifiers.
 */
static uint32
GetNextColocationId()
{
	text *sequenceName = cstring_to_text(COLOCATIONID_SEQUENCE_NAME);
	Oid sequenceId = ResolveRelationId(sequenceName);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	Datum colocationIdDatum = 0;
	uint32 colocationId = INVALID_COLOCATION_ID;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique colocation id from sequence */
	colocationIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	colocationId = DatumGetUInt32(colocationIdDatum);

	return colocationId;
}
