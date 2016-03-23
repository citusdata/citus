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

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_opclass.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/pg_dist_partition.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/inval.h"


/* local function forward declarations */
static char LookupDistributionMethod(Oid distributionMethodOid);
static void RecordDistributedRelationDependencies(Oid distributedRelationId,
												  Node *distributionKey);
static Oid SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
									int16 supportFunctionNumber);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_create_distributed_table);


/*
 * master_create_distributed_table accepts a table, distribution column and
 * method and performs the corresponding catalog changes.
 *
 * XXX: We should perform more checks here to see if this table is fit for
 * partitioning. At a minimum, we should validate the following: (i) this node
 * runs as the master node, (ii) table does not make use of the inheritance
 * mechanism, (iii) table does not own columns that are sequences, and (iv)
 * table does not have collated columns. (v) table does not have
 * preexisting content.
 */
Datum
master_create_distributed_table(PG_FUNCTION_ARGS)
{
	Oid distributedRelationId = PG_GETARG_OID(0);
	text *distributionColumnText = PG_GETARG_TEXT_P(1);
	Oid distributionMethodOid = PG_GETARG_OID(2);

	Relation distributedRelation = NULL;
	char *distributedRelationName = NULL;
	char relationKind = '\0';

	Relation pgDistPartition = NULL;
	char distributionMethod = LookupDistributionMethod(distributionMethodOid);
	char *distributionColumnName = text_to_cstring(distributionColumnText);
	Node *distributionKey = NULL;
	Var *distributionColumn = NULL;
	char *distributionKeyString = NULL;

	List *indexOidList = NIL;
	ListCell *indexOidCell = NULL;

	HeapTuple newTuple = NULL;
	Datum newValues[Natts_pg_dist_partition];
	bool newNulls[Natts_pg_dist_partition];

	/*
	 * Lock target relation with an access exclusive lock - there's no way to
	 * make sense of this table until we've committed, and we don't want
	 * multiple backends manipulating this relation.
	 */
	distributedRelation = relation_open(distributedRelationId, AccessExclusiveLock);
	distributedRelationName = RelationGetRelationName(distributedRelation);

	/* open system catalog and insert new tuple */
	pgDistPartition = heap_open(DistPartitionRelationId(), RowExclusiveLock);

	/* check that the relation is not already distributed */
	if (IsDistributedTable(distributedRelationId))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("table \"%s\" is already distributed",
							   distributedRelationName)));
	}

	/* verify target relation is either regular or foreign table */
	relationKind = distributedRelation->rd_rel->relkind;
	if (relationKind != RELKIND_RELATION && relationKind != RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("cannot distribute relation: %s",
							   distributedRelationName),
						errdetail("Distributed relations must be regular or "
								  "foreign tables.")));
	}

	distributionKey = BuildDistributionKeyFromColumnName(distributedRelation,
														 distributionColumnName);
	distributionKeyString = nodeToString(distributionKey);

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

	/*
	 * Do not allow UNIQUE constraint and/or PRIMARY KEY on append partitioned tables,
	 * since currently there is no way of enforcing uniqueness for overlapping shards.
	 *
	 * Similarly, do not allow UNIQUE constraint and/or PRIMARY KEY if it does not
	 * include partition column. This check is important for two reasons. First,
	 * currently Citus does not enforce uniqueness constraint on multiple shards.
	 * Second, INSERT INTO .. ON CONFLICT (i.e., UPSERT) queries can be executed with no
	 * further check for constraints.
	 */
	indexOidList = RelationGetIndexList(distributedRelation);
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

		/* only check unique indexes */
		if (indexInfo->ii_Unique == false)
		{
			index_close(indexDesc, NoLock);
			continue;
		}

		/*
		 * Citus cannot enforce uniqueness constraints with overlapping shards. Thus,
		 * emit a warning for unique indexes on append partitioned tables.
		 */
		if (distributionMethod == DISTRIBUTE_BY_APPEND)
		{
			ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							  errmsg("table \"%s\" has a unique constraint",
									 distributedRelationName),
							  errdetail("Unique constraints and primary keys on "
										"append-partitioned tables cannot be enforced."),
							  errhint("Consider using hash partitioning.")));
		}

		attributeCount = indexInfo->ii_NumIndexAttrs;
		attributeNumberArray = indexInfo->ii_KeyAttrNumbers;

		for (attributeIndex = 0; attributeIndex < attributeCount; attributeIndex++)
		{
			AttrNumber attributeNumber = attributeNumberArray[attributeIndex];
			if (distributionColumn->varattno == attributeNumber)
			{
				hasDistributionColumn = true;
				break;
			}
		}

		if (!hasDistributionColumn)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot distribute relation: \"%s\"",
								   distributedRelationName),
							errdetail("Distributed relations cannot have "
									  "UNIQUE constraints or PRIMARY KEYs that do not "
									  "include the partition column.")));
		}

		index_close(indexDesc, NoLock);
	}

	/* form new tuple for pg_dist_partition */
	memset(newValues, 0, sizeof(newValues));
	memset(newNulls, false, sizeof(newNulls));

	newValues[Anum_pg_dist_partition_logicalrelid - 1] =
		ObjectIdGetDatum(distributedRelationId);
	newValues[Anum_pg_dist_partition_partmethod - 1] =
		CharGetDatum(distributionMethod);
	newValues[Anum_pg_dist_partition_partkey - 1] =
		CStringGetTextDatum(distributionKeyString);

	newTuple = heap_form_tuple(RelationGetDescr(pgDistPartition), newValues, newNulls);

	/* finally insert tuple, build index entries & register cache invalidation */
	simple_heap_insert(pgDistPartition, newTuple);
	CatalogUpdateIndexes(pgDistPartition, newTuple);
	CitusInvalidateRelcacheByRelid(distributedRelationId);

	RecordDistributedRelationDependencies(distributedRelationId, distributionKey);

	heap_close(pgDistPartition, NoLock);
	relation_close(distributedRelation, NoLock);

	PG_RETURN_VOID();
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
