/*
 * multi_partitioning_utils.c
 *	  Utility functions for declarative partitioning
 *
 * Copyright (c) 2017, Citus Data, Inc.
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/partition.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#if (PG_VERSION_NUM < 110000)
#include "catalog/pg_constraint_fn.h"
#endif
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/shardinterval_utils.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#if PG_VERSION_NUM >= 120000
#include "partitioning/partdesc.h"
#endif
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


static char * PartitionBound(Oid partitionId);


/*
 * Returns true if the given relation is a partitioned table.
 */
bool
PartitionedTable(Oid relationId)
{
	Relation rel = heap_open(relationId, AccessShareLock);
	bool partitionedTable = false;

	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		partitionedTable = true;
	}

	/* keep the lock */
	heap_close(rel, NoLock);

	return partitionedTable;
}


/*
 * Returns true if the given relation is a partitioned table. The function
 * doesn't acquire any locks on the input relation, thus the caller is
 * reponsible for holding the appropriate locks.
 */
bool
PartitionedTableNoLock(Oid relationId)
{
	Relation rel = try_relation_open(relationId, NoLock);
	bool partitionedTable = false;

	/* don't error out for tables that are dropped */
	if (rel == NULL)
	{
		return false;
	}

	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		partitionedTable = true;
	}

	/* keep the lock */
	heap_close(rel, NoLock);

	return partitionedTable;
}


/*
 * Returns true if the given relation is a partition.
 */
bool
PartitionTable(Oid relationId)
{
	Relation rel = heap_open(relationId, AccessShareLock);
	bool partitionTable = false;

	partitionTable = rel->rd_rel->relispartition;

	/* keep the lock */
	heap_close(rel, NoLock);

	return partitionTable;
}


/*
 * Returns true if the given relation is a partition.  The function
 * doesn't acquire any locks on the input relation, thus the caller is
 * reponsible for holding the appropriate locks.
 */
bool
PartitionTableNoLock(Oid relationId)
{
	Relation rel = try_relation_open(relationId, NoLock);
	bool partitionTable = false;

	/* don't error out for tables that are dropped */
	if (rel == NULL)
	{
		return false;
	}

	partitionTable = rel->rd_rel->relispartition;

	/* keep the lock */
	heap_close(rel, NoLock);

	return partitionTable;
}


/*
 * IsChildTable returns true if the table is inherited. Note that
 * partition tables inherites by default. However, this function
 * returns false if the given table is a partition.
 */
bool
IsChildTable(Oid relationId)
{
	Relation pgInherits = NULL;
	SysScanDesc scan = NULL;
	ScanKeyData key[1];
	HeapTuple inheritsTuple = NULL;
	bool tableInherits = false;

	pgInherits = heap_open(InheritsRelationId, AccessShareLock);

	ScanKeyInit(&key[0], Anum_pg_inherits_inhrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));

	scan = systable_beginscan(pgInherits, InvalidOid, false,
							  NULL, 1, key);

	while ((inheritsTuple = systable_getnext(scan)) != NULL)
	{
		Oid inheritedRelationId =
			((Form_pg_inherits) GETSTRUCT(inheritsTuple))->inhrelid;

		if (relationId == inheritedRelationId)
		{
			tableInherits = true;
			break;
		}
	}

	systable_endscan(scan);
	heap_close(pgInherits, AccessShareLock);

	if (tableInherits && PartitionTable(relationId))
	{
		tableInherits = false;
	}

	return tableInherits;
}


/*
 * IsParentTable returns true if the table is inherited. Note that
 * partitioned tables inherited by default. However, this function
 * returns false if the given table is a partitioned table.
 */
bool
IsParentTable(Oid relationId)
{
	Relation pgInherits = NULL;
	SysScanDesc scan = NULL;
	ScanKeyData key[1];
	bool tableInherited = false;

	pgInherits = heap_open(InheritsRelationId, AccessShareLock);

	ScanKeyInit(&key[0], Anum_pg_inherits_inhparent,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));

	scan = systable_beginscan(pgInherits, InheritsParentIndexId, true,
							  NULL, 1, key);

	if (systable_getnext(scan) != NULL)
	{
		tableInherited = true;
	}
	systable_endscan(scan);
	heap_close(pgInherits, AccessShareLock);

	if (tableInherited && PartitionedTable(relationId))
	{
		tableInherited = false;
	}

	return tableInherited;
}


/*
 * Wrapper around get_partition_parent
 *
 * Note: Because this function assumes that the relation whose OID is passed
 * as an argument will have precisely one parent, it should only be called
 * when it is known that the relation is a partition.
 */
Oid
PartitionParentOid(Oid partitionOid)
{
	Oid partitionParentOid = InvalidOid;

	partitionParentOid = get_partition_parent(partitionOid);

	return partitionParentOid;
}


/*
 * Takes a parent relation and returns Oid list of its partitions. The
 * function errors out if the given relation is not a parent.
 */
List *
PartitionList(Oid parentRelationId)
{
	Relation rel = heap_open(parentRelationId, AccessShareLock);
	List *partitionList = NIL;

	int partitionIndex = 0;
	int partitionCount = 0;

	if (!PartitionedTable(parentRelationId))
	{
		char *relationName = get_rel_name(parentRelationId);

		ereport(ERROR, (errmsg("\"%s\" is not a parent table", relationName)));
	}

	Assert(rel->rd_partdesc != NULL);

	partitionCount = rel->rd_partdesc->nparts;
	for (partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex)
	{
		partitionList =
			lappend_oid(partitionList, rel->rd_partdesc->oids[partitionIndex]);
	}

	/* keep the lock */
	heap_close(rel, NoLock);

	return partitionList;
}


/*
 * GenerateDetachPartitionCommand gets a partition table and returns
 * "ALTER TABLE parent_table DETACH PARTITION partitionName" command.
 */
char *
GenerateDetachPartitionCommand(Oid partitionTableId)
{
	StringInfo detachPartitionCommand = makeStringInfo();
	Oid parentId = InvalidOid;
	char *tableQualifiedName = NULL;
	char *parentTableQualifiedName = NULL;

	if (!PartitionTable(partitionTableId))
	{
		char *relationName = get_rel_name(partitionTableId);

		ereport(ERROR, (errmsg("\"%s\" is not a partition", relationName)));
	}

	parentId = get_partition_parent(partitionTableId);
	tableQualifiedName = generate_qualified_relation_name(partitionTableId);
	parentTableQualifiedName = generate_qualified_relation_name(parentId);

	appendStringInfo(detachPartitionCommand,
					 "ALTER TABLE IF EXISTS %s DETACH PARTITION %s;",
					 parentTableQualifiedName, tableQualifiedName);

	return detachPartitionCommand->data;
}


/*
 * GenereatePartitioningInformation returns the partitioning type and partition column
 * for the given parent table in the form of "PARTITION TYPE (partitioning column(s)/expression(s))".
 */
char *
GeneratePartitioningInformation(Oid parentTableId)
{
	char *partitionBoundCString = "";
	Datum partitionBoundDatum = 0;

	if (!PartitionedTable(parentTableId))
	{
		char *relationName = get_rel_name(parentTableId);

		ereport(ERROR, (errmsg("\"%s\" is not a parent table", relationName)));
	}

	partitionBoundDatum = DirectFunctionCall1(pg_get_partkeydef,
											  ObjectIdGetDatum(parentTableId));

	partitionBoundCString = TextDatumGetCString(partitionBoundDatum);

	return partitionBoundCString;
}


/*
 * GenerateAttachShardPartitionCommand generates command to attach a child table
 * table to its parent in a partitioning hierarchy.
 */
char *
GenerateAttachShardPartitionCommand(ShardInterval *shardInterval)
{
	Oid schemaId = get_rel_namespace(shardInterval->relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *escapedSchemaName = quote_literal_cstr(schemaName);

	char *command = GenerateAlterTableAttachPartitionCommand(shardInterval->relationId);
	char *escapedCommand = quote_literal_cstr(command);
	int shardIndex = ShardIndex(shardInterval);

	Oid parentSchemaId = InvalidOid;
	char *parentSchemaName = NULL;
	char *escapedParentSchemaName = NULL;
	uint64 parentShardId = INVALID_SHARD_ID;

	StringInfo attachPartitionCommand = makeStringInfo();

	Oid parentRelationId = PartitionParentOid(shardInterval->relationId);
	if (parentRelationId == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot attach partition"),
						errdetail("Referenced relation cannot be found.")));
	}

	parentSchemaId = get_rel_namespace(parentRelationId);
	parentSchemaName = get_namespace_name(parentSchemaId);
	escapedParentSchemaName = quote_literal_cstr(parentSchemaName);
	parentShardId = ColocatedShardIdInRelation(parentRelationId, shardIndex);

	appendStringInfo(attachPartitionCommand,
					 WORKER_APPLY_INTER_SHARD_DDL_COMMAND, parentShardId,
					 escapedParentSchemaName, shardInterval->shardId,
					 escapedSchemaName, escapedCommand);

	return attachPartitionCommand->data;
}


/*
 * GenerateAlterTableAttachPartitionCommand returns the necessary command to
 * attach the given partition to its parent.
 */
char *
GenerateAlterTableAttachPartitionCommand(Oid partitionTableId)
{
	StringInfo createPartitionCommand = makeStringInfo();
	char *partitionBoundCString = NULL;

	Oid parentId = InvalidOid;
	char *tableQualifiedName = NULL;
	char *parentTableQualifiedName = NULL;

	if (!PartitionTable(partitionTableId))
	{
		char *relationName = get_rel_name(partitionTableId);

		ereport(ERROR, (errmsg("\"%s\" is not a partition", relationName)));
	}

	parentId = get_partition_parent(partitionTableId);
	tableQualifiedName = generate_qualified_relation_name(partitionTableId);
	parentTableQualifiedName = generate_qualified_relation_name(parentId);

	partitionBoundCString = PartitionBound(partitionTableId);

	appendStringInfo(createPartitionCommand, "ALTER TABLE %s ATTACH PARTITION %s %s;",
					 parentTableQualifiedName, tableQualifiedName,
					 partitionBoundCString);

	return createPartitionCommand->data;
}


/*
 * This function heaviliy inspired from RelationBuildPartitionDesc()
 * which is avaliable in src/backend/catalog/partition.c.
 *
 * The function simply reads the pg_class and gets the partition bound.
 * Later, converts it to text format and returns.
 */
static char *
PartitionBound(Oid partitionId)
{
	char *partitionBoundString = NULL;
	HeapTuple tuple = NULL;
	Datum datum = 0;
	bool isnull = false;
	Datum partitionBoundDatum = 0;

	tuple = SearchSysCache1(RELOID, partitionId);
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "cache lookup failed for relation %u", partitionId);
	}

	/*
	 * It is possible that the pg_class tuple of a partition has not been
	 * updated yet to set its relpartbound field.  The only case where
	 * this happens is when we open the parent relation to check using its
	 * partition descriptor that a new partition's bound does not overlap
	 * some existing partition.
	 */
	if (!((Form_pg_class) GETSTRUCT(tuple))->relispartition)
	{
		ReleaseSysCache(tuple);
		return "";
	}

	datum = SysCacheGetAttr(RELOID, tuple,
							Anum_pg_class_relpartbound,
							&isnull);
	Assert(!isnull);

	partitionBoundDatum =
		DirectFunctionCall2(pg_get_expr, datum, ObjectIdGetDatum(partitionId));

	partitionBoundString = TextDatumGetCString(partitionBoundDatum);

	ReleaseSysCache(tuple);

	return partitionBoundString;
}
