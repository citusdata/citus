/*-------------------------------------------------------------------------
 *
 * index.c
 *    Commands for creating and altering indices on distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/reloptions.h"
#include "distributed/pg_version_constants.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_opfamily.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_planner.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/namespace_utils.h"
#include "distributed/resource_lock.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/relation_utils.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parse_oper.h"
#include "parser/parse_utilcmd.h"
#include "partitioning/partdesc.h"
#include "rewrite/rewriteManip.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/syscache.h"


/* Local functions forward declarations for helper functions */
static void ErrorIfCreateIndexHasTooManyColumns(IndexStmt *createIndexStatement);
static int GetNumberOfIndexParameters(IndexStmt *createIndexStatement);
static bool IndexAlreadyExists(IndexStmt *createIndexStatement);
static Oid CreateIndexStmtGetIndexId(IndexStmt *createIndexStatement);
static Oid CreateIndexStmtGetSchemaId(IndexStmt *createIndexStatement);
static void SwitchToSequentialAndLocalExecutionIfIndexNameTooLong(
	IndexStmt *createIndexStatement);
static char * GenerateLongestShardPartitionIndexName(IndexStmt *createIndexStatement);
static char * GenerateDefaultIndexName(IndexStmt *createIndexStatement);
static List * GenerateIndexParameters(IndexStmt *createIndexStatement);
static DDLJob * GenerateCreateIndexDDLJob(IndexStmt *createIndexStatement,
										  const char *createIndexCommand);
static Oid CreateIndexStmtGetRelationId(IndexStmt *createIndexStatement);
static List * CreateIndexTaskList(IndexStmt *indexStmt);
static List * CreateReindexTaskList(Oid relationId, ReindexStmt *reindexStmt);
static void RangeVarCallbackForDropIndex(const RangeVar *rel, Oid relOid, Oid oldRelOid,
										 void *arg);
static void RangeVarCallbackForReindexIndex(const RangeVar *rel, Oid relOid, Oid
											oldRelOid,
											void *arg);
static void ErrorIfUnsupportedIndexStmt(IndexStmt *createIndexStatement);
static List * DropIndexTaskList(Oid relationId, Oid indexId, DropStmt *dropStmt);
static void CreatePartitionIndexes(IndexStmt *stmt, Oid relationId, LOCKMODE lockMode);
static void ComputeIndexAttrs(IndexInfo *indexInfo,
							  Oid *typeOidP,
							  Oid *collationOidP,
							  Oid *classOidP,
							  int16 *colOptionP,
							  List *attList, /* list of IndexElem's */
							  List *exclusionOpNames,
							  Oid relId,
							  const char *accessMethodName,
							  Oid accessMethodId,
							  bool amcanorder,
							  bool isconstraint);


/*
 * This struct defines the state for the callback for drop statements.
 * It is copied as it is from commands/tablecmds.c in Postgres source.
 */
struct DropRelationCallbackState
{
	char relkind;
	Oid heapOid;
	bool concurrent;
};

/*
 * This struct defines the state for the callback for reindex statements.
 * It is copied as it is from commands/indexcmds.c in Postgres source.
 */
struct ReindexIndexCallbackState
{
	bool concurrent;
	Oid locked_table_oid;
};


/*
 * IsIndexRenameStmt returns whether the passed-in RenameStmt is the following
 * form:
 *
 *   - ALTER INDEX RENAME
 */
bool
IsIndexRenameStmt(RenameStmt *renameStmt)
{
	bool isIndexRenameStmt = false;

	if (renameStmt->renameType == OBJECT_INDEX)
	{
		isIndexRenameStmt = true;
	}

	return isIndexRenameStmt;
}


/*
 * PreprocessIndexStmt determines whether a given CREATE INDEX statement involves
 * a distributed table. If so (and if the statement does not use unsupported
 * options), it modifies the input statement to ensure proper execution against
 * the coordinator node table and creates a DDLJob to encapsulate information needed
 * during the worker node portion of DDL execution before returning that DDLJob
 * in a List. If no distributed table is involved, this function returns NIL.
 */
List *
PreprocessIndexStmt(Node *node, const char *createIndexCommand,
					ProcessUtilityContext processUtilityContext)
{
	IndexStmt *createIndexStatement = castNode(IndexStmt, node);

	RangeVar *relationRangeVar = createIndexStatement->relation;
	if (relationRangeVar == NULL)
	{
		/* let's be on the safe side */
		return NIL;
	}

	/*
	 * We first check whether a distributed relation is affected. For that,
	 * we need to open the relation. To prevent race conditions with later
	 * lookups, lock the table.
	 *
	 * XXX: Consider using RangeVarGetRelidExtended with a permission
	 * checking callback. Right now we'll acquire the lock before having
	 * checked permissions, and will only fail when executing the actual
	 * index statements.
	 */
	LOCKMODE lockMode = GetCreateIndexRelationLockMode(createIndexStatement);
	Relation relation = table_openrv(relationRangeVar, lockMode);

	/*
	 * Before we do any further processing, fix the schema name to make sure
	 * that a (distributed) table with the same name does not appear on the
	 * search_path in front of the current schema. We do this even if the
	 * table is not distributed, since a distributed table may appear on the
	 * search_path by the time postgres starts processing this command.
	 */
	if (relationRangeVar->schemaname == NULL)
	{
		/* ensure we copy string into proper context */
		MemoryContext relationContext = GetMemoryChunkContext(relationRangeVar);
		char *namespaceName = RelationGetNamespaceName(relation);
		relationRangeVar->schemaname = MemoryContextStrdup(relationContext,
														   namespaceName);
	}

	table_close(relation, NoLock);

	Oid relationId = CreateIndexStmtGetRelationId(createIndexStatement);
	if (!IsCitusTable(relationId))
	{
		return NIL;
	}

	if (createIndexStatement->idxname == NULL)
	{
		/*
		 * Postgres does not support indexes with over INDEX_MAX_KEYS columns
		 * and we should not attempt to generate an index name for such cases.
		 */
		ErrorIfCreateIndexHasTooManyColumns(createIndexStatement);

		/* ensure we copy string into proper context */
		MemoryContext relationContext = GetMemoryChunkContext(relationRangeVar);
		char *defaultIndexName = GenerateDefaultIndexName(createIndexStatement);
		createIndexStatement->idxname = MemoryContextStrdup(relationContext,
															defaultIndexName);
	}

	if (IndexAlreadyExists(createIndexStatement))
	{
		/*
		 * Let standard_processUtility to error out or skip if command has
		 * IF NOT EXISTS.
		 */
		return NIL;
	}

	ErrorIfUnsupportedIndexStmt(createIndexStatement);

	/*
	 * Citus has the logic to truncate the long shard names to prevent
	 * various issues, including self-deadlocks. However, for partitioned
	 * tables, when index is created on the parent table, the index names
	 * on the partitions are auto-generated by Postgres. We use the same
	 * Postgres function to generate the index names on the shards of the
	 * partitions. If the length exceeds the limit, we switch to sequential
	 * execution mode.
	 *
	 * The root cause of the problem is that postgres truncates the
	 * table/index names if they are longer than "NAMEDATALEN - 1".
	 * From Citus' perspective, running commands in parallel on the
	 * shards could mean these table/index names are truncated to be
	 * the same, and thus forming a self-deadlock as these tables/
	 * indexes are inserted into postgres' metadata tables, like pg_class.
	 */
	SwitchToSequentialAndLocalExecutionIfIndexNameTooLong(createIndexStatement);

	/* if this table is partitioned table, and we're not using ONLY, create indexes on partitions */
	if (PartitionedTable(relationId) && createIndexStatement->relation->inh &&
		list_length(PartitionList(relationId)) > 0)
	{
		createIndexStatement = transformIndexStmt(relationId, createIndexStatement,
												  createIndexCommand);
		CreatePartitionIndexes(createIndexStatement, relationId, lockMode);
	}

	DDLJob *ddlJob = GenerateCreateIndexDDLJob(createIndexStatement, createIndexCommand);
	return list_make1(ddlJob);
}


/*
 * CreatePartitionIndexes
 * Most of the code borrowed from Postgres
 */
static void
CreatePartitionIndexes(IndexStmt *stmt, Oid relationId, LOCKMODE lockMode)
{
	Relation rel = table_open(relationId, lockMode);

	/*
	 * look up the access method, verify it can handle the requested features
	 */
	char *accessMethodName = stmt->accessMethod;
	HeapTuple tuple = SearchSysCache1(AMNAME, PointerGetDatum(accessMethodName));
	if (!HeapTupleIsValid(tuple))
	{
		/*
		 * Hack to provide more-or-less-transparent updating of old RTREE
		 * indexes to GiST: if RTREE is requested and not found, use GIST.
		 */
		if (strcmp(accessMethodName, "rtree") == 0)
		{
			ereport(NOTICE,
					(errmsg(
						 "substituting access method \"gist\" for obsolete method \"rtree\"")));
			accessMethodName = "gist";
			tuple = SearchSysCache1(AMNAME, PointerGetDatum(accessMethodName));
		}

		if (!HeapTupleIsValid(tuple))
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("access method \"%s\" does not exist",
							accessMethodName)));
		}
	}
	Form_pg_am accessMethodForm = (Form_pg_am) GETSTRUCT(tuple);
	Oid accessMethodId = accessMethodForm->oid;
	IndexAmRoutine *amRoutine = GetIndexAmRoutine(accessMethodForm->amhandler);
	bool amcanorder = amRoutine->amcanorder;
	ReleaseSysCache(tuple);

	/*
	 * Force non-concurrent build on temporary relations, even if CONCURRENTLY
	 * was requested.  Other backends can't access a temporary relation, so
	 * there's no harm in grabbing a stronger lock, and a non-concurrent DROP
	 * is more efficient.  Do this before any use of the concurrent option is
	 * done.
	 */
	bool concurrent = false;
	if (stmt->concurrent && get_rel_persistence(relationId) != RELPERSISTENCE_TEMP)
	{
		concurrent = true;
	}

	/*
	 * count key attributes in index
	 */
	int numberOfKeyAttributes = list_length(stmt->indexParams);

	/*
	 * Calculate the new list of index columns including both key columns and
	 * INCLUDE columns.  Later we can determine which of these are key
	 * columns, and which are just part of the INCLUDE list by checking the
	 * list position.  A list item in a position less than ii_NumIndexKeyAttrs
	 * is part of the key columns, and anything equal to and over is part of
	 * the INCLUDE columns.
	 */
	List *allIndexParams = list_concat_copy(stmt->indexParams,
											stmt->indexIncludingParams);
	int numberOfAttributes = list_length(allIndexParams);

	IndexInfo *indexInfo = makeIndexInfo(numberOfAttributes,
										 numberOfKeyAttributes,
										 accessMethodId,
										 NIL, /* expressions, NIL for now */
										 make_ands_implicit((Expr *) stmt->whereClause),
										 stmt->unique,
										 !concurrent,
										 concurrent);

	Oid *typeObjectId = (Oid *) palloc(numberOfAttributes * sizeof(Oid));
	Oid *collationObjectId = (Oid *) palloc(numberOfAttributes * sizeof(Oid));
	Oid *classObjectId = (Oid *) palloc(numberOfAttributes * sizeof(Oid));
	int16 *coloptions = (int16 *) palloc(numberOfAttributes * sizeof(int16));

	/* this function is non-export in PG, how to handle this? */

	ComputeIndexAttrs(indexInfo,
					  typeObjectId, collationObjectId, classObjectId,
					  coloptions, allIndexParams,
					  stmt->excludeOpNames, relationId,
					  accessMethodName, accessMethodId,
					  amcanorder, stmt->isconstraint);

	PartitionDesc partdesc;
	partdesc = RelationGetPartitionDesc(rel, true);
	if (partdesc->nparts > 0)
	{
		int nparts = partdesc->nparts;
		Oid *part_oids = palloc(sizeof(Oid) * nparts);
		TupleDesc parentDesc;
		Oid *opfamOids;

		memcpy(part_oids, partdesc->oids, sizeof(Oid) * nparts);

		parentDesc = RelationGetDescr(rel);
		opfamOids = palloc(sizeof(Oid) * numberOfKeyAttributes);
		for (int i = 0; i < numberOfKeyAttributes; i++)
		{
			opfamOids[i] = get_opclass_family(classObjectId[i]);
		}

		/*
		 * For each partition, scan all existing indexes; if one matches
		 * our index definition and is not already attached to some other
		 * parent index, we don't need to build a new one.
		 *
		 * If none matches, build a new index by calling ourselves
		 * recursively with the same options (except for the index name).
		 */
		for (int i = 0; i < nparts; i++)
		{
			Oid childRelid = part_oids[i];
			Relation childrel;
			List *childidxs;
			ListCell *cell;
			AttrMap *attmap;
			bool found = false;

			childrel = table_open(childRelid, lockMode);

			/*
			 * Don't try to create indexes on foreign tables, though. Skip
			 * those if a regular index, or fail if trying to create a
			 * constraint index.
			 */
			if (childrel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			{
				if (stmt->unique || stmt->primary)
				{
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg(
								 "cannot create unique index on partitioned table \"%s\"",
								 RelationGetRelationName(rel)),
							 errdetail(
								 "Table \"%s\" contains partitions that are foreign tables.",
								 RelationGetRelationName(rel))));
				}

				table_close(childrel, lockMode);
				continue;
			}

			childidxs = RelationGetIndexList(childrel);
			attmap =
				build_attrmap_by_name(RelationGetDescr(childrel),
									  parentDesc);

			foreach(cell, childidxs)     /*ONLY is not used here */
			{
				Oid cldidxid = lfirst_oid(cell);
				Relation cldidx;
				IndexInfo *cldIdxInfo;

				/* this index is already partition of another one */
				if (has_superclass(cldidxid))
				{
					continue;
				}

				cldidx = index_open(cldidxid, lockMode);
				cldIdxInfo = BuildIndexInfo(cldidx);
				if (CompareIndexInfo(cldIdxInfo, indexInfo,
									 cldidx->rd_indcollation,
									 collationObjectId,
									 cldidx->rd_opfamily,
									 opfamOids,
									 attmap))
				{
					found = true;

					/* keep lock till commit */
					index_close(cldidx, NoLock);
					break;
				}

				index_close(cldidx, lockMode);
			}

			list_free(childidxs);
			table_close(childrel, NoLock);

			/*
			 * If no matching index was found, create our own.
			 */
			if (!found)
			{
				IndexStmt *childStmt = copyObject(stmt);
				bool found_whole_row;
				ListCell *lc;

				/*
				 * We can't use the same index name for the child index,
				 * so clear idxname to let the recursive invocation choose
				 * a new name.  Likewise, the existing target relation
				 * field is wrong, and if indexOid or oldNode are set,
				 * they mustn't be applied to the child either.
				 */
				childStmt->idxname = NULL;
				childStmt->relation = NULL;
				childStmt->indexOid = InvalidOid;
				childStmt->oldNode = InvalidOid;
				childStmt->oldCreateSubid = InvalidSubTransactionId;
				childStmt->oldFirstRelfilenodeSubid = InvalidSubTransactionId;

				/*
				 * Adjust any Vars (both in expressions and in the index's
				 * WHERE clause) to match the partition's column numbering
				 * in case it's different from the parent's.
				 */
				foreach(lc, childStmt->indexParams)
				{
					IndexElem *ielem = lfirst(lc);

					/*
					 * If the index parameter is an expression, we must
					 * translate it to contain child Vars.
					 */
					if (ielem->expr)
					{
						ielem->expr =
							map_variable_attnos((Node *) ielem->expr,
												1, 0, attmap,
												InvalidOid,
												&found_whole_row);
						if (found_whole_row)
						{
							elog(ERROR, "cannot convert whole-row table reference");
						}
					}
				}
				childStmt->whereClause =
					map_variable_attnos(stmt->whereClause, 1, 0,
										attmap,
										InvalidOid, &found_whole_row);
				if (found_whole_row)
				{
					elog(ERROR, "cannot convert whole-row table reference");
				}

				char *partitionNamespace = get_namespace_name(get_rel_namespace(
																  childRelid));
				char *partitionName = get_rel_name(childRelid);
				childStmt->relation = makeRangeVar(partitionNamespace, partitionName, -1);
				childStmt->relation->inh = false;

				const char *dummyString = "-";

				/* since the command is an IndexStmt, a dummy command string works fine */
				ProcessUtilityParseTree((Node *) childStmt, dummyString,
										PROCESS_UTILITY_QUERY, NULL, None_Receiver, NULL);
			}
			free_attrmap(attmap);
		}
	}
	table_close(rel, NoLock);
}


/*
 * Compute per-index-column information, including indexed column numbers
 * or index expressions, opclasses and their options. Note, all output vectors
 * should be allocated for all columns, including "including" ones.
 *
 * ALL METHOD BORROWED FROM POSTGRES SINCE IT'S STATIC
 */
static void
ComputeIndexAttrs(IndexInfo *indexInfo,
				  Oid *typeOidP,
				  Oid *collationOidP,
				  Oid *classOidP,
				  int16 *colOptionP,
				  List *attList,    /* list of IndexElem's */
				  List *exclusionOpNames,
				  Oid relId,
				  const char *accessMethodName,
				  Oid accessMethodId,
				  bool amcanorder,
				  bool isconstraint)
{
	ListCell *nextExclOp;
	ListCell *lc;
	int attn;
	int nkeycols = indexInfo->ii_NumIndexKeyAttrs;

	/* Allocate space for exclusion operator info, if needed */
	if (exclusionOpNames)
	{
		Assert(list_length(exclusionOpNames) == nkeycols);
		indexInfo->ii_ExclusionOps = (Oid *) palloc(sizeof(Oid) * nkeycols);
		indexInfo->ii_ExclusionProcs = (Oid *) palloc(sizeof(Oid) * nkeycols);
		indexInfo->ii_ExclusionStrats = (uint16 *) palloc(sizeof(uint16) * nkeycols);
		nextExclOp = list_head(exclusionOpNames);
	}
	else
	{
		nextExclOp = NULL;
	}

	/*
	 * process attributeList
	 */
	attn = 0;
	foreach(lc, attList)
	{
		IndexElem *attribute = (IndexElem *) lfirst(lc);
		Oid atttype;
		Oid attcollation;

		/*
		 * Process the column-or-expression to be indexed.
		 */
		if (attribute->name != NULL)
		{
			/* Simple index attribute */
			HeapTuple atttuple;
			Form_pg_attribute attform;

			Assert(attribute->expr == NULL);
			atttuple = SearchSysCacheAttName(relId, attribute->name);
			if (!HeapTupleIsValid(atttuple))
			{
				/* difference in error message spellings is historical */
				if (isconstraint)
				{
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" named in key does not exist",
									attribute->name)));
				}
				else
				{
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" does not exist",
									attribute->name)));
				}
			}
			attform = (Form_pg_attribute) GETSTRUCT(atttuple);
			indexInfo->ii_IndexAttrNumbers[attn] = attform->attnum;
			atttype = attform->atttypid;
			attcollation = attform->attcollation;
			ReleaseSysCache(atttuple);
		}
		else
		{
			/* Index expression */
			Node *expr = attribute->expr;

			Assert(expr != NULL);

			if (attn >= nkeycols)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("expressions are not supported in included columns")));
			}
			atttype = exprType(expr);
			attcollation = exprCollation(expr);

			/*
			 * Strip any top-level COLLATE clause.  This ensures that we treat
			 * "x COLLATE y" and "(x COLLATE y)" alike.
			 */
			while (IsA(expr, CollateExpr))
			{
				expr = (Node *) ((CollateExpr *) expr)->arg;
			}

			if (IsA(expr, Var) &&
				((Var *) expr)->varattno != InvalidAttrNumber)
			{
				/*
				 * User wrote "(column)" or "(column COLLATE something)".
				 * Treat it like simple attribute anyway.
				 */
				indexInfo->ii_IndexAttrNumbers[attn] = ((Var *) expr)->varattno;
			}
			else
			{
				indexInfo->ii_IndexAttrNumbers[attn] = 0;   /* marks expression */
				indexInfo->ii_Expressions = lappend(indexInfo->ii_Expressions,
													expr);

				/*
				 * transformExpr() should have already rejected subqueries,
				 * aggregates, and window functions, based on the EXPR_KIND_
				 * for an index expression.
				 */

				/*
				 * An expression using mutable functions is probably wrong,
				 * since if you aren't going to get the same result for the
				 * same data every time, it's not clear what the index entries
				 * mean at all.
				 */
				if (contain_mutable_functions((Node *) expression_planner((Expr *) expr)))
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							 errmsg(
								 "functions in index expression must be marked IMMUTABLE")));
				}
			}
		}

		typeOidP[attn] = atttype;

		/*
		 * Included columns have no collation, no opclass and no ordering
		 * options.
		 */
		if (attn >= nkeycols)
		{
			if (attribute->collation)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("including column does not support a collation")));
			}
			if (attribute->opclass)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("including column does not support an operator class")));
			}
			if (attribute->ordering != SORTBY_DEFAULT)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("including column does not support ASC/DESC options")));
			}
			if (attribute->nulls_ordering != SORTBY_NULLS_DEFAULT)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg(
							 "including column does not support NULLS FIRST/LAST options")));
			}

			classOidP[attn] = InvalidOid;
			colOptionP[attn] = 0;
			collationOidP[attn] = InvalidOid;
			attn++;

			continue;
		}

		/*
		 * Apply collation override if any
		 */
		if (attribute->collation)
		{
			attcollation = get_collation_oid(attribute->collation, false);
		}

		/*
		 * Check we have a collation iff it's a collatable type.  The only
		 * expected failures here are (1) COLLATE applied to a noncollatable
		 * type, or (2) index expression had an unresolved collation.  But we
		 * might as well code this to be a complete consistency check.
		 */
		if (type_is_collatable(atttype))
		{
			if (!OidIsValid(attcollation))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INDETERMINATE_COLLATION),
						 errmsg(
							 "could not determine which collation to use for index expression"),
						 errhint(
							 "Use the COLLATE clause to set the collation explicitly.")));
			}
		}
		else
		{
			if (OidIsValid(attcollation))
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("collations are not supported by type %s",
								format_type_be(atttype))));
			}
		}

		collationOidP[attn] = attcollation;

		/*
		 * Identify the opclass to use.
		 */
		classOidP[attn] = ResolveOpClass(attribute->opclass,
										 atttype,
										 accessMethodName,
										 accessMethodId);

		/*
		 * Identify the exclusion operator, if any.
		 */
		if (nextExclOp)
		{
			List *opname = (List *) lfirst(nextExclOp);
			Oid opid;
			Oid opfamily;
			int strat;

			/*
			 * Find the operator --- it must accept the column datatype
			 * without runtime coercion (but binary compatibility is OK)
			 */
			opid = compatible_oper_opid(opname, atttype, atttype, false);

			/*
			 * Only allow commutative operators to be used in exclusion
			 * constraints. If X conflicts with Y, but Y does not conflict
			 * with X, bad things will happen.
			 */
			if (get_commutator(opid) != opid)
			{
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("operator %s is not commutative",
								format_operator(opid)),
						 errdetail(
							 "Only commutative operators can be used in exclusion constraints.")));
			}

			/*
			 * Operator must be a member of the right opfamily, too
			 */
			opfamily = get_opclass_family(classOidP[attn]);
			strat = get_op_opfamily_strategy(opid, opfamily);
			if (strat == 0)
			{
				HeapTuple opftuple;
				Form_pg_opfamily opfform;

				/*
				 * attribute->opclass might not explicitly name the opfamily,
				 * so fetch the name of the selected opfamily for use in the
				 * error message.
				 */
				opftuple = SearchSysCache1(OPFAMILYOID,
										   ObjectIdGetDatum(opfamily));
				if (!HeapTupleIsValid(opftuple))
				{
					elog(ERROR, "cache lookup failed for opfamily %u",
						 opfamily);
				}
				opfform = (Form_pg_opfamily) GETSTRUCT(opftuple);

				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("operator %s is not a member of operator family \"%s\"",
								format_operator(opid),
								NameStr(opfform->opfname)),
						 errdetail(
							 "The exclusion operator must be related to the index operator class for the constraint.")));
			}

			indexInfo->ii_ExclusionOps[attn] = opid;
			indexInfo->ii_ExclusionProcs[attn] = get_opcode(opid);
			indexInfo->ii_ExclusionStrats[attn] = strat;
			nextExclOp = lnext(exclusionOpNames, nextExclOp);
		}

		/*
		 * Set up the per-column options (indoption field).  For now, this is
		 * zero for any un-ordered index, while ordered indexes have DESC and
		 * NULLS FIRST/LAST options.
		 */
		colOptionP[attn] = 0;
		if (amcanorder)
		{
			/* default ordering is ASC */
			if (attribute->ordering == SORTBY_DESC)
			{
				colOptionP[attn] |= INDOPTION_DESC;
			}

			/* default null ordering is LAST for ASC, FIRST for DESC */
			if (attribute->nulls_ordering == SORTBY_NULLS_DEFAULT)
			{
				if (attribute->ordering == SORTBY_DESC)
				{
					colOptionP[attn] |= INDOPTION_NULLS_FIRST;
				}
			}
			else if (attribute->nulls_ordering == SORTBY_NULLS_FIRST)
			{
				colOptionP[attn] |= INDOPTION_NULLS_FIRST;
			}
		}
		else
		{
			/* index AM does not support ordering */
			if (attribute->ordering != SORTBY_DEFAULT)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("access method \"%s\" does not support ASC/DESC options",
								accessMethodName)));
			}
			if (attribute->nulls_ordering != SORTBY_NULLS_DEFAULT)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg(
							 "access method \"%s\" does not support NULLS FIRST/LAST options",
							 accessMethodName)));
			}
		}

		/* Set up the per-column opclass options (attoptions field). */
		if (attribute->opclassopts)
		{
			Assert(attn < nkeycols);

			if (!indexInfo->ii_OpclassOptions)
			{
				indexInfo->ii_OpclassOptions =
					palloc0(sizeof(Datum) * indexInfo->ii_NumIndexAttrs);
			}

			indexInfo->ii_OpclassOptions[attn] =
				transformRelOptions((Datum) 0, attribute->opclassopts,
									NULL, NULL, false, false);
		}

		attn++;
	}
}


/*
 * ErrorIfCreateIndexHasTooManyColumns errors out if given CREATE INDEX command
 * would use more than INDEX_MAX_KEYS columns.
 */
static void
ErrorIfCreateIndexHasTooManyColumns(IndexStmt *createIndexStatement)
{
	int numberOfIndexParameters = GetNumberOfIndexParameters(createIndexStatement);
	if (numberOfIndexParameters <= INDEX_MAX_KEYS)
	{
		return;
	}

	ereport(ERROR, (errcode(ERRCODE_TOO_MANY_COLUMNS),
					errmsg("cannot use more than %d columns in an index",
						   INDEX_MAX_KEYS)));
}


/*
 * GetNumberOfIndexParameters returns number of parameters to be used when
 * creating the index to be defined by given CREATE INDEX command.
 */
static int
GetNumberOfIndexParameters(IndexStmt *createIndexStatement)
{
	List *indexParams = createIndexStatement->indexParams;
	List *indexIncludingParams = createIndexStatement->indexIncludingParams;
	return list_length(indexParams) + list_length(indexIncludingParams);
}


/*
 * IndexAlreadyExists returns true if index to be created by given CREATE INDEX
 * command already exists.
 */
static bool
IndexAlreadyExists(IndexStmt *createIndexStatement)
{
	Oid indexRelationId = CreateIndexStmtGetIndexId(createIndexStatement);
	return OidIsValid(indexRelationId);
}


/*
 * CreateIndexStmtGetIndexId returns OID of the index that given CREATE INDEX
 * command attempts to create if it's already created before. Otherwise, returns
 * InvalidOid.
 */
static Oid
CreateIndexStmtGetIndexId(IndexStmt *createIndexStatement)
{
	char *indexName = createIndexStatement->idxname;
	Oid namespaceId = CreateIndexStmtGetSchemaId(createIndexStatement);
	Oid indexRelationId = get_relname_relid(indexName, namespaceId);
	return indexRelationId;
}


/*
 * CreateIndexStmtGetSchemaId returns schemaId of the schema that given
 * CREATE INDEX command operates on.
 */
static Oid
CreateIndexStmtGetSchemaId(IndexStmt *createIndexStatement)
{
	RangeVar *relationRangeVar = createIndexStatement->relation;
	char *schemaName = relationRangeVar->schemaname;
	bool missingOk = false;
	Oid namespaceId = get_namespace_oid(schemaName, missingOk);
	return namespaceId;
}


/*
 * ExecuteFunctionOnEachTableIndex executes the given pgIndexProcessor function on each
 * index of the given relation.
 * It returns a list that is filled by the pgIndexProcessor.
 */
List *
ExecuteFunctionOnEachTableIndex(Oid relationId, PGIndexProcessor pgIndexProcessor,
								int indexFlags)
{
	List *result = NIL;

	Relation relation = RelationIdGetRelation(relationId);
	List *indexIdList = RelationGetIndexList(relation);
	Oid indexId = InvalidOid;
	foreach_oid(indexId, indexIdList)
	{
		HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexId));
		if (!HeapTupleIsValid(indexTuple))
		{
			ereport(ERROR, (errmsg("cache lookup failed for index with oid %u",
								   indexId)));
		}

		Form_pg_index indexForm = (Form_pg_index) GETSTRUCT(indexTuple);
		pgIndexProcessor(indexForm, &result, indexFlags);
		ReleaseSysCache(indexTuple);
	}

	RelationClose(relation);
	return result;
}


/*
 * SwitchToSequentialOrLocalExecutionIfIndexNameTooLong generates the longest index name
 * on the shards of the partitions, and if exceeds the limit switches to sequential and
 * local execution to prevent self-deadlocks.
 */
static void
SwitchToSequentialAndLocalExecutionIfIndexNameTooLong(IndexStmt *createIndexStatement)
{
	Oid relationId = CreateIndexStmtGetRelationId(createIndexStatement);
	if (!PartitionedTable(relationId))
	{
		/* Citus already handles long names for regular tables */
		return;
	}

	if (ShardIntervalCount(relationId) == 0)
	{
		/*
		 * Relation has no shards, so we cannot run into "long shard index
		 * name" issue.
		 */
		return;
	}

	char *indexName = GenerateLongestShardPartitionIndexName(createIndexStatement);
	if (indexName &&
		strnlen(indexName, NAMEDATALEN) >= NAMEDATALEN - 1)
	{
		if (ParallelQueryExecutedInTransaction())
		{
			/*
			 * If there has already been a parallel query executed, the sequential mode
			 * would still use the already opened parallel connections to the workers,
			 * thus contradicting our purpose of using sequential mode.
			 */
			ereport(ERROR, (errmsg(
								"The index name (%s) on a shard is too long and could lead "
								"to deadlocks when executed in a transaction "
								"block after a parallel query", indexName),
							errhint("Try re-running the transaction with "
									"\"SET LOCAL citus.multi_shard_modify_mode TO "
									"\'sequential\';\"")));
		}
		else
		{
			elog(DEBUG1, "the index name on the shards of the partition "
						 "is too long, switching to sequential and local execution "
						 "mode to prevent self deadlocks: %s", indexName);

			SetLocalMultiShardModifyModeToSequential();
			SetLocalExecutionStatus(LOCAL_EXECUTION_REQUIRED);
		}
	}
}


/*
 * GenerateLongestShardPartitionIndexName emulates Postgres index name
 * generation for partitions on the shards. It returns the longest
 * possible index name.
 */
static char *
GenerateLongestShardPartitionIndexName(IndexStmt *createIndexStatement)
{
	Oid relationId = CreateIndexStmtGetRelationId(createIndexStatement);
	Oid longestNamePartitionId = PartitionWithLongestNameRelationId(relationId);
	if (!OidIsValid(longestNamePartitionId))
	{
		/* no partitions have been created yet */
		return NULL;
	}

	char *longestPartitionShardName = get_rel_name(longestNamePartitionId);
	ShardInterval *shardInterval = LoadShardIntervalWithLongestShardName(
		longestNamePartitionId);
	AppendShardIdToName(&longestPartitionShardName, shardInterval->shardId);

	IndexStmt *createLongestShardIndexStmt = copyObject(createIndexStatement);
	createLongestShardIndexStmt->relation->relname = longestPartitionShardName;

	char *choosenIndexName = GenerateDefaultIndexName(createLongestShardIndexStmt);
	return choosenIndexName;
}


/*
 * GenerateDefaultIndexName is a wrapper around postgres function ChooseIndexName
 * that generates default index name for the index to be created by given CREATE
 * INDEX statement as postgres would do.
 *
 * (See DefineIndex at postgres/src/backend/commands/indexcmds.c)
 */
static char *
GenerateDefaultIndexName(IndexStmt *createIndexStatement)
{
	char *relationName = createIndexStatement->relation->relname;
	Oid namespaceId = CreateIndexStmtGetSchemaId(createIndexStatement);
	List *indexParams = GenerateIndexParameters(createIndexStatement);
	List *indexColNames = ChooseIndexColumnNames(indexParams);
	char *indexName = ChooseIndexName(relationName, namespaceId, indexColNames,
									  createIndexStatement->excludeOpNames,
									  createIndexStatement->primary,
									  createIndexStatement->isconstraint);

	return indexName;
}


/*
 * GenerateIndexParameters is a helper function that creates a list of parameters
 * required to assign a default index name for the index to be created by given
 * CREATE INDEX command.
 */
static List *
GenerateIndexParameters(IndexStmt *createIndexStatement)
{
	List *indexParams = createIndexStatement->indexParams;
	List *indexIncludingParams = createIndexStatement->indexIncludingParams;
	List *allIndexParams = list_concat(list_copy(indexParams),
									   list_copy(indexIncludingParams));
	return allIndexParams;
}


/*
 * GenerateCreateIndexDDLJob returns DDLJob for given CREATE INDEX command.
 */
static DDLJob *
GenerateCreateIndexDDLJob(IndexStmt *createIndexStatement, const char *createIndexCommand)
{
	DDLJob *ddlJob = palloc0(sizeof(DDLJob));

	ddlJob->targetRelationId = CreateIndexStmtGetRelationId(createIndexStatement);
	ddlJob->concurrentIndexCmd = createIndexStatement->concurrent;
	ddlJob->startNewTransaction = createIndexStatement->concurrent;
	ddlJob->commandString = createIndexCommand;
	ddlJob->taskList = CreateIndexTaskList(createIndexStatement);

	return ddlJob;
}


/*
 * CreateIndexStmtGetRelationId returns relationId for relation that given
 * CREATE INDEX command operates on.
 */
static Oid
CreateIndexStmtGetRelationId(IndexStmt *createIndexStatement)
{
	RangeVar *relationRangeVar = createIndexStatement->relation;
	LOCKMODE lockMode = GetCreateIndexRelationLockMode(createIndexStatement);
	bool missingOk = false;
	Oid relationId = RangeVarGetRelid(relationRangeVar, lockMode, missingOk);
	return relationId;
}


/*
 * GetCreateIndexRelationLockMode returns required lock mode to open the
 * relation that given CREATE INDEX command operates on.
 */
LOCKMODE
GetCreateIndexRelationLockMode(IndexStmt *createIndexStatement)
{
	if (createIndexStatement->concurrent)
	{
		return ShareUpdateExclusiveLock;
	}
	else
	{
		return ShareLock;
	}
}


/*
 * PreprocessReindexStmt determines whether a given REINDEX statement involves
 * a distributed table. If so (and if the statement does not use unsupported
 * options), it modifies the input statement to ensure proper execution against
 * the coordinator node table and creates a DDLJob to encapsulate information needed
 * during the worker node portion of DDL execution before returning that DDLJob
 * in a List. If no distributed table is involved, this function returns NIL.
 */
List *
PreprocessReindexStmt(Node *node, const char *reindexCommand,
					  ProcessUtilityContext processUtilityContext)
{
	ReindexStmt *reindexStatement = castNode(ReindexStmt, node);
	List *ddlJobs = NIL;

	/*
	 * We first check whether a distributed relation is affected. For that, we need to
	 * open the relation. To prevent race conditions with later lookups, lock the table,
	 * and modify the rangevar to include the schema.
	 */
	if (reindexStatement->relation != NULL)
	{
		Relation relation = NULL;
		Oid relationId = InvalidOid;
		LOCKMODE lockmode = IsReindexWithParam_compat(reindexStatement, "concurrently") ?
							ShareUpdateExclusiveLock : AccessExclusiveLock;
		MemoryContext relationContext = NULL;

		Assert(reindexStatement->kind == REINDEX_OBJECT_INDEX ||
			   reindexStatement->kind == REINDEX_OBJECT_TABLE);

		if (reindexStatement->kind == REINDEX_OBJECT_INDEX)
		{
			struct ReindexIndexCallbackState state;
			state.concurrent = IsReindexWithParam_compat(reindexStatement,
														 "concurrently");
			state.locked_table_oid = InvalidOid;

			Oid indOid = RangeVarGetRelidExtended(reindexStatement->relation,
												  lockmode, 0,
												  RangeVarCallbackForReindexIndex,
												  &state);
			relation = index_open(indOid, NoLock);
			relationId = IndexGetRelation(indOid, false);
		}
		else
		{
			RangeVarGetRelidExtended(reindexStatement->relation, lockmode, 0,
									 RangeVarCallbackOwnsTable, NULL);

			relation = table_openrv(reindexStatement->relation, NoLock);
			relationId = RelationGetRelid(relation);
		}

		bool isCitusRelation = IsCitusTable(relationId);

		if (reindexStatement->relation->schemaname == NULL)
		{
			/*
			 * Before we do any further processing, fix the schema name to make sure
			 * that a (distributed) table with the same name does not appear on the
			 * search path in front of the current schema. We do this even if the
			 * table is not distributed, since a distributed table may appear on the
			 * search path by the time postgres starts processing this statement.
			 */
			char *namespaceName = get_namespace_name(RelationGetNamespace(relation));

			/* ensure we copy string into proper context */
			relationContext = GetMemoryChunkContext(reindexStatement->relation);
			reindexStatement->relation->schemaname = MemoryContextStrdup(relationContext,
																		 namespaceName);
		}

		if (reindexStatement->kind == REINDEX_OBJECT_INDEX)
		{
			index_close(relation, NoLock);
		}
		else
		{
			table_close(relation, NoLock);
		}

		if (isCitusRelation)
		{
			if (PartitionedTable(relationId))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("REINDEX TABLE queries on distributed partitioned "
									   "tables are not supported")));
			}

			DDLJob *ddlJob = palloc0(sizeof(DDLJob));
			ddlJob->targetRelationId = relationId;
			ddlJob->concurrentIndexCmd = IsReindexWithParam_compat(reindexStatement,
																   "concurrently");
			ddlJob->startNewTransaction = IsReindexWithParam_compat(reindexStatement,
																	"concurrently");
			ddlJob->commandString = reindexCommand;
			ddlJob->taskList = CreateReindexTaskList(relationId, reindexStatement);

			ddlJobs = list_make1(ddlJob);
		}
	}

	return ddlJobs;
}


/*
 * PreprocessDropIndexStmt determines whether a given DROP INDEX statement involves
 * a distributed table. If so (and if the statement does not use unsupported
 * options), it modifies the input statement to ensure proper execution against
 * the coordinator node table and creates a DDLJob to encapsulate information needed
 * during the worker node portion of DDL execution before returning that DDLJob
 * in a List. If no distributed table is involved, this function returns NIL.
 */
List *
PreprocessDropIndexStmt(Node *node, const char *dropIndexCommand,
						ProcessUtilityContext processUtilityContext)
{
	DropStmt *dropIndexStatement = castNode(DropStmt, node);
	List *ddlJobs = NIL;
	Oid distributedIndexId = InvalidOid;
	Oid distributedRelationId = InvalidOid;

	Assert(dropIndexStatement->removeType == OBJECT_INDEX);

	/* check if any of the indexes being dropped belong to a distributed table */
	List *objectNameList = NULL;
	foreach_ptr(objectNameList, dropIndexStatement->objects)
	{
		struct DropRelationCallbackState state;
		uint32 rvrFlags = RVR_MISSING_OK;
		LOCKMODE lockmode = AccessExclusiveLock;

		RangeVar *rangeVar = makeRangeVarFromNameList(objectNameList);

		/*
		 * We don't support concurrently dropping indexes for distributed
		 * tables, but till this point, we don't know if it is a regular or a
		 * distributed table.
		 */
		if (dropIndexStatement->concurrent)
		{
			lockmode = ShareUpdateExclusiveLock;
		}

		/*
		 * The next few statements are based on RemoveRelations() in
		 * commands/tablecmds.c in Postgres source.
		 */
		AcceptInvalidationMessages();

		state.relkind = RELKIND_INDEX;
		state.heapOid = InvalidOid;
		state.concurrent = dropIndexStatement->concurrent;

		Oid indexId = RangeVarGetRelidExtended(rangeVar, lockmode, rvrFlags,
											   RangeVarCallbackForDropIndex,
											   (void *) &state);

		/*
		 * If the index does not exist, we don't do anything here, and allow
		 * postgres to throw appropriate error or notice message later.
		 */
		if (!OidIsValid(indexId))
		{
			continue;
		}

		Oid relationId = IndexGetRelation(indexId, false);
		bool isCitusRelation = IsCitusTable(relationId);
		if (isCitusRelation)
		{
			if (OidIsValid(distributedIndexId))
			{
				/*
				 * We already have a distributed index in the list, and Citus
				 * currently only support dropping a single distributed index.
				 */
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot drop multiple distributed objects in "
									   "a single command"),
								errhint("Try dropping each object in a separate DROP "
										"command.")));
			}

			distributedIndexId = indexId;
			distributedRelationId = relationId;
		}
	}

	if (OidIsValid(distributedIndexId))
	{
		DDLJob *ddlJob = palloc0(sizeof(DDLJob));

		if (AnyForeignKeyDependsOnIndex(distributedIndexId))
		{
			MarkInvalidateForeignKeyGraph();
		}

		ddlJob->targetRelationId = distributedRelationId;
		ddlJob->concurrentIndexCmd = dropIndexStatement->concurrent;

		/*
		 * We do not want DROP INDEX CONCURRENTLY to commit locally before
		 * sending commands, because if there is a failure we would like to
		 * to be able to repeat the DROP INDEX later.
		 */
		ddlJob->startNewTransaction = false;
		ddlJob->commandString = dropIndexCommand;
		ddlJob->taskList = DropIndexTaskList(distributedRelationId, distributedIndexId,
											 dropIndexStatement);

		ddlJobs = list_make1(ddlJob);
	}

	return ddlJobs;
}


/*
 * PostprocessIndexStmt marks new indexes invalid if they were created using the
 * CONCURRENTLY flag. This (non-transactional) change provides the fallback
 * state if an error is raised, otherwise a sub-sequent change to valid will be
 * committed.
 */
List *
PostprocessIndexStmt(Node *node, const char *queryString)
{
	IndexStmt *indexStmt = castNode(IndexStmt, node);

	/* we are only processing CONCURRENT index statements */
	if (!indexStmt->concurrent)
	{
		return NIL;
	}

	/* this logic only applies to the coordinator */
	if (!IsCoordinator())
	{
		return NIL;
	}

	/*
	 * We make sure schema name is not null in the PreprocessIndexStmt
	 */
	Oid schemaId = get_namespace_oid(indexStmt->relation->schemaname, true);
	Oid relationId = get_relname_relid(indexStmt->relation->relname, schemaId);
	if (!IsCitusTable(relationId))
	{
		return NIL;
	}

	/* commit the current transaction and start anew */
	CommitTransactionCommand();
	StartTransactionCommand();

	/* get the affected relation and index */
	Relation relation = table_openrv(indexStmt->relation, ShareUpdateExclusiveLock);
	Oid indexRelationId = get_relname_relid(indexStmt->idxname,
											schemaId);
	Relation indexRelation = index_open(indexRelationId, RowExclusiveLock);

	/* close relations but retain locks */
	table_close(relation, NoLock);
	index_close(indexRelation, NoLock);

	/* mark index as invalid, in-place (cannot be rolled back) */
	index_set_state_flags(indexRelationId, INDEX_DROP_CLEAR_VALID);

	/* re-open a transaction command from here on out */
	CommitTransactionCommand();
	StartTransactionCommand();

	return NIL;
}


/*
 * ErrorIfUnsupportedAlterIndexStmt checks if the corresponding alter index
 * statement is supported for distributed tables and errors out if it is not.
 * Currently, only the following commands are supported.
 *
 * ALTER INDEX SET ()
 * ALTER INDEX RESET ()
 * ALTER INDEX ALTER COLUMN SET STATISTICS
 */
void
ErrorIfUnsupportedAlterIndexStmt(AlterTableStmt *alterTableStatement)
{
	/* error out if any of the subcommands are unsupported */
	List *commandList = alterTableStatement->cmds;
	AlterTableCmd *command = NULL;
	foreach_ptr(command, commandList)
	{
		AlterTableType alterTableType = command->subtype;

		switch (alterTableType)
		{
			case AT_SetRelOptions:  /* SET (...) */
			case AT_ResetRelOptions:    /* RESET (...) */
			case AT_ReplaceRelOptions:  /* replace entire option list */
			case AT_SetStatistics:  /* SET STATISTICS */
			case AT_AttachPartition: /* ATTACH PARTITION */
			{
				/* this command is supported by Citus */
				break;
			}

			/* unsupported create index statements */
			case AT_SetTableSpace:
			default:
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("alter index ... set tablespace ... "
								"is currently unsupported"),
						 errdetail("Only RENAME TO, SET (), RESET (), ATTACH PARTITION "
								   "and SET STATISTICS are supported.")));
				return; /* keep compiler happy */
			}
		}
	}
}


/*
 * CreateIndexTaskList builds a list of tasks to execute a CREATE INDEX command.
 */
static List *
CreateIndexTaskList(IndexStmt *indexStmt)
{
	List *taskList = NIL;
	Oid relationId = CreateIndexStmtGetRelationId(indexStmt);
	List *shardIntervalList = LoadShardIntervalList(relationId);
	StringInfoData ddlString;
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	initStringInfo(&ddlString);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;

		deparse_shard_index_statement(indexStmt, relationId, shardId, &ddlString);

		Task *task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		SetTaskQueryString(task, pstrdup(ddlString.data));
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependentTaskList = NULL;
		task->anchorShardId = shardId;
		task->taskPlacementList = ActiveShardPlacementList(shardId);

		taskList = lappend(taskList, task);

		resetStringInfo(&ddlString);
	}

	return taskList;
}


/*
 * CreateReindexTaskList builds a list of tasks to execute a REINDEX command
 * against a specified distributed table.
 */
static List *
CreateReindexTaskList(Oid relationId, ReindexStmt *reindexStmt)
{
	List *taskList = NIL;
	List *shardIntervalList = LoadShardIntervalList(relationId);
	StringInfoData ddlString;
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	initStringInfo(&ddlString);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;

		deparse_shard_reindex_statement(reindexStmt, relationId, shardId, &ddlString);

		Task *task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		SetTaskQueryString(task, pstrdup(ddlString.data));
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependentTaskList = NULL;
		task->anchorShardId = shardId;
		task->taskPlacementList = ActiveShardPlacementList(shardId);

		taskList = lappend(taskList, task);

		resetStringInfo(&ddlString);
	}

	return taskList;
}


/*
 * Before acquiring a table lock, check whether we have sufficient rights.
 * In the case of DROP INDEX, also try to lock the table before the index.
 *
 * This code is heavily borrowed from RangeVarCallbackForDropRelation() in
 * commands/tablecmds.c in Postgres source. We need this to ensure the right
 * order of locking while dealing with DROP INDEX statements. Because we are
 * exclusively using this callback for INDEX processing, the PARTITION-related
 * logic from PostgreSQL's similar callback has been omitted as unneeded.
 */
static void
RangeVarCallbackForDropIndex(const RangeVar *rel, Oid relOid, Oid oldRelOid, void *arg)
{
	/* *INDENT-OFF* */
	HeapTuple	tuple;
	char		relkind;
	char		expected_relkind;
	LOCKMODE	heap_lockmode;

	struct DropRelationCallbackState *state = (struct DropRelationCallbackState *) arg;
	relkind = state->relkind;
	heap_lockmode = state->concurrent ?
	                ShareUpdateExclusiveLock : AccessExclusiveLock;

	Assert(relkind == RELKIND_INDEX);

	/*
	 * If we previously locked some other index's heap, and the name we're
	 * looking up no longer refers to that relation, release the now-useless
	 * lock.
	 */
	if (relOid != oldRelOid && OidIsValid(state->heapOid))
	{
		UnlockRelationOid(state->heapOid, heap_lockmode);
		state->heapOid = InvalidOid;
	}

	/* Didn't find a relation, so no need for locking or permission checks. */
	if (!OidIsValid(relOid))
		return;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relOid));
	if (!HeapTupleIsValid(tuple))
		return;					/* concurrently dropped, so nothing to do */
	Form_pg_class classform = (Form_pg_class) GETSTRUCT(tuple);

	/*
	 * PG 11 sends relkind as partitioned index for an index
	 * on partitioned table. It is handled the same
	 * as regular index as far as we are concerned here.
	 *
	 * See tablecmds.c:RangeVarCallbackForDropRelation()
	 */
	expected_relkind = classform->relkind;

	if (expected_relkind == RELKIND_PARTITIONED_INDEX)
		expected_relkind = RELKIND_INDEX;

	if (expected_relkind != relkind)
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
				errmsg("\"%s\" is not an index", rel->relname)));

	/* Allow DROP to either table owner or schema owner */
	if (!pg_class_ownercheck(relOid, GetUserId()) &&
	    !pg_namespace_ownercheck(classform->relnamespace, GetUserId()))
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_INDEX, rel->relname);
	}

	if (!allowSystemTableMods && IsSystemClass(relOid, classform))
		ereport(ERROR,
		        (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				        errmsg("permission denied: \"%s\" is a system catalog",
				               rel->relname)));

	ReleaseSysCache(tuple);

	/*
	 * In DROP INDEX, attempt to acquire lock on the parent table before
	 * locking the index.  index_drop() will need this anyway, and since
	 * regular queries lock tables before their indexes, we risk deadlock if
	 * we do it the other way around.  No error if we don't find a pg_index
	 * entry, though --- the relation may have been dropped.
	 */
	if (relkind == RELKIND_INDEX && relOid != oldRelOid)
	{
		state->heapOid = IndexGetRelation(relOid, true);
		if (OidIsValid(state->heapOid))
			LockRelationOid(state->heapOid, heap_lockmode);
	}
	/* *INDENT-ON* */
}


/*
 * Check permissions on table before acquiring relation lock; also lock
 * the heap before the RangeVarGetRelidExtended takes the index lock, to avoid
 * deadlocks.
 *
 * This code is borrowed from RangeVarCallbackForReindexIndex() in
 * commands/indexcmds.c in Postgres source. We need this to ensure the right
 * order of locking while dealing with REINDEX statements.
 */
static void
RangeVarCallbackForReindexIndex(const RangeVar *relation, Oid relId, Oid oldRelId,
								void *arg)
{
	/* *INDENT-OFF* */
	char		relkind;
	struct ReindexIndexCallbackState *state = arg;
	LOCKMODE	table_lockmode;

	/*
	 * Lock level here should match table lock in reindex_index() for
	 * non-concurrent case and table locks used by index_concurrently_*() for
	 * concurrent case.
	 */
	table_lockmode = state->concurrent ? ShareUpdateExclusiveLock : ShareLock;

	/*
	 * If we previously locked some other index's heap, and the name we're
	 * looking up no longer refers to that relation, release the now-useless
	 * lock.
	 */
	if (relId != oldRelId && OidIsValid(oldRelId))
	{
		UnlockRelationOid(state->locked_table_oid, table_lockmode);
		state->locked_table_oid = InvalidOid;
	}

	/* If the relation does not exist, there's nothing more to do. */
	if (!OidIsValid(relId))
		return;

	/*
	 * If the relation does exist, check whether it's an index.  But note that
	 * the relation might have been dropped between the time we did the name
	 * lookup and now.  In that case, there's nothing to do.
	 */
	relkind = get_rel_relkind(relId);
	if (!relkind)
		return;
	if (relkind != RELKIND_INDEX && relkind != RELKIND_PARTITIONED_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index", relation->relname)));

	/* Check permissions */
	if (!pg_class_ownercheck(relId, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_INDEX, relation->relname);

	/* Lock heap before index to avoid deadlock. */
	if (relId != oldRelId)
	{
		Oid			table_oid = IndexGetRelation(relId, true);

		/*
		 * If the OID isn't valid, it means the index was concurrently
		 * dropped, which is not a problem for us; just return normally.
		 */
		if (OidIsValid(table_oid))
		{
			LockRelationOid(table_oid, table_lockmode);
			state->locked_table_oid = table_oid;
		}
	}
	/* *INDENT-ON* */
}


/*
 * ErrorIfUnsupportedIndexStmt checks if the corresponding index statement is
 * supported for distributed tables and errors out if it is not.
 */
static void
ErrorIfUnsupportedIndexStmt(IndexStmt *createIndexStatement)
{
	if (createIndexStatement->tableSpace != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("specifying tablespaces with CREATE INDEX statements is "
							   "currently unsupported")));
	}

	if (createIndexStatement->unique)
	{
		RangeVar *relation = createIndexStatement->relation;
		bool missingOk = false;

		/* caller uses ShareLock for non-concurrent indexes, use the same lock here */
		LOCKMODE lockMode = ShareLock;
		Oid relationId = RangeVarGetRelid(relation, lockMode, missingOk);
		bool indexContainsPartitionColumn = false;

		/*
		 * Non-distributed tables do not have partition key, and unique constraints
		 * are allowed for them. Thus, we added a short-circuit for non-distributed tables.
		 */
		if (IsCitusTableType(relationId, CITUS_TABLE_WITH_NO_DIST_KEY))
		{
			return;
		}

		if (IsCitusTableType(relationId, APPEND_DISTRIBUTED))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("creating unique indexes on append-partitioned tables "
								   "is currently unsupported")));
		}

		Var *partitionKey = DistPartitionKeyOrError(relationId);
		List *indexParameterList = createIndexStatement->indexParams;
		IndexElem *indexElement = NULL;
		foreach_ptr(indexElement, indexParameterList)
		{
			const char *columnName = indexElement->name;

			/* column name is null for index expressions, skip it */
			if (columnName == NULL)
			{
				continue;
			}

			AttrNumber attributeNumber = get_attnum(relationId, columnName);
			if (attributeNumber == partitionKey->varattno)
			{
				indexContainsPartitionColumn = true;
			}
		}

		if (!indexContainsPartitionColumn)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("creating unique indexes on non-partition "
								   "columns is currently unsupported")));
		}
	}
}


/*
 * DropIndexTaskList builds a list of tasks to execute a DROP INDEX command
 * against a specified distributed table.
 */
static List *
DropIndexTaskList(Oid relationId, Oid indexId, DropStmt *dropStmt)
{
	List *taskList = NIL;
	List *shardIntervalList = LoadShardIntervalList(relationId);
	char *indexName = get_rel_name(indexId);
	Oid schemaId = get_rel_namespace(indexId);
	char *schemaName = get_namespace_name(schemaId);
	StringInfoData ddlString;
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	initStringInfo(&ddlString);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;
		char *shardIndexName = pstrdup(indexName);

		AppendShardIdToName(&shardIndexName, shardId);

		/* deparse shard-specific DROP INDEX command */
		appendStringInfo(&ddlString, "DROP INDEX %s %s %s %s",
						 (dropStmt->concurrent ? "CONCURRENTLY" : ""),
						 (dropStmt->missing_ok ? "IF EXISTS" : ""),
						 quote_qualified_identifier(schemaName, shardIndexName),
						 (dropStmt->behavior == DROP_RESTRICT ? "RESTRICT" : "CASCADE"));

		Task *task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		SetTaskQueryString(task, pstrdup(ddlString.data));
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependentTaskList = NULL;
		task->anchorShardId = shardId;
		task->taskPlacementList = ActiveShardPlacementList(shardId);

		taskList = lappend(taskList, task);

		resetStringInfo(&ddlString);
	}

	return taskList;
}


/*
 * MarkIndexValid marks an index as valid after a CONCURRENTLY command. We mark
 * indexes invalid in PostProcessIndexStmt and then commit, such that any failure
 * leaves behind an invalid index. We mark it as valid here when the command
 * completes.
 */
void
MarkIndexValid(IndexStmt *indexStmt)
{
	Assert(indexStmt->concurrent);
	Assert(IsCoordinator());

	/*
	 * We make sure schema name is not null in the PreprocessIndexStmt
	 */
	bool missingOk = false;
	Oid schemaId = get_namespace_oid(indexStmt->relation->schemaname, missingOk);

	Oid relationId PG_USED_FOR_ASSERTS_ONLY =
		get_relname_relid(indexStmt->relation->relname, schemaId);

	Assert(IsCitusTable(relationId));

	/* get the affected relation and index */
	Relation relation = table_openrv(indexStmt->relation, ShareUpdateExclusiveLock);
	Oid indexRelationId = get_relname_relid(indexStmt->idxname,
											schemaId);
	Relation indexRelation = index_open(indexRelationId, RowExclusiveLock);

	/* mark index as valid, in-place (cannot be rolled back) */
	index_set_state_flags(indexRelationId, INDEX_CREATE_SET_VALID);

	table_close(relation, NoLock);
	index_close(indexRelation, NoLock);
}
